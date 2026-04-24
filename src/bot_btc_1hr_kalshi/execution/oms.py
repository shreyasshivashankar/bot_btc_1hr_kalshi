"""Order Management System.

Responsibilities:
  * Translate TrapSignal + sized contracts -> OrderRequest
  * Call risk.check() as the single gate (hard rule: risk.check gates every order)
  * Submit via Broker (await ack — no fire-and-forget; hard rule #10 spirit)
  * Apply fills to Portfolio (open / close)
  * Emit DecisionRecord for every decision (approved AND rejected — hard rule #6)
  * Emit BetOutcome once per closed bet
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass

from bot_btc_1hr_kalshi.config.settings import RiskSettings
from bot_btc_1hr_kalshi.execution.broker.base import (
    Broker,
    Fill,
    OrderAck,
    OrderRequest,
    OrderType,
)
from bot_btc_1hr_kalshi.market_data.types import TradeEvent
from bot_btc_1hr_kalshi.obs.activity import ActivityTracker
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.lifecycle import LifecycleEmitter
from bot_btc_1hr_kalshi.obs.logging import BET_OUTCOMES_LOGGER, get_logger
from bot_btc_1hr_kalshi.obs.schemas import (
    BetOutcome,
    DecisionRecord,
    ExitReason,
    Features,
    Sizing,
)
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.risk.check import Approve, Reject, RiskInput, check
from bot_btc_1hr_kalshi.risk.kelly import kelly_contracts
from bot_btc_1hr_kalshi.signal.types import TrapSignal


@dataclass(frozen=True, slots=True)
class EntryResult:
    decision: DecisionRecord
    ack: OrderAck | None
    position_id: str | None


@dataclass(frozen=True, slots=True)
class ExitResult:
    ack: OrderAck
    bet_outcome: BetOutcome | None


@dataclass(frozen=True, slots=True)
class _RestingExitInfo:
    """Per-position bookkeeping for a maker exit that the broker has acked
    as `resting`. Held until the order fills (via `on_trade_event`), is
    cancelled (via `cancel_resting_exit`), or — in live mode — surfaces
    through reconciliation. Stored separately from `_pending_exit` (the
    in-flight submit lock) because resting orders persist across ticks."""

    order_id: str
    exit_reason: ExitReason
    posted_at_ns: int
    posted_price_cents: int


class OMS:
    def __init__(
        self,
        *,
        broker: Broker,
        portfolio: Portfolio,
        breakers: BreakerState,
        risk_settings: RiskSettings,
        min_signal_confidence: float,
        clock: Clock,
        lifecycle: LifecycleEmitter | None = None,
        activity: ActivityTracker | None = None,
        calendar_is_blocked: Callable[[int], bool] | None = None,
    ) -> None:
        self._broker = broker
        self._portfolio = portfolio
        self._breakers = breakers
        self._risk = risk_settings
        self._min_conf = min_signal_confidence
        self._clock = clock
        self._log = get_logger("bot_btc_1hr_kalshi.oms")
        self._outcomes_log = get_logger(BET_OUTCOMES_LOGGER)
        # Optional — when None the OMS is silent on the lifecycle channel.
        # Tests wire None; production wires a real emitter so the audit
        # trail covers every order transition.
        self._lifecycle = lifecycle
        # Optional — when set, every consider_entry stamps the tracker so
        # a process watchdog can detect a wedged decision loop.
        self._activity = activity
        # Optional — when set, every consider_entry asks the callback whether
        # `now_ns` is inside a tier-1 macro-event blackout window and threads
        # the verdict to `risk.check`. Wired by `__main__` via
        # `attach_calendar_guard` after the guard is constructed; default
        # None keeps tests and calendar-disabled paths green.
        self._calendar_is_blocked = calendar_is_blocked
        # Counter for partial-close bet_id suffixing. A position that fully
        # closes after N partials will have emitted outcomes p1..pN plus a
        # final unsuffixed outcome for the remainder.
        self._partial_seq: dict[str, int] = {}
        # Smart-router exit bookkeeping. `_resting_exits` maps position_id
        # → resting maker exit info; `_resting_exits_by_order` is the
        # reverse for fast match_trade fill lookup. Both stay coherent —
        # every mutation updates both. Cleared when the order fully fills,
        # is cancelled, or the position closes for any other reason.
        self._resting_exits: dict[str, _RestingExitInfo] = {}
        self._resting_exits_by_order: dict[str, str] = {}

    def attach_calendar_guard(
        self, is_blocked: Callable[[int], bool] | None
    ) -> None:
        """Late-binding setter: `CalendarGuard` is built after the OMS in
        the startup sequence, so we wire the `is_blocked` callback once the
        guard exists. Passing `None` disables the gate (used when calendar
        is absent / empty)."""
        self._calendar_is_blocked = is_blocked

    async def consider_entry(
        self,
        *,
        signal: TrapSignal,
        market_id: str,
        settlement_ts_ns: int = 0,
    ) -> EntryResult:
        decision_id = str(uuid.uuid4())
        now_ns = self._clock.now_ns()
        if self._activity is not None:
            self._activity.mark_decision(now_ns)

        # Strike-laddering Kelly divisor (Slice 12). Splits a single
        # full-Kelly bet across the configured number of correlated
        # rungs so a fully-built ladder approximates one full-Kelly bet
        # on the underlying directional thesis instead of N stacked
        # full-Kelly bets. Applied uniformly to every rung regardless of
        # current correlated count — the discipline is "size for the
        # ladder you intend to build"; an unfilled ladder ends up under-
        # sized vs theory, which is the conservative side of the trade.
        # Divisor of 1 (max_correlated_positions=1) leaves Kelly untouched.
        ladder_divisor = float(self._risk.max_correlated_positions)
        ladder_scaled_fraction = self._risk.kelly_fraction / ladder_divisor

        sized = kelly_contracts(
            edge_cents=signal.edge_cents,
            entry_price_cents=signal.entry_price_cents,
            kelly_fraction=ladder_scaled_fraction,
            bankroll_usd=self._portfolio.bankroll_usd,
            max_notional_usd=self._risk.max_position_notional_usd,
            inverted_risk_threshold_cents=self._risk.inverted_risk_threshold_cents,
            inverted_risk_kelly_multiplier=self._risk.inverted_risk_kelly_multiplier,
        )

        # Record the EFFECTIVE fractional Kelly after both the ladder
        # divisor and the inverted-risk clip (Slice 11 Phase 3.2): the
        # decision journal reflects what was actually applied, so tuning
        # queries can correlate clip- and ladder-triggered outcomes vs
        # baseline without reconstructing the rules from settings at
        # emit time.
        effective_kelly_fraction = ladder_scaled_fraction
        if signal.entry_price_cents >= self._risk.inverted_risk_threshold_cents:
            effective_kelly_fraction *= self._risk.inverted_risk_kelly_multiplier

        sizing = Sizing(
            kelly_fraction=effective_kelly_fraction,
            edge_cents=signal.edge_cents,
            variance_estimate=_binary_variance(signal.entry_price_cents),
            notional_usd=sized * signal.entry_price_cents / 100.0,
            contracts=sized,
        )

        correlated_count = self._portfolio.count_correlated_open(
            side=signal.side, settlement_ts_ns=settlement_ts_ns
        )
        calendar_blocked = (
            self._calendar_is_blocked(now_ns)
            if self._calendar_is_blocked is not None
            else False
        )
        verdict = check(
            RiskInput(
                signal=signal,
                contracts=sized,
                bankroll_usd=self._portfolio.bankroll_usd,
                open_positions_notional_usd=self._portfolio.open_positions_notional_usd,
                daily_realized_pnl_usd=self._portfolio.daily_realized_pnl_usd,
                breakers=self._breakers,
                now_ns=now_ns,
                min_signal_confidence=self._min_conf,
                correlated_open_positions_count=correlated_count,
                calendar_blocked=calendar_blocked,
            ),
            self._risk,
        )

        approved = isinstance(verdict, Approve)
        reject_reason = verdict.reason if isinstance(verdict, Reject) else None

        decision = DecisionRecord(
            decision_id=decision_id,
            ts_ns=now_ns,
            market_id=market_id,
            trap=signal.trap,
            side=signal.side,
            entry_price_cents=signal.entry_price_cents,
            features=signal.features,
            sizing=sizing,
            approved=approved,
            reject_reason=reject_reason,
        )
        self._log.info("decision", **decision.model_dump())
        if self._lifecycle is not None:
            self._lifecycle.decision(
                decision_id=decision_id,
                market_id=market_id,
                trap=signal.trap,
                side=signal.side,
                approved=approved,
                contracts=sized,
                reject_reason=reject_reason,
            )

        if not approved:
            return EntryResult(decision=decision, ack=None, position_id=None)

        approved_contracts = verdict.contracts if isinstance(verdict, Approve) else 0
        req = OrderRequest(
            client_order_id=decision_id,
            market_id=market_id,
            side=signal.side,
            action="BUY",
            limit_price_cents=signal.entry_price_cents,
            contracts=approved_contracts,
            order_type="maker",
        )
        if self._lifecycle is not None:
            self._lifecycle.order_submitted(
                decision_id=decision_id,
                client_order_id=req.client_order_id,
                market_id=req.market_id,
                side=req.side,
                action=req.action,
                contracts=req.contracts,
                limit_price_cents=req.limit_price_cents,
                order_type=req.order_type,
            )
        ack = await self._broker.submit(req)
        if self._lifecycle is not None:
            self._lifecycle.order_ack(
                decision_id=decision_id,
                client_order_id=ack.client_order_id,
                order_id=ack.order_id,
                status=ack.status,
                filled_contracts=ack.filled_contracts,
                remaining_contracts=ack.remaining_contracts,
                reason=ack.reason,
            )
        return EntryResult(decision=decision, ack=ack, position_id=decision_id)

    def on_entry_fill(
        self,
        *,
        decision_id: str,
        fill: Fill,
        trap: str,
        features_at_entry: Features,
        settlement_ts_ns: int = 0,
    ) -> None:
        self._portfolio.open_from_fill(
            position_id=decision_id,
            decision_id=decision_id,
            fill=fill,
            trap=trap,
            features_at_entry=features_at_entry,
            settlement_ts_ns=settlement_ts_ns,
        )
        if self._lifecycle is not None:
            self._lifecycle.position_opened(
                position_id=decision_id,
                decision_id=decision_id,
                market_id=fill.market_id,
                side=fill.side,
                contracts=fill.contracts,
                entry_price_cents=fill.price_cents,
            )

    async def submit_exit(
        self,
        *,
        position_id: str,
        limit_price_cents: int,
        exit_reason: ExitReason,
        order_type: OrderType = "ioc",
    ) -> ExitResult:
        pos = self._portfolio.get(position_id)
        if pos is None:
            raise ValueError(f"no open position: {position_id}")

        # Nonce the client_order_id so that a retry after a lost ack doesn't
        # double-execute against a broker that de-dupes by client_order_id.
        now_ns = self._clock.now_ns()
        req = OrderRequest(
            client_order_id=f"exit-{position_id}-{now_ns}",
            market_id=pos.market_id,
            side=pos.side,
            action="SELL",
            limit_price_cents=limit_price_cents,
            contracts=pos.contracts,
            order_type=order_type,
        )
        ack = await self._broker.submit(req)

        bet_outcome: BetOutcome | None = None
        if ack.status == "resting":
            # Maker exit posted but not yet filled. Track so the monitor
            # can (a) skip re-submission while it sits, (b) cancel + re-
            # submit IOC if urgency escalates, and (c) finalize when
            # `on_trade_event` matches a public-tape fill against it.
            info = _RestingExitInfo(
                order_id=ack.order_id,
                exit_reason=exit_reason,
                posted_at_ns=now_ns,
                posted_price_cents=limit_price_cents,
            )
            self._resting_exits[position_id] = info
            self._resting_exits_by_order[ack.order_id] = position_id
            if self._lifecycle is not None:
                self._lifecycle.order_ack(
                    decision_id=position_id,
                    client_order_id=ack.client_order_id,
                    order_id=ack.order_id,
                    status=ack.status,
                    filled_contracts=ack.filled_contracts,
                    remaining_contracts=ack.remaining_contracts,
                    reason=ack.reason,
                )
            return ExitResult(ack=ack, bet_outcome=None)
        if ack.status == "filled" and len(ack.fills) > 0:
            exit_fill = _aggregate_sell_fill(ack.fills)
            bet_outcome = self._portfolio.close(
                position_id=position_id,
                exit_fill=exit_fill,
                exit_reason=exit_reason,
            )
            self._outcomes_log.info("bet_outcome", **bet_outcome.model_dump())
            self._partial_seq.pop(position_id, None)
            if self._lifecycle is not None:
                self._lifecycle.position_closed(
                    position_id=position_id,
                    exit_price_cents=exit_fill.price_cents,
                    net_pnl_usd=bet_outcome.net_pnl_usd,
                    exit_reason=exit_reason,
                )
        elif ack.status == "partially_filled" and len(ack.fills) > 0:
            # Kalshi IOC cancels the remainder automatically, so there is no
            # orphaned resting order — we close the filled slice and let the
            # monitor decide whether to re-fire on the remaining contracts
            # next tick.
            exit_fill = _aggregate_sell_fill(ack.fills)
            seq = self._partial_seq.get(position_id, 0) + 1
            self._partial_seq[position_id] = seq
            bet_outcome = self._portfolio.partial_close(
                position_id=position_id,
                exit_fill=exit_fill,
                exit_reason=exit_reason,
                partial_seq=seq,
            )
            self._outcomes_log.info("bet_outcome", **bet_outcome.model_dump())
            if self._lifecycle is not None:
                # partial_close mutates pos.contracts in place to the new
                # (reduced) count, so pos.contracts is already the remainder.
                self._lifecycle.position_partial_closed(
                    position_id=position_id,
                    closed_contracts=exit_fill.contracts,
                    remaining_contracts=pos.contracts,
                    exit_price_cents=exit_fill.price_cents,
                    partial_seq=seq,
                )
        elif ack.status in ("rejected", "cancelled"):
            # No fills, no state change. Log so repeated rejections are visible;
            # caller / monitor decides whether to retry.
            self._log.warning(
                "exit.non_fill",
                position_id=position_id,
                status=ack.status,
                reason=ack.reason,
            )
        return ExitResult(ack=ack, bet_outcome=bet_outcome)

    # ---- smart-router exit lifecycle -----------------------------------

    def has_resting_exit(self, position_id: str) -> bool:
        return position_id in self._resting_exits

    def resting_exit_reason(self, position_id: str) -> ExitReason | None:
        info = self._resting_exits.get(position_id)
        return info.exit_reason if info is not None else None

    async def cancel_resting_exit(self, position_id: str) -> bool:
        """Cancel the maker exit currently resting for `position_id`.

        Used by `PositionMonitor` when an urgency-tier exit (early cashout,
        soft stop, tier-1 flatten) supersedes a patient exit already in
        the book — we cancel the maker so we can resubmit IOC at the
        cross. Returns True iff the broker confirmed a cancellation.
        Even on False (broker says no such order — e.g., it filled in
        the meantime), we drop our local registry: a fill update will
        arrive separately via `on_trade_event`.
        """
        info = self._resting_exits.pop(position_id, None)
        if info is None:
            return False
        self._resting_exits_by_order.pop(info.order_id, None)
        cancelled = await self._broker.cancel(info.order_id)
        if not cancelled:
            self._log.info(
                "exit.cancel_resting_no_op",
                position_id=position_id,
                order_id=info.order_id,
            )
        return cancelled

    async def on_trade_event(self, trade: TradeEvent) -> tuple[Fill, ...]:
        """Drive resting maker exits to fills using public-tape trades.

        Called from the feedloop / replay orchestrator on every
        `TradeEvent`. Delegates the match to the broker (PaperBroker
        simulates; live brokers no-op), then for each returned fill:
        looks up the owning position via `_resting_exits_by_order`,
        applies the close (or partial close) through the portfolio,
        emits the BetOutcome, and tidies the resting-exit registry.

        Returns the fills that did **not** match a known resting exit.
        These are entry-side fills (replay registers entries elsewhere)
        or stray fills the caller may want to handle. The feedloop in
        production ignores the return value because live brokers
        no-op `match_trade` and entry fills arrive via reconciliation.
        Replay routes the unhandled fills into `_apply_entry_fill`.
        """
        fills = await self._broker.match_trade(trade)
        if not fills:
            return ()
        unhandled: list[Fill] = []
        for fill in fills:
            position_id = self._resting_exits_by_order.get(fill.order_id)
            if position_id is None:
                unhandled.append(fill)
                continue
            info = self._resting_exits.get(position_id)
            if info is None:
                # Defensive: order_id was registered but position-level
                # mapping was cleared (race with cancel_resting_exit).
                self._resting_exits_by_order.pop(fill.order_id, None)
                continue
            pos = self._portfolio.get(position_id)
            if pos is None:
                # Position closed elsewhere — drop registry and move on.
                self._resting_exits.pop(position_id, None)
                self._resting_exits_by_order.pop(fill.order_id, None)
                continue
            if fill.contracts >= pos.contracts:
                bet_outcome = self._portfolio.close(
                    position_id=position_id,
                    exit_fill=fill,
                    exit_reason=info.exit_reason,
                )
                self._outcomes_log.info("bet_outcome", **bet_outcome.model_dump())
                self._partial_seq.pop(position_id, None)
                self._resting_exits.pop(position_id, None)
                self._resting_exits_by_order.pop(fill.order_id, None)
                if self._lifecycle is not None:
                    self._lifecycle.position_closed(
                        position_id=position_id,
                        exit_price_cents=fill.price_cents,
                        net_pnl_usd=bet_outcome.net_pnl_usd,
                        exit_reason=info.exit_reason,
                    )
            else:
                seq = self._partial_seq.get(position_id, 0) + 1
                self._partial_seq[position_id] = seq
                bet_outcome = self._portfolio.partial_close(
                    position_id=position_id,
                    exit_fill=fill,
                    exit_reason=info.exit_reason,
                    partial_seq=seq,
                )
                self._outcomes_log.info("bet_outcome", **bet_outcome.model_dump())
                if self._lifecycle is not None:
                    self._lifecycle.position_partial_closed(
                        position_id=position_id,
                        closed_contracts=fill.contracts,
                        remaining_contracts=pos.contracts,
                        exit_price_cents=fill.price_cents,
                        partial_seq=seq,
                    )
                # Resting order is still live for the remainder; keep the
                # registry mapping. PaperBroker keeps the resting entry
                # alive too (its own `_resting[oid]` is only deleted when
                # remaining hits 0). If the next tick decides to escalate,
                # cancel_resting_exit will tear it down cleanly.
        return tuple(unhandled)


def _binary_variance(price_cents: int) -> float:
    """p*(1-p) for a binary contract priced in cents."""
    p = price_cents / 100.0
    return p * (1 - p)


def _aggregate_sell_fill(fills: tuple[Fill, ...]) -> Fill:
    """Collapse multi-level IOC fills into a single synthetic Fill with VWAP price."""
    if len(fills) == 1:
        return fills[0]
    total = sum(f.contracts for f in fills)
    notional_cents = sum(f.price_cents * f.contracts for f in fills)
    fees = sum(f.fees_usd for f in fills)
    # Integer round-half-up. Built-in `round()` is banker's rounding in 3.x,
    # which on .5 drifts toward even and biases cumulative VWAPs. The
    # `(n + d/2) // d` form rounds half away from zero for non-negative ints
    # and keeps cents math deterministic under replay.
    vwap = (notional_cents + total // 2) // total
    f0 = fills[0]
    return Fill(
        order_id=f0.order_id,
        client_order_id=f0.client_order_id,
        market_id=f0.market_id,
        side=f0.side,
        action=f0.action,
        price_cents=vwap,
        contracts=total,
        # Use the first fill's timestamp: it is the moment our IOC began
        # executing against the book. Sub-ms differences between first and
        # last prints do not meaningfully change our exit-time semantics,
        # but anchoring at the first print pairs cleanly with the order
        # submission timestamp and matches how downstream latency tracking
        # attributes the exit to the tick that triggered it.
        ts_ns=fills[0].ts_ns,
        fees_usd=fees,
    )
