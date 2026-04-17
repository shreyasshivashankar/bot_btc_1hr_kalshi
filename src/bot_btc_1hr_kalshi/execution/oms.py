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
from dataclasses import dataclass

from bot_btc_1hr_kalshi.config.settings import RiskSettings
from bot_btc_1hr_kalshi.execution.broker.base import (
    Broker,
    Fill,
    OrderAck,
    OrderRequest,
)
from bot_btc_1hr_kalshi.obs.clock import Clock
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
    ) -> None:
        self._broker = broker
        self._portfolio = portfolio
        self._breakers = breakers
        self._risk = risk_settings
        self._min_conf = min_signal_confidence
        self._clock = clock
        self._log = get_logger("bot_btc_1hr_kalshi.oms")
        self._outcomes_log = get_logger(BET_OUTCOMES_LOGGER)
        # Counter for partial-close bet_id suffixing. A position that fully
        # closes after N partials will have emitted outcomes p1..pN plus a
        # final unsuffixed outcome for the remainder.
        self._partial_seq: dict[str, int] = {}

    async def consider_entry(
        self,
        *,
        signal: TrapSignal,
        market_id: str,
    ) -> EntryResult:
        decision_id = str(uuid.uuid4())
        now_ns = self._clock.now_ns()

        sized = kelly_contracts(
            edge_cents=signal.edge_cents,
            entry_price_cents=signal.entry_price_cents,
            kelly_fraction=self._risk.kelly_fraction,
            bankroll_usd=self._portfolio.bankroll_usd,
            max_notional_usd=self._risk.max_position_notional_usd,
        )

        sizing = Sizing(
            kelly_fraction=self._risk.kelly_fraction,
            edge_cents=signal.edge_cents,
            variance_estimate=_binary_variance(signal.entry_price_cents),
            notional_usd=sized * signal.entry_price_cents / 100.0,
            contracts=sized,
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
        ack = await self._broker.submit(req)
        return EntryResult(decision=decision, ack=ack, position_id=decision_id)

    def on_entry_fill(
        self,
        *,
        decision_id: str,
        fill: Fill,
        trap: str,
        features_at_entry: Features,
    ) -> None:
        self._portfolio.open_from_fill(
            position_id=decision_id,
            decision_id=decision_id,
            fill=fill,
            trap=trap,
            features_at_entry=features_at_entry,
        )

    async def submit_exit(
        self,
        *,
        position_id: str,
        limit_price_cents: int,
        exit_reason: ExitReason,
    ) -> ExitResult:
        pos = self._portfolio.get(position_id)
        if pos is None:
            raise ValueError(f"no open position: {position_id}")

        # Nonce the client_order_id so that a retry after a lost ack doesn't
        # double-execute against a broker that de-dupes by client_order_id.
        req = OrderRequest(
            client_order_id=f"exit-{position_id}-{self._clock.now_ns()}",
            market_id=pos.market_id,
            side=pos.side,
            action="SELL",
            limit_price_cents=limit_price_cents,
            contracts=pos.contracts,
            order_type="ioc",
        )
        ack = await self._broker.submit(req)

        bet_outcome: BetOutcome | None = None
        if ack.status == "filled" and len(ack.fills) > 0:
            exit_fill = _aggregate_sell_fill(ack.fills)
            bet_outcome = self._portfolio.close(
                position_id=position_id,
                exit_fill=exit_fill,
                exit_reason=exit_reason,
            )
            self._outcomes_log.info("bet_outcome", **bet_outcome.model_dump())
            self._partial_seq.pop(position_id, None)
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
    vwap = round(notional_cents / total)
    f0 = fills[0]
    return Fill(
        order_id=f0.order_id,
        client_order_id=f0.client_order_id,
        market_id=f0.market_id,
        side=f0.side,
        action=f0.action,
        price_cents=vwap,
        contracts=total,
        ts_ns=fills[-1].ts_ns,
        fees_usd=fees,
    )
