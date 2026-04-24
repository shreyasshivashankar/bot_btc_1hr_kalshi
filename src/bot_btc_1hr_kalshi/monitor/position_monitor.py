"""PositionMonitor: per-tick exit decisions for every open position.

Priority order (DESIGN.md §7.4a):
  1. early_cashout  (best bid >= 99c)
  2. theta_net_target  (edge eroded — book depth suggests we can cleanly cash out
     above the mid without giving back all remaining theta)
  3. adaptive_soft_stop  (drawdown on the bet exceeds an adjusted threshold)

Only one exit order in flight per position. After submitting an exit we mark
the position "pending_exit" and skip it on subsequent ticks until OMS clears it.

Smart-router exit routing
-------------------------
Two routes:
  * **IOC (cross immediately):** for urgent exits where giving back any
    edge is worse than paying the spread — `early_cashout_99`,
    `soft_stop`, `tier1_flatten`. We post at the current best bid (on
    the position's owned side) and Kalshi cancels any unfilled remainder.
  * **Maker (post inside the spread):** for patient exits where we have
    time to capture the spread — `theta_net_target`, `arb_basis_closed`.
    We post one cent above the best bid, sitting between bid and ask.
    The OMS keeps the order in `_resting_exits` until it fills (driven
    by `match_trade` against the public tape) or we cancel it on
    escalation. If the spread is too tight (top-of-book bid+1 ≥ ask),
    we degrade to IOC because there's no room to make.

If a resting maker exit is already live for a position when the monitor
re-evaluates, we let it sit unless an urgency-tier reason (early
cashout, soft stop, tier-1 flatten) fires — in which case we cancel
the resting maker and resubmit IOC. This is the IOC escalation ladder
referenced in `CLAUDE.md`'s execution module description.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.config.settings import MonitorSettings
from bot_btc_1hr_kalshi.execution.broker.base import OrderType
from bot_btc_1hr_kalshi.execution.oms import OMS, ExitResult
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookLevel
from bot_btc_1hr_kalshi.obs.schemas import ExitReason
from bot_btc_1hr_kalshi.portfolio.positions import OpenPosition, Portfolio
from bot_btc_1hr_kalshi.signal.edge_model import settlement_prob_yes

# Arb basis is considered "closed" (alpha captured) when the market price
# on the owned side is within this many cents of current fair value. 3c
# matches the spec's fast-exit threshold: any further waiting risks
# giving back alpha to theta + adverse reversal.
_ARB_BASIS_CLOSE_CENTS: float = 3.0

# Exit reasons that must cross immediately. Anything not in this set is
# a candidate for the maker route (subject to spread-width feasibility).
_URGENT_EXIT_REASONS: frozenset[ExitReason] = frozenset(
    {"early_cashout_99", "soft_stop", "tier1_flatten"}
)

MonitorAction = Literal[
    "noop",
    "skip_pending_exit",
    "skip_book_invalid",
    "skip_resting_maker_exit",
    "early_cashout_99",
    "theta_net_target",
    "soft_stop",
    "arb_basis_closed",
    "escalated_resting_to_ioc",
]


def _classify_exit_route(reason: ExitReason) -> OrderType:
    """Urgent exits cross; everything else tries to make first."""
    return "ioc" if reason in _URGENT_EXIT_REASONS else "maker"


@dataclass(frozen=True, slots=True)
class MonitorTick:
    position_id: str
    action: MonitorAction
    exit: ExitResult | None = None


class PositionMonitor:
    def __init__(
        self,
        *,
        oms: OMS,
        portfolio: Portfolio,
        settings: MonitorSettings,
    ) -> None:
        self._oms = oms
        self._portfolio = portfolio
        self._settings = settings
        self._pending_exit: set[str] = set()

    def mark_exit_cleared(self, position_id: str) -> None:
        """Called by OMS after an exit order resolves (fill or reject)."""
        self._pending_exit.discard(position_id)

    async def evaluate(
        self,
        *,
        book: L2Book,
        minutes_to_settlement: float,
        regime_vol: str,
        regime_trend: str,
        spot_btc_usd: float | None = None,
        strike_usd: float | None = None,
    ) -> tuple[MonitorTick, ...]:
        results: list[MonitorTick] = []
        for pos in self._portfolio.open_positions:
            if pos.market_id != book.market_id:
                continue
            results.append(
                await self._evaluate_one(
                    pos=pos,
                    book=book,
                    minutes_to_settlement=minutes_to_settlement,
                    regime_vol=regime_vol,
                    regime_trend=regime_trend,
                    spot_btc_usd=spot_btc_usd,
                    strike_usd=strike_usd,
                )
            )
        return tuple(results)

    async def _evaluate_one(
        self,
        *,
        pos: OpenPosition,
        book: L2Book,
        minutes_to_settlement: float,
        regime_vol: str,
        regime_trend: str,
        spot_btc_usd: float | None,
        strike_usd: float | None,
    ) -> MonitorTick:
        if pos.position_id in self._pending_exit:
            return MonitorTick(position_id=pos.position_id, action="skip_pending_exit")
        if not book.valid:
            return MonitorTick(position_id=pos.position_id, action="skip_book_invalid")

        # Evaluate in the position's own price space (YES-position → YES bid,
        # NO-position → NO bid which is 100 - YES ask).
        best_bid = book.best_bid_for(pos.side)
        if best_bid is None:
            return MonitorTick(position_id=pos.position_id, action="noop")
        best_ask = book.best_ask_for(pos.side)

        # Priority 1: early cashout. Always wins, including over a resting
        # maker exit — at 99c the fill is essentially free and waiting
        # for the maker to clear costs us the chance.
        if best_bid.price_cents >= self._settings.early_cashout_price_cents:
            if self._oms.has_resting_exit(pos.position_id):
                await self._oms.cancel_resting_exit(pos.position_id)
                tick = await self._submit_exit(
                    pos, best_bid, best_ask, "early_cashout_99"
                )
                return MonitorTick(
                    position_id=pos.position_id,
                    action="escalated_resting_to_ioc",
                    exit=tick.exit,
                )
            return await self._submit_exit(pos, best_bid, best_ask, "early_cashout_99")

        # Priority 1b: arb-basis-closed fast exit. The implied-basis-arb
        # trap's alpha is the mispricing vs Normal-CDF fair value; once
        # the market price on the owned side has converged to within
        # ~3c of fair, the thesis has played out and continuing to hold
        # just accrues theta + fade risk. Requires current spot + strike
        # (FeedLoop passes them; test harnesses that omit them skip this
        # branch — other priorities still apply).
        if (
            pos.trap == "implied_basis_arb"
            and spot_btc_usd is not None
            and strike_usd is not None
        ):
            q_yes = settlement_prob_yes(
                spot_usd=spot_btc_usd,
                strike_usd=strike_usd,
                sigma_per_minute_usd=pos.features_at_entry.atr_cents,
                minutes_to_settlement=minutes_to_settlement,
            )
            fair_cents = q_yes * 100.0 if pos.side == "YES" else (1.0 - q_yes) * 100.0
            if abs(best_bid.price_cents - fair_cents) <= _ARB_BASIS_CLOSE_CENTS:
                if self._oms.has_resting_exit(pos.position_id):
                    return MonitorTick(
                        position_id=pos.position_id,
                        action="skip_resting_maker_exit",
                    )
                return await self._submit_exit(
                    pos, best_bid, best_ask, "arb_basis_closed"
                )

        # Priority 2: theta-net target
        depth = book.book_depth(levels=5)
        if depth >= self._settings.theta_net_book_depth_threshold:
            # pct gain vs entry; take if we're meaningfully above entry and time is thin.
            gain_cents = best_bid.price_cents - pos.entry_price_cents
            if gain_cents >= 5 and minutes_to_settlement <= 15.0:
                if self._oms.has_resting_exit(pos.position_id):
                    return MonitorTick(
                        position_id=pos.position_id,
                        action="skip_resting_maker_exit",
                    )
                return await self._submit_exit(
                    pos, best_bid, best_ask, "theta_net_target"
                )

        # Priority 3: adaptive soft stop
        stop_frac = _adjusted_stop_fraction(
            base=self._settings.soft_stop.base_fraction,
            regime_vol=regime_vol,
            regime_trend=regime_trend,
            minutes_to_settlement=minutes_to_settlement,
            high_vol_mult=self._settings.soft_stop.regime_multiplier_high_vol,
            trending_mult=self._settings.soft_stop.regime_multiplier_trending,
            late_window_mult=self._settings.soft_stop.time_multiplier_late_window,
        )
        stop_price = int(pos.entry_price_cents * (1 - stop_frac))
        if best_bid.price_cents <= stop_price:
            if self._oms.has_resting_exit(pos.position_id):
                await self._oms.cancel_resting_exit(pos.position_id)
                tick = await self._submit_exit(pos, best_bid, best_ask, "soft_stop")
                return MonitorTick(
                    position_id=pos.position_id,
                    action="escalated_resting_to_ioc",
                    exit=tick.exit,
                )
            return await self._submit_exit(pos, best_bid, best_ask, "soft_stop")

        # Nothing fired this tick. If a maker exit is still resting, the
        # caller already knows about it via has_resting_exit; surface a
        # dedicated action so observability can distinguish "doing
        # nothing because no signal" from "doing nothing because we're
        # parked in the book waiting for a maker fill."
        if self._oms.has_resting_exit(pos.position_id):
            return MonitorTick(
                position_id=pos.position_id, action="skip_resting_maker_exit"
            )
        return MonitorTick(position_id=pos.position_id, action="noop")

    async def _submit_exit(
        self,
        pos: OpenPosition,
        best_bid: BookLevel,
        best_ask: BookLevel | None,
        reason: ExitReason,
    ) -> MonitorTick:
        route = _classify_exit_route(reason)
        limit_price_cents, route = _exit_price_for_route(
            route=route, best_bid=best_bid, best_ask=best_ask
        )

        # The lock prevents a second exit order from being queued within the
        # same evaluate() call for the same position. It is *always* cleared
        # on return: after a partial fill the position still exists (with
        # shrunk contracts) and must be re-evaluable next tick; after a
        # rejection or a full close we also want it unlocked.
        self._pending_exit.add(pos.position_id)
        try:
            result = await self._oms.submit_exit(
                position_id=pos.position_id,
                limit_price_cents=limit_price_cents,
                exit_reason=reason,
                order_type=route,
            )
        finally:
            self._pending_exit.discard(pos.position_id)
        action: MonitorAction
        if reason == "early_cashout_99":
            action = "early_cashout_99"
        elif reason == "theta_net_target":
            action = "theta_net_target"
        elif reason == "arb_basis_closed":
            action = "arb_basis_closed"
        else:
            action = "soft_stop"
        return MonitorTick(position_id=pos.position_id, action=action, exit=result)


def _exit_price_for_route(
    *,
    route: OrderType,
    best_bid: BookLevel,
    best_ask: BookLevel | None,
) -> tuple[int, OrderType]:
    """Choose the limit price for an exit.

    For IOC: cross at the best bid on the owned side — we get an
    immediate fill at the visible price (or worse on subsequent levels
    if size > top-of-book).

    For maker: post one cent above the best bid, capturing the spread.
    If the spread is too tight (`bid + 1 >= ask`) there's no room to
    sit between bid and ask, so degrade to IOC. Same fallback if the
    other side of the book is missing — without a visible ask we can't
    confirm we wouldn't cross.
    """
    if route == "ioc":
        return best_bid.price_cents, "ioc"
    inside_price = best_bid.price_cents + 1
    if best_ask is None or inside_price >= best_ask.price_cents:
        return best_bid.price_cents, "ioc"
    return inside_price, "maker"


def _adjusted_stop_fraction(
    *,
    base: float,
    regime_vol: str,
    regime_trend: str,
    minutes_to_settlement: float,
    high_vol_mult: float,
    trending_mult: float,
    late_window_mult: float,
) -> float:
    f = base
    if regime_vol == "high":
        f *= high_vol_mult
    if regime_trend in ("up", "down"):
        f *= trending_mult
    if minutes_to_settlement <= 10.0:
        f *= late_window_mult
    return min(f, 0.9)
