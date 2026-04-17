"""PositionMonitor: per-tick exit decisions for every open position.

Priority order (DESIGN.md §7.4a):
  1. early_cashout  (best bid >= 99c)
  2. theta_net_target  (edge eroded — book depth suggests we can cleanly cash out
     above the mid without giving back all remaining theta)
  3. adaptive_soft_stop  (drawdown on the bet exceeds an adjusted threshold)

Only one exit order in flight per position. After submitting an exit we mark
the position "pending_exit" and skip it on subsequent ticks until OMS clears it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.config.settings import MonitorSettings
from bot_btc_1hr_kalshi.execution.oms import OMS, ExitResult
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.obs.schemas import ExitReason
from bot_btc_1hr_kalshi.portfolio.positions import OpenPosition, Portfolio

MonitorAction = Literal[
    "noop",
    "skip_pending_exit",
    "skip_book_invalid",
    "early_cashout_99",
    "theta_net_target",
    "soft_stop",
]


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

        # Priority 1: early cashout
        if best_bid.price_cents >= self._settings.early_cashout_price_cents:
            return await self._submit_exit(pos, best_bid.price_cents, "early_cashout_99")

        # Priority 2: theta-net target
        depth = book.book_depth(levels=5)
        if depth >= self._settings.theta_net_book_depth_threshold:
            # pct gain vs entry; take if we're meaningfully above entry and time is thin.
            gain_cents = best_bid.price_cents - pos.entry_price_cents
            if gain_cents >= 5 and minutes_to_settlement <= 15.0:
                return await self._submit_exit(pos, best_bid.price_cents, "theta_net_target")

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
            return await self._submit_exit(pos, best_bid.price_cents, "soft_stop")

        return MonitorTick(position_id=pos.position_id, action="noop")

    async def _submit_exit(
        self, pos: OpenPosition, limit_price_cents: int, reason: ExitReason
    ) -> MonitorTick:
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
            )
        finally:
            self._pending_exit.discard(pos.position_id)
        action: MonitorAction
        if reason == "early_cashout_99":
            action = "early_cashout_99"
        elif reason == "theta_net_target":
            action = "theta_net_target"
        else:
            action = "soft_stop"
        return MonitorTick(position_id=pos.position_id, action=action, exit=result)


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
