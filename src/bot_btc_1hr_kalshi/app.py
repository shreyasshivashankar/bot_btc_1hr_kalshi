"""App: holds the runtime-assembled pieces and exposes operator-level actions.

Admin routes delegate here so the HTTP layer stays thin. The App owns mutable
state: `trading_halted` (soft halt — we still monitor open positions, just
refuse new entries) and `tier1_override` (human kill-switch — halt + flatten).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bot_btc_1hr_kalshi.config.settings import Settings
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.schemas import BetOutcome
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState


@dataclass(slots=True)
class App:
    settings: Settings
    clock: Clock
    breakers: BreakerState
    portfolio: Portfolio
    oms: OMS
    monitor: PositionMonitor
    books: dict[str, L2Book] = field(default_factory=dict)
    trading_halted: bool = False
    tier1_override_active: bool = False

    def status(self) -> dict[str, Any]:
        return {
            "mode": self.settings.mode,
            "trading_halted": self.trading_halted,
            "tier1_override_active": self.tier1_override_active,
            "breaker_reason": self.breakers.reason(self.clock.now_ns()),
            "any_breaker_tripped": self.breakers.any_tripped(self.clock.now_ns()),
            "bankroll_usd": self.portfolio.bankroll_usd,
            "open_positions_count": len(self.portfolio.open_positions),
            "open_positions_notional_usd": self.portfolio.open_positions_notional_usd,
            "daily_realized_pnl_usd": self.portfolio.daily_realized_pnl_usd,
            "markets_tracked": sorted(self.books.keys()),
        }

    def halt(self) -> None:
        self.trading_halted = True

    def resume(self) -> None:
        if self.tier1_override_active:
            raise RuntimeError("cannot resume while tier1_override_active is true")
        self.trading_halted = False

    async def flatten(self) -> list[BetOutcome]:
        """Submit IOC exits for every open position. Tier-1 flatten semantics
        (hard rule #8): both winners and losers are flattened.
        """
        outcomes: list[BetOutcome] = []
        for pos in list(self.portfolio.open_positions):
            book = self.books.get(pos.market_id)
            if book is None or not book.valid or book.best_bid is None:
                continue
            result = await self.oms.submit_exit(
                position_id=pos.position_id,
                limit_price_cents=book.best_bid.price_cents,
                exit_reason="tier1_flatten",
            )
            if result.bet_outcome is not None:
                outcomes.append(result.bet_outcome)
        return outcomes

    async def tier1_override(self) -> list[BetOutcome]:
        """Human kill-switch: halt + flatten."""
        self.tier1_override_active = True
        self.trading_halted = True
        return await self.flatten()

    def register_book(self, book: L2Book) -> None:
        self.books[book.market_id] = book

    def ready(self) -> tuple[bool, str]:
        """Cloud Run readiness check: ready iff no breaker tripped AND we have
        at least one valid book to trade against (if no books yet, we're booting).
        """
        if self.breakers.any_tripped(self.clock.now_ns()):
            return False, f"breaker:{self.breakers.reason(self.clock.now_ns())}"
        if not self.books:
            return False, "no_markets_registered"
        if not any(b.valid for b in self.books.values()):
            return False, "no_valid_books"
        return True, "ok"
