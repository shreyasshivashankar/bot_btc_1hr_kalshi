"""App: holds the runtime-assembled pieces and exposes operator-level actions.

Admin routes delegate here so the HTTP layer stays thin. The App owns mutable
state: `trading_halted` (soft halt — we still monitor open positions, just
refuse new entries) and `tier1_override` (human kill-switch — halt + flatten).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from bot_btc_1hr_kalshi.archive.writer import ArchiveWriter
from bot_btc_1hr_kalshi.config.settings import Settings
from bot_btc_1hr_kalshi.execution.broker.base import Broker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.market_data.bars import MultiTimeframeBus
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.derivatives_oracle import DerivativesOracle
from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotOracle
from bot_btc_1hr_kalshi.market_data.types import (
    LiquidationHeatmapSample,
    OpenInterestSample,
    WhaleAlertSample,
)
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.activity import ActivityTracker
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.lifecycle import LifecycleEmitter
from bot_btc_1hr_kalshi.obs.schemas import BetOutcome
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import FeatureEngine


@dataclass(slots=True)
class App:
    settings: Settings
    clock: Clock
    breakers: BreakerState
    portfolio: Portfolio
    oms: OMS
    monitor: PositionMonitor
    broker: Broker | None = None
    lifecycle: LifecycleEmitter | None = None
    activity: ActivityTracker | None = None
    # When set, the feed-loop consumer should call `archive_writer.write(event)`
    # for every FeedEvent it processes so `make backtest` has a tick archive
    # to replay. Opened/closed by __main__; None in tests and in replay.
    archive_writer: ArchiveWriter | None = None
    # Persistent BTC spot source. Lives at App scope so discovery and the
    # FeatureEngine see a continuous price stream across hour-rolls (Slice
    # 6 — replaces the previous per-session SpotFeed ownership). None when
    # the feed loop is disabled (dev mode, unit tests).
    spot_oracle: SpotOracle | None = None
    # Persistent BTC derivatives source (PR-A: Hyperliquid OI; PR-B+: Bybit).
    # Lives at App scope alongside `spot_oracle`. Coinglass HTTP poller
    # remains wired in parallel until parity is observed in soak — both
    # paths write to `latest_open_interest` and the most recent wins.
    derivatives_oracle: DerivativesOracle | None = None
    # Multi-timeframe bar bus (Slice 7). Downstreams that need candle-closed
    # views of spot (RSI, VWAP, HTF alignment) subscribe to the relevant
    # timeframe here instead of re-aggregating from raw ticks. Fed by
    # `spot_oracle.subscribe_primary(bar_bus.ingest)` at startup so its
    # lifetime tracks the oracle's. None when the feed loop is disabled.
    bar_bus: MultiTimeframeBus | None = None
    # TF-keyed feature engine (Slice 8, Phase 2). Lives at App scope
    # alongside bar_bus — accumulator state must survive hourly market
    # rolls (the 1H RSI alone needs ~14 hours of 1h closes to warm up).
    # `__main__` attaches it to the bar bus once; each per-session
    # FeedLoop receives the same reference.
    feature_engine: FeatureEngine | None = None
    # Live-mode KalshiBroker's REST client. Held here so the process
    # shutdown path in `__main__.serve()` can `aclose()` it alongside
    # other async resources. None in dev / paper / shadow modes.
    kalshi_rest_client: httpx.AsyncClient | None = None
    # Latest Coinglass open-interest sample (Slice 11 P2 — shadow). Polled
    # on cadence; attached to outbound `MarketSnapshot` for optional trap
    # consumption. None during warmup or when the poller is disabled.
    # Not currently gating any signal — observation only pending a
    # risk-committee promotion decision.
    latest_open_interest: OpenInterestSample | None = None
    # Latest Coinglass liquidation-heatmap snapshot (Slice 11 P3 — shadow).
    # Observation-only summary (total, peak cluster, peak price) polled
    # on cadence. Same promotion contract as `latest_open_interest`:
    # risk-committee sign-off required before any trap reads this.
    latest_liquidation_heatmap: LiquidationHeatmapSample | None = None
    # Latest Whale Alert rolling summary (Slice 11 P4 — shadow). Net
    # exchange-flow USD over the polling window; observational only
    # until shadow-soak + risk-committee sign-off justify a threshold.
    latest_whale_alert: WhaleAlertSample | None = None
    books: dict[str, L2Book] = field(default_factory=dict)
    trading_halted: bool = False
    tier1_override_active: bool = False

    def status(self) -> dict[str, Any]:
        now_ns = self.clock.now_ns()
        base: dict[str, Any] = {
            "mode": self.settings.mode,
            "trading_halted": self.trading_halted,
            "tier1_override_active": self.tier1_override_active,
            "breaker_reason": self.breakers.reason(now_ns),
            "any_breaker_tripped": self.breakers.any_tripped(now_ns),
            "bankroll_usd": self.portfolio.bankroll_usd,
            "open_positions_count": len(self.portfolio.open_positions),
            "open_positions_notional_usd": self.portfolio.open_positions_notional_usd,
            "daily_realized_pnl_usd": self.portfolio.daily_realized_pnl_usd,
            "markets_tracked": sorted(self.books.keys()),
        }
        if self.activity is not None:
            base["activity"] = self.activity.snapshot(now_ns=now_ns)
        return base

    def mark_tick(self, ts_ns: int) -> None:
        if self.activity is not None:
            self.activity.mark_tick(ts_ns)

    def halt(self, *, reason: str = "operator") -> None:
        self.trading_halted = True
        if self.lifecycle is not None:
            self.lifecycle.halt(reason=reason)

    def resume(self, *, reason: str = "operator") -> None:
        if self.tier1_override_active:
            raise RuntimeError("cannot resume while tier1_override_active is true")
        self.trading_halted = False
        if self.lifecycle is not None:
            self.lifecycle.resume(reason=reason)

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
