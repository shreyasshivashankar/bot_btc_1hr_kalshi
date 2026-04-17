"""Tick replay orchestrator.

Drives a stream of FeedEvents through the full signal -> risk -> OMS -> monitor
pipeline against a PaperBroker. Used by:
  * `tests/integration/*` to assert end-to-end determinism
  * `make replay` / `make backtest` CLIs

Single-market by design — the hourly Kalshi BTC market is one contract at a
time. Multi-market replay is a Slice 2 concern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.execution.broker.base import Fill
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import EntryResult
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookUpdate, FeedEvent, SpotTick, TradeEvent
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.signal.features import FeatureEngine
from bot_btc_1hr_kalshi.signal.registry import run_traps
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot


@dataclass(slots=True)
class PendingEntry:
    decision_id: str
    trap: str
    features_at_entry: Features


@dataclass(slots=True)
class ReplayResult:
    entries_attempted: int = 0
    entries_approved: int = 0
    entries_rejected: int = 0
    fills: list[Fill] = field(default_factory=list)
    reject_reasons: list[str] = field(default_factory=list)


class ReplayOrchestrator:
    """Drives the trading graph from a replay feed. All state lives in `App`."""

    def __init__(
        self,
        *,
        app: App,
        broker: PaperBroker,
        clock: ManualClock,
        market_id: str,
        feature_engine: FeatureEngine,
        minutes_to_settlement_fn: Any | None = None,
    ) -> None:
        self._app = app
        self._broker = broker
        self._clock = clock
        self._market_id = market_id
        self._features = feature_engine
        self._book = L2Book(market_id)
        self._app.register_book(self._book)
        self._broker.register_book(self._book)
        self._pending: dict[str, PendingEntry] = {}
        self._spot_price: float | None = None
        self._mts_fn = minutes_to_settlement_fn or (lambda _ns: 30.0)
        self.result = ReplayResult()

    @property
    def book(self) -> L2Book:
        return self._book

    async def handle(self, event: FeedEvent) -> None:
        # Advance clock to event time (deterministic — hard rule #5).
        if event.ts_ns > self._clock.now_ns():
            self._clock.set_ns(event.ts_ns)

        if isinstance(event, BookUpdate):
            self._book.apply(event)
        elif isinstance(event, TradeEvent):
            for fill in await self._broker.match_trade(event):
                self._apply_entry_fill(fill)
                self.result.fills.append(fill)
        elif isinstance(event, SpotTick):
            self._features.update_spot(event.price_usd)
            self._spot_price = event.price_usd

        await self._maybe_enter()
        await self._monitor_tick()

    def _apply_entry_fill(self, fill: Fill) -> None:
        pending = self._pending.pop(fill.client_order_id, None)
        if pending is None:
            return
        self._app.oms.on_entry_fill(
            decision_id=pending.decision_id,
            fill=fill,
            trap=pending.trap,
            features_at_entry=pending.features_at_entry,
        )

    async def _maybe_enter(self) -> None:
        if self._app.trading_halted or self._pending:
            return  # only one entry in flight at a time (single-market)
        if self._app.portfolio.open_positions:
            return

        snap = self._snapshot()
        if snap is None:
            return
        signal = run_traps(snap, min_confidence=self._app.settings.signal.min_signal_confidence)
        if signal is None:
            return

        self.result.entries_attempted += 1
        result: EntryResult = await self._app.oms.consider_entry(
            signal=signal, market_id=self._market_id
        )
        if not result.decision.approved:
            self.result.entries_rejected += 1
            if result.decision.reject_reason:
                self.result.reject_reasons.append(result.decision.reject_reason)
            return

        self.result.entries_approved += 1
        if result.position_id is not None:
            self._pending[result.decision.decision_id] = PendingEntry(
                decision_id=result.decision.decision_id,
                trap=result.decision.trap,
                features_at_entry=result.decision.features,
            )

    async def _monitor_tick(self) -> None:
        if not self._app.portfolio.open_positions:
            return
        await self._app.monitor.evaluate(
            book=self._book,
            minutes_to_settlement=self._mts_fn(self._clock.now_ns()),
            regime_vol=self._features.regime_vol(),
            regime_trend=self._features.regime_trend(),
        )

    def _snapshot(self) -> MarketSnapshot | None:
        if not self._book.valid:
            return None
        pct_b = self._features.bollinger_pct_b()
        atr = self._features.atr()
        if pct_b is None or atr is None or self._spot_price is None:
            return None
        spread = self._book.spread_cents
        if spread is None:
            return None

        features = Features(
            regime_trend=self._features.regime_trend(),
            regime_vol=self._features.regime_vol(),
            signal_confidence=min(1.0, abs(pct_b)),
            bollinger_pct_b=pct_b,
            atr_cents=float(atr),
            book_depth_at_entry=self._book.book_depth(levels=5),
            spread_cents=spread,
            spot_btc_usd=self._spot_price,
            minutes_to_settlement=self._mts_fn(self._clock.now_ns()),
        )
        return MarketSnapshot(
            market_id=self._market_id,
            book=self._book,
            features=features,
            spot_btc_usd=self._spot_price,
            minutes_to_settlement=features.minutes_to_settlement,
        )


async def replay(events: list[FeedEvent], orch: ReplayOrchestrator) -> ReplayResult:
    for event in events:
        await orch.handle(event)
    return orch.result
