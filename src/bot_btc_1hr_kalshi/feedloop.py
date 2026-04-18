"""Live feed loop: routes Kalshi WS + spot WS events through the trading graph.

Runs as a long-lived asyncio task alongside the admin HTTP server. On each
hour-boundary roll it rediscovers the current BTC hourly market via REST,
reconnects the Kalshi WS to the new ticker, and resets the L2 book. Spot
feeds (Coinbase / Binance) are persistent — they feed BTC-USD ticker prices
into the FeatureEngine independent of which Kalshi contract is active.

Design notes:
  * One entry in flight: `_entry_guard` ensures only one `consider_entry`
    awaits broker.submit at a time. Matches ReplayOrchestrator semantics.
  * Archive hook: every FeedEvent is written to `app.archive_writer` (if
    set) so the backtest CLI has replayable input.
  * Clock: pulled from `app.clock`. No `datetime.now()` anywhere (hard
    rule #5 — the live clock is SystemClock, but we still go through the
    Clock interface so tests can inject ManualClock).
  * Session ends when the clock passes `settlement_ts_ns + grace_sec`; the
    outer `run_forever` then redrives discovery for the next hour.

This module depends only on structural wiring — it does not know about
specific traps. All strategy logic stays in `signal/` and `risk/`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.execution.broker.base import Broker
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import EntryResult
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed, WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    binance_parser,
    build_coinbase_subscribe,
    coinbase_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.kalshi_rest import (
    HourlyMarket,
    KalshiRestClient,
    MarketDiscoveryError,
)
from bot_btc_1hr_kalshi.market_data.types import (
    BookUpdate,
    FeedEvent,
    SpotTick,
    TradeEvent,
)
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.signal.features import FeatureEngine
from bot_btc_1hr_kalshi.signal.registry import run_traps
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot

_log = structlog.get_logger("bot_btc_1hr_kalshi.feedloop")


@dataclass(slots=True)
class _PendingEntry:
    decision_id: str
    trap: str
    features: Features


def minutes_to_settlement_fn(settlement_ts_ns: int) -> Callable[[int], float]:
    """Return an MTS function bound to a specific settlement timestamp."""
    def mts(now_ns: int) -> float:
        return max(0.0, (settlement_ts_ns - now_ns) / 60_000_000_000)
    return mts


class FeedLoop:
    """Runs one Kalshi market session end-to-end.

    Lifetime: construct → run() → exits when the clock passes settlement.
    Caller re-constructs for the next hour with a fresh market_id.
    """

    def __init__(
        self,
        *,
        app: App,
        broker: Broker,
        book: L2Book,
        kalshi_feed: KalshiFeed,
        spot_feeds: list[SpotFeed],
        feature_engine: FeatureEngine,
        market_id: str,
        strike_usd: float,
        settlement_ts_ns: int,
        clock: Clock,
        grace_sec: float = 5.0,
    ) -> None:
        self._app = app
        self._broker = broker
        self._book = book
        self._kalshi_feed = kalshi_feed
        self._spot_feeds = spot_feeds
        self._features = feature_engine
        self._market_id = market_id
        self._strike_usd = strike_usd
        self._settlement_ns = settlement_ts_ns
        self._clock = clock
        self._grace_ns = int(grace_sec * 1_000_000_000)
        self._pending: dict[str, _PendingEntry] = {}
        self._entry_guard = asyncio.Lock()
        self._spot_price: float | None = None
        self._mts_fn = minutes_to_settlement_fn(settlement_ts_ns)

    async def run(self) -> None:
        self._app.register_book(self._book)
        if isinstance(self._broker, PaperBroker):
            self._broker.register_book(self._book)

        tasks: list[asyncio.Task[Any]] = []
        tasks.append(asyncio.create_task(self._consume_kalshi(), name="kalshi-consume"))
        for idx, spot in enumerate(self._spot_feeds):
            tasks.append(
                asyncio.create_task(self._consume_spot(spot), name=f"spot-{idx}")
            )
        deadline = self._settlement_ns + self._grace_ns
        try:
            while self._clock.now_ns() < deadline:
                # Poll every 500ms; clock-advance under replay is deterministic.
                # Under live SystemClock this is a cheap wake.
                await asyncio.sleep(0.5)
                if all(t.done() for t in tasks):
                    # All feeds exited — surface their exceptions, then stop.
                    break
        finally:
            for t in tasks:
                t.cancel()
            for t in tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t

    async def _consume_kalshi(self) -> None:
        async for event in self._kalshi_feed.events():
            await self._handle_event(event)

    async def _consume_spot(self, feed: SpotFeed) -> None:
        async for tick in feed.events():
            await self._handle_event(tick)

    async def _handle_event(self, event: FeedEvent) -> None:
        # Advance App watchdog + archive before any strategy work — we want
        # liveness + audit trail even if trap evaluation bails out.
        self._app.mark_tick(event.ts_ns)
        if self._app.archive_writer is not None:
            try:
                self._app.archive_writer.write(event)
            except Exception as exc:  # pragma: no cover — I/O failure
                _log.warning("feedloop.archive_write_error", error=str(exc))

        if isinstance(event, BookUpdate):
            self._book.apply(event)
        elif isinstance(event, TradeEvent):
            # Paper-broker maker-match happens via book.apply crossings;
            # trade events do not directly produce fills in our model.
            pass
        elif isinstance(event, SpotTick):
            self._features.update_spot(event.price_usd)
            self._spot_price = event.price_usd

        await self._maybe_enter()
        await self._monitor_tick()

    async def _maybe_enter(self) -> None:
        if self._app.trading_halted:
            return
        # Single-market, one-entry-in-flight: bail if already building up
        # a pending entry, or if we already have an open position.
        if self._pending or self._app.portfolio.open_positions:
            return
        snap = self._snapshot()
        if snap is None:
            return
        signal = run_traps(
            snap,
            min_confidence=self._app.settings.signal.min_signal_confidence,
        )
        if signal is None:
            return

        # Serialize consider_entry so two feeds can't both trigger at once.
        async with self._entry_guard:
            if self._pending or self._app.portfolio.open_positions:
                return
            result: EntryResult = await self._app.oms.consider_entry(
                signal=signal, market_id=self._market_id
            )
            if not result.decision.approved:
                return
            if result.position_id is not None:
                self._pending[result.decision.decision_id] = _PendingEntry(
                    decision_id=result.decision.decision_id,
                    trap=result.decision.trap,
                    features=result.decision.features,
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
        mts = self._mts_fn(self._clock.now_ns())
        features = Features(
            regime_trend=self._features.regime_trend(),
            regime_vol=self._features.regime_vol(),
            signal_confidence=min(1.0, abs(pct_b)),
            bollinger_pct_b=pct_b,
            atr_cents=float(atr),
            book_depth_at_entry=self._book.book_depth(levels=5),
            spread_cents=spread,
            spot_btc_usd=self._spot_price,
            minutes_to_settlement=mts,
        )
        return MarketSnapshot(
            market_id=self._market_id,
            book=self._book,
            features=features,
            spot_btc_usd=self._spot_price,
            minutes_to_settlement=mts,
            strike_usd=self._strike_usd,
        )


async def ws_connect_websockets(url: str) -> Any:
    """Default WSConnect wrapping `websockets.connect`.

    Kept lazy-imported so unit tests that inject a FakeConn never pay the
    `websockets` import cost.
    """
    import websockets
    return await websockets.connect(url, max_size=8 * 1024 * 1024, ping_interval=20)


async def run_forever(
    *,
    app: App,
    broker: Broker,
    rest_http_client: httpx.AsyncClient,
    clock: Clock,
    kalshi_ws_url: str,
    coinbase_ws_url: str,
    binance_ws_url: str,
    ws_connect: WSConnect = ws_connect_websockets,
    rest_base: str = "/trade-api/v2",
    series_ticker: str = "KXBTC",
    on_book_invalidate: Callable[[str], None] | None = None,
    max_discovery_backoff_sec: float = 60.0,
) -> None:
    """Discover → run session → repeat. Never returns under normal operation.

    Exits cleanly on asyncio.CancelledError (shutdown path).
    """
    discovery = KalshiRestClient(client=rest_http_client, api_base=rest_base)
    backoff = 2.0
    while True:
        try:
            market = await discovery.current_btc_hourly_market(
                now_ns=clock.now_ns(), series_ticker=series_ticker
            )
            backoff = 2.0  # reset on success
        except (MarketDiscoveryError, httpx.HTTPError) as exc:
            _log.warning(
                "feedloop.discovery_failed",
                error=str(exc),
                backoff_sec=backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(max_discovery_backoff_sec, backoff * 2.0)
            continue

        _log.info(
            "feedloop.session_start",
            market_id=market.ticker,
            strike_usd=market.strike_usd,
            settlement_ts_ns=market.settlement_ts_ns,
        )
        try:
            await _run_one_session(
                app=app,
                broker=broker,
                clock=clock,
                market=market,
                kalshi_ws_url=kalshi_ws_url,
                coinbase_ws_url=coinbase_ws_url,
                binance_ws_url=binance_ws_url,
                ws_connect=ws_connect,
                on_book_invalidate=on_book_invalidate,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover — defensive
            _log.error("feedloop.session_error", error=str(exc), market=market.ticker)
            await asyncio.sleep(2.0)


async def _run_one_session(
    *,
    app: App,
    broker: Broker,
    clock: Clock,
    market: HourlyMarket,
    kalshi_ws_url: str,
    coinbase_ws_url: str,
    binance_ws_url: str,
    ws_connect: WSConnect,
    on_book_invalidate: Callable[[str], None] | None,
) -> None:
    book = L2Book(market.ticker)

    def _invalidate(reason: str) -> None:
        book.invalidate(reason)
        if on_book_invalidate is not None:
            on_book_invalidate(reason)

    kalshi_staleness = StalenessTracker(
        name="kalshi",
        clock=clock,
        threshold_ms=app.settings.feeds.kalshi.staleness_halt_ms,
    )
    coinbase_staleness = StalenessTracker(
        name="coinbase",
        clock=clock,
        threshold_ms=app.settings.feeds.coinbase.staleness_halt_ms,
    )
    binance_staleness = StalenessTracker(
        name="binance",
        clock=clock,
        threshold_ms=app.settings.feeds.binance.staleness_halt_ms,
    )

    kalshi_feed = KalshiFeed(
        ws_url=kalshi_ws_url,
        market_tickers=[market.ticker],
        clock=clock,
        ws_connect=ws_connect,
        staleness=kalshi_staleness,
        on_reconnect=_invalidate,
    )
    spot_feeds = [
        SpotFeed(
            name="coinbase",
            ws_url=coinbase_ws_url,
            clock=clock,
            ws_connect=ws_connect,
            staleness=coinbase_staleness,
            parse=coinbase_parser(clock),
            subscribe=build_coinbase_subscribe(["BTC-USD"]),
        ),
        SpotFeed(
            name="binance",
            ws_url=binance_ws_url,
            clock=clock,
            ws_connect=ws_connect,
            staleness=binance_staleness,
            parse=binance_parser(clock),
            subscribe=None,
        ),
    ]

    feature_engine = FeatureEngine(
        bollinger_period=app.settings.signal.bollinger_period_bars,
        bollinger_std_mult=app.settings.signal.bollinger_std_mult,
    )
    loop = FeedLoop(
        app=app,
        broker=broker,
        book=book,
        kalshi_feed=kalshi_feed,
        spot_feeds=spot_feeds,
        feature_engine=feature_engine,
        market_id=market.ticker,
        strike_usd=market.strike_usd,
        settlement_ts_ns=market.settlement_ts_ns,
        clock=clock,
    )
    try:
        await loop.run()
    finally:
        # Drop the book reference so the next session can register a new one
        # under the new ticker. If an open position still exists we keep it
        # (hard rule #10 — operator must flatten).
        app.books.pop(market.ticker, None)


__all__ = [
    "FeedLoop",
    "minutes_to_settlement_fn",
    "run_forever",
    "ws_connect_websockets",
]


# ---------------------------------------------------------------------------
# Back-compat / convenience — some callers want to drive a single session
# without the forever-loop wrapper.
_SingleSessionCallable = Callable[..., Awaitable[None]]
run_one_session: _SingleSessionCallable = _run_one_session
