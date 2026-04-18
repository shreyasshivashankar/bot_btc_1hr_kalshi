"""Live feed loop: routes Kalshi WS + spot WS events through the trading graph.

Runs as a long-lived asyncio task alongside the admin HTTP server. On each
hour-boundary roll it rediscovers the current BTC hourly market via REST,
reconnects the Kalshi WS to the new ticker, and resets the L2 book. Spot
feeds are persistent: Coinbase is the PRIMARY spot venue and feeds the
FeatureEngine; Kraken is the CONFIRMATION venue and feeds only the
`IntegrityTracker` (see `signal/integrity.py` — vetoes an entry only on
active directional disagreement).

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
from urllib.parse import urlparse

import httpx
import structlog

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.execution.broker.base import Broker
from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import EntryResult
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed, WSConnect, WSConnection
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
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
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import FeatureEngine
from bot_btc_1hr_kalshi.signal.integrity import IntegrityTracker
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


async def _watch_kalshi_staleness(
    *,
    breakers: BreakerState,
    tracker: StalenessTracker,
    poll_sec: float = 0.25,
) -> None:
    """Polls the Kalshi StalenessTracker and flips the feed_staleness breaker.

    Hard rule: primary feed staleness > 2s halts new entries. The tracker
    knows its own threshold (`threshold_ms` from the feed config); we just
    compare and flip the breaker on transition. Edge-triggered: we only
    touch the breaker when the stale bit changes, to keep the breaker
    log line meaningful rather than a recurring heartbeat.
    """
    prev_stale = False
    while True:
        cur_stale = tracker.is_stale()
        if cur_stale != prev_stale:
            if cur_stale:
                _log.error(
                    "staleness.halt",
                    feed=tracker.name,
                    age_ms=tracker.age_ms(),
                )
            else:
                _log.info("staleness.recovered", feed=tracker.name)
            breakers.set_feed_halt(halted=cur_stale)
            prev_stale = cur_stale
        await asyncio.sleep(poll_sec)


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
        integrity: IntegrityTracker | None = None,
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
        self._integrity = integrity

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
            # Primary vs confirmation routing: Coinbase drives features;
            # Kraken only feeds the integrity gate. Any other venue is
            # silently dropped — the Venue literal prevents that at type
            # check but a mis-wired test fixture would still reach here.
            if event.venue == "coinbase":
                self._features.update_spot(event.price_usd)
                self._spot_price = event.price_usd
                if self._integrity is not None:
                    self._integrity.record_primary(event.ts_ns, event.price_usd)
            elif event.venue == "kraken":
                if self._integrity is not None:
                    self._integrity.record_confirmation(event.ts_ns, event.price_usd)

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

        # Primary/Confirmation integrity gate (signal/integrity.py). Only
        # vetoes on active directional disagreement between Coinbase and
        # Kraken over a ~1s window, plus fail-closed on long confirmation
        # silence. Absence of the tracker (tests, dev) skips the gate.
        if self._integrity is not None:
            decision = self._integrity.check_entry(self._clock.now_ns())
            if not decision.approved:
                _log.warning(
                    "feedloop.entry_vetoed_integrity",
                    trap=signal.trap,
                    reason=decision.reason,
                )
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


def ws_connect_kalshi_signed(signer: KalshiSigner) -> WSConnect:
    """WSConnect factory that signs the Kalshi WS handshake.

    Kalshi's trading-api WS (`/trade-api/ws/v2`) authenticates the opening
    GET using the same KALSHI-ACCESS-{KEY,TIMESTAMP,SIGNATURE} header trio
    as REST — see `execution/broker/kalshi_signer.py`. We derive the path
    from the URL so staging / mock hosts work without reconfiguration:
    `wss://host/trade-api/ws/v2` → signed path `/trade-api/ws/v2`.

    Spot feeds (Coinbase, Kraken) are public and must NOT be sent these
    headers; `_run_one_session` keeps a separate unsigned `WSConnect` for
    them.
    """

    async def _connect(url: str) -> WSConnection:
        import websockets

        path = urlparse(url).path or "/"
        headers = signer.headers(method="GET", path=path)
        return await websockets.connect(
            url,
            additional_headers=headers,
            max_size=8 * 1024 * 1024,
            ping_interval=20,
        )

    return _connect


async def run_forever(
    *,
    app: App,
    broker: Broker,
    rest_http_client: httpx.AsyncClient,
    clock: Clock,
    kalshi_ws_url: str,
    coinbase_ws_url: str,
    kraken_ws_url: str,
    ws_connect: WSConnect = ws_connect_websockets,
    spot_ws_connect: WSConnect | None = None,
    rest_base: str = "/trade-api/v2",
    series_ticker: str = "KXBTC",
    on_book_invalidate: Callable[[str], None] | None = None,
    max_discovery_backoff_sec: float = 60.0,
) -> None:
    """Discover → run session → repeat. Never returns under normal operation.

    Exits cleanly on asyncio.CancelledError (shutdown path).

    `ws_connect` is used for the Kalshi WS feed (may be signed). `spot_ws_connect`
    — if None, defaults to `ws_connect` for test back-compat — is used for the
    public spot venues, which MUST NOT receive Kalshi auth headers.
    """
    discovery = KalshiRestClient(client=rest_http_client, api_base=rest_base)
    spot_connect = spot_ws_connect if spot_ws_connect is not None else ws_connect
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
                kraken_ws_url=kraken_ws_url,
                ws_connect=ws_connect,
                spot_ws_connect=spot_connect,
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
    kraken_ws_url: str,
    ws_connect: WSConnect,
    spot_ws_connect: WSConnect | None = None,
    on_book_invalidate: Callable[[str], None] | None,
) -> None:
    spot_connect = spot_ws_connect if spot_ws_connect is not None else ws_connect
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
    kraken_staleness = StalenessTracker(
        name="kraken",
        clock=clock,
        threshold_ms=app.settings.feeds.kraken.staleness_halt_ms,
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
            ws_connect=spot_connect,
            staleness=coinbase_staleness,
            parse=coinbase_parser(clock),
            subscribe=build_coinbase_subscribe(["BTC-USD"]),
        ),
        SpotFeed(
            name="kraken",
            ws_url=kraken_ws_url,
            clock=clock,
            ws_connect=spot_connect,
            staleness=kraken_staleness,
            parse=kraken_parser(clock),
            subscribe=build_kraken_subscribe(["BTC/USD"]),
        ),
    ]

    feature_engine = FeatureEngine(
        bollinger_period=app.settings.signal.bollinger_period_bars,
        bollinger_std_mult=app.settings.signal.bollinger_std_mult,
    )
    integrity = IntegrityTracker(
        velocity_window_sec=app.settings.integrity.velocity_window_sec,
        active_disagreement_floor_usd=app.settings.integrity.active_disagreement_floor_usd,
        stale_halt_sec=app.settings.integrity.stale_halt_sec,
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
        integrity=integrity,
    )
    # Enforce hard-rule invariant "primary feed staleness > 2s → halt" by
    # polling the Kalshi tracker. Scoped to the session: cancelled on exit
    # and the next session spawns a fresh one against a fresh tracker.
    staleness_task = asyncio.create_task(
        _watch_kalshi_staleness(breakers=app.breakers, tracker=kalshi_staleness),
        name="kalshi-staleness-watchdog",
    )
    try:
        await loop.run()
    finally:
        staleness_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await staleness_task
        # Clear stale-feed breaker at session end: the next session's fresh
        # tracker starts with last_msg_ns=None (never reports stale until its
        # feed produces), so leaving the breaker latched would spuriously
        # halt the new session.
        app.breakers.set_feed_halt(halted=False)
        # Drop the book reference so the next session can register a new one
        # under the new ticker. If an open position still exists we keep it
        # (hard rule #10 — operator must flatten).
        app.books.pop(market.ticker, None)


__all__ = [
    "FeedLoop",
    "minutes_to_settlement_fn",
    "run_forever",
    "ws_connect_kalshi_signed",
    "ws_connect_websockets",
]


# ---------------------------------------------------------------------------
# Back-compat / convenience — some callers want to drive a single session
# without the forever-loop wrapper.
_SingleSessionCallable = Callable[..., Awaitable[None]]
run_one_session: _SingleSessionCallable = _run_one_session
