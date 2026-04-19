"""Live feed loop: routes Kalshi WS + spot-oracle events through the trading graph.

Runs as a long-lived asyncio task alongside the admin HTTP server. On each
hour-boundary roll it rediscovers the current BTC hourly market via REST
(gated on fresh spot from the App-level `SpotOracle`), reconnects the
Kalshi WS to the new ticker, and resets the L2 book.

Spot feeds are **persistent across hour-rolls** — they live inside the
App-level `SpotOracle`, not the per-session `FeedLoop`. The loop
subscribes to primary/confirmation callbacks at session start and
unsubscribes at end; it never owns the spot WS lifetime. This closes the
gap where an hour-roll used to start with no spot reference at all,
forcing market discovery to tiebreak strikes alphabetically (and pick
deep-ITM markets with no edge).

Design notes:
  * One entry in flight: `_entry_guard` ensures only one `consider_entry`
    awaits broker.submit at a time. Matches ReplayOrchestrator semantics.
  * Archive hook: every FeedEvent is written to `app.archive_writer` (if
    set) so the backtest CLI has replayable input. Spot ticks are routed
    through the loop's `_record_primary_tick` / `_record_confirmation_tick`
    oracle callbacks, so they hit the archive + mark_tick path the same
    way they did when spot was an in-loop feed.
  * Clock: pulled from `app.clock`. No `datetime.now()` anywhere (hard
    rule #5 — live clock is SystemClock, but we still go through the
    Clock interface so tests can inject ManualClock).
  * Session ends when the clock passes `settlement_ts_ns + grace_sec`; the
    outer `run_forever` then redrives discovery for the next hour.
  * Hard LastSpot contract: `run_forever` calls
    `spot_oracle.get_primary(max_age_ms=risk.spot_staleness_halt_ms)` and
    backs off when it raises — no stale fallback to a cached price.

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
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.kalshi_rest import (
    HourlyMarket,
    KalshiRestClient,
    MarketDiscoveryError,
)
from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotOracle, SpotStaleError
from bot_btc_1hr_kalshi.market_data.types import (
    BookUpdate,
    FeedEvent,
    SpotTick,
    TradeEvent,
)
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import CVD_ROLLING_PERIODS, FeatureEngine
from bot_btc_1hr_kalshi.signal.integrity import IntegrityTracker
from bot_btc_1hr_kalshi.signal.registry import run_traps
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot

_log = structlog.get_logger("bot_btc_1hr_kalshi.feedloop")

# The trap-trigger TF (DESIGN.md §5). FeatureEngine is keyed by TF label;
# the snapshot builder here reads pct_b / ATR / regime at this TF and the
# Slice-8 HTF fields (1H RSI, 24h move) from their respective TFs. Traps
# consume all of them via `snap.features` — no parameterless reads remain.
_PRIMARY_TF: str = "5m"


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

    Spot is injected via `spot_oracle` (persistent App-level instance).
    The loop subscribes to primary/confirmation callbacks at run() start
    and unsubscribes in the finally block — this is the only place spot
    ticks enter the per-session trading graph.
    """

    def __init__(
        self,
        *,
        app: App,
        broker: Broker,
        book: L2Book,
        kalshi_feed: KalshiFeed,
        spot_oracle: SpotOracle,
        feature_engine: FeatureEngine,
        market_id: str,
        strike_usd: float,
        settlement_ts_ns: int,
        clock: Clock,
        grace_sec: float = 5.0,
        integrity: IntegrityTracker | None = None,
        spot_staleness_max_age_ms: int = 1000,
    ) -> None:
        self._app = app
        self._broker = broker
        self._book = book
        self._kalshi_feed = kalshi_feed
        self._spot_oracle = spot_oracle
        self._features = feature_engine
        self._market_id = market_id
        self._strike_usd = strike_usd
        self._settlement_ns = settlement_ts_ns
        self._clock = clock
        self._grace_ns = int(grace_sec * 1_000_000_000)
        self._pending: dict[str, _PendingEntry] = {}
        self._entry_guard = asyncio.Lock()
        self._mts_fn = minutes_to_settlement_fn(settlement_ts_ns)
        self._integrity = integrity
        self._spot_staleness_max_age_ms = spot_staleness_max_age_ms

    async def run(self) -> None:
        self._app.register_book(self._book)
        if isinstance(self._broker, PaperBroker):
            self._broker.register_book(self._book)

        # Subscribe to the persistent oracle. On cold start the callback
        # fires immediately with the current cached tick (see SpotOracle
        # docstring) so the FeatureEngine warms up on the first hour-roll
        # without waiting for the next Coinbase print.
        unsub_primary = self._spot_oracle.subscribe_primary(self._on_primary_tick)
        unsub_confirmation = self._spot_oracle.subscribe_confirmation(
            self._on_confirmation_tick
        )

        kalshi_task = asyncio.create_task(self._consume_kalshi(), name="kalshi-consume")
        deadline = self._settlement_ns + self._grace_ns
        try:
            while self._clock.now_ns() < deadline:
                await asyncio.sleep(0.5)
                if kalshi_task.done():
                    break
        finally:
            unsub_primary()
            unsub_confirmation()
            kalshi_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await kalshi_task

    async def _consume_kalshi(self) -> None:
        async for event in self._kalshi_feed.events():
            await self._handle_event(event)

    def _on_primary_tick(self, tick: SpotTick) -> None:
        """Oracle callback: Coinbase (primary) — feeds integrity only.

        FeatureEngine is fed via the MultiTimeframeBus (attached at App
        scope in `__main__`). Raw ticks drive the bus; the bus fires
        bar closes into the engine's subscribed callback — so this
        handler no longer poke the engine directly.
        """
        self._archive_and_mark(tick)
        if self._integrity is not None:
            self._integrity.record_primary(tick.ts_ns, tick.price_usd)

    def _on_confirmation_tick(self, tick: SpotTick) -> None:
        """Oracle callback: Kraken (confirmation) — feeds integrity only."""
        self._archive_and_mark(tick)
        if self._integrity is not None:
            self._integrity.record_confirmation(tick.ts_ns, tick.price_usd)

    def _archive_and_mark(self, event: FeedEvent) -> None:
        self._app.mark_tick(event.ts_ns)
        if self._app.archive_writer is not None:
            try:
                self._app.archive_writer.write(event)
            except Exception as exc:  # pragma: no cover — I/O failure
                _log.warning("feedloop.archive_write_error", error=str(exc))

    async def _handle_event(self, event: FeedEvent) -> None:
        # Advance App watchdog + archive before any strategy work — we want
        # liveness + audit trail even if trap evaluation bails out.
        self._archive_and_mark(event)

        if isinstance(event, BookUpdate):
            self._book.apply(event)
        elif isinstance(event, TradeEvent):
            # Paper-broker maker-match happens via book.apply crossings;
            # trade events do not directly produce fills in our model.
            pass
        elif isinstance(event, SpotTick):
            # Spot ticks arrive via oracle callbacks, not the Kalshi feed;
            # if one shows up here it's a mis-wired test fixture. Drop it
            # rather than double-counting (callbacks already routed it).
            return

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
        signal = run_traps(snap, settings=self._app.settings.signal)
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
                signal=signal,
                market_id=self._market_id,
                settlement_ts_ns=self._settlement_ns,
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
            regime_vol=self._features.regime_vol(_PRIMARY_TF),
            regime_trend=self._features.regime_trend(_PRIMARY_TF),
        )

    def _snapshot(self) -> MarketSnapshot | None:
        if not self._book.valid:
            return None
        pct_b = self._features.bollinger_pct_b(_PRIMARY_TF)
        atr = self._features.atr(_PRIMARY_TF)
        if pct_b is None or atr is None:
            return None
        # Hard LastSpot contract: if the primary spot is stale, sit this
        # tick out. `get_primary_or_none` returns None on both cold-start
        # and staleness, so the upstream "no signal yet" branch handles
        # both — identical to refusing to trade, which is what the
        # contract demands.
        spot = self._spot_oracle.get_primary_or_none(
            max_age_ms=self._spot_staleness_max_age_ms
        )
        if spot is None:
            return None
        spread = self._book.spread_cents
        if spread is None:
            return None
        mts = self._mts_fn(self._clock.now_ns())
        features = Features(
            regime_trend=self._features.regime_trend(_PRIMARY_TF),
            regime_vol=self._features.regime_vol(_PRIMARY_TF),
            signal_confidence=min(1.0, abs(pct_b)),
            bollinger_pct_b=pct_b,
            atr_cents=float(atr),
            book_depth_at_entry=self._book.book_depth(levels=5),
            spread_cents=spread,
            spot_btc_usd=spot,
            minutes_to_settlement=mts,
            rsi_5m=self._features.rsi("5m"),
            rsi_1h=self._features.rsi("1h"),
            move_24h_pct=self._features.move_24h_pct(),
            cvd_1m_usd=self._features.cvd("1m", periods=CVD_ROLLING_PERIODS),
        )
        return MarketSnapshot(
            market_id=self._market_id,
            book=self._book,
            features=features,
            spot_btc_usd=spot,
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
    spot_oracle: SpotOracle,
    ws_connect: WSConnect = ws_connect_websockets,
    rest_base: str = "/trade-api/v2",
    series_ticker: str = "KXBTC",
    on_book_invalidate: Callable[[str], None] | None = None,
    max_discovery_backoff_sec: float = 60.0,
) -> None:
    """Discover → run session → repeat. Never returns under normal operation.

    Exits cleanly on asyncio.CancelledError (shutdown path).

    `ws_connect` is used for the Kalshi WS feed (may be signed). Spot WS
    lifetimes are owned by `spot_oracle` at App level; this loop only
    reads `oracle.get_primary(...)` to steer strike selection.

    Discovery is gated on a fresh primary-spot reading (hard LastSpot
    contract). If the oracle's primary is stale or missing, we back off
    and retry — never fall back to alphabetical strike selection.
    """
    discovery = KalshiRestClient(client=rest_http_client, api_base=rest_base)
    spot_max_age_ms = app.settings.risk.spot_staleness_halt_ms
    backoff = 2.0
    while True:
        try:
            btc_spot = spot_oracle.get_primary(max_age_ms=spot_max_age_ms)
        except SpotStaleError as exc:
            _log.warning(
                "feedloop.discovery_waiting_on_spot",
                error=str(exc),
                backoff_sec=backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(max_discovery_backoff_sec, backoff * 2.0)
            continue

        try:
            market = await discovery.current_btc_hourly_market(
                now_ns=clock.now_ns(),
                series_ticker=series_ticker,
                btc_spot_usd=btc_spot,
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
            btc_spot_usd=btc_spot,
            strike_gap_usd=abs(market.strike_usd - btc_spot),
        )
        try:
            await _run_one_session(
                app=app,
                broker=broker,
                clock=clock,
                market=market,
                kalshi_ws_url=kalshi_ws_url,
                spot_oracle=spot_oracle,
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
    spot_oracle: SpotOracle,
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

    kalshi_feed = KalshiFeed(
        ws_url=kalshi_ws_url,
        market_tickers=[market.ticker],
        clock=clock,
        ws_connect=ws_connect,
        staleness=kalshi_staleness,
        on_reconnect=_invalidate,
    )

    # FeatureEngine is App-scoped (Slice 8) — its accumulator state must
    # persist across hourly market rolls. `__main__` builds and attaches
    # it to the bar_bus once; each session just reads the reference.
    if app.feature_engine is None:  # pragma: no cover — wiring invariant
        raise RuntimeError("app.feature_engine must be set before run_forever")
    feature_engine = app.feature_engine
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
        spot_oracle=spot_oracle,
        feature_engine=feature_engine,
        market_id=market.ticker,
        strike_usd=market.strike_usd,
        settlement_ts_ns=market.settlement_ts_ns,
        clock=clock,
        integrity=integrity,
        spot_staleness_max_age_ms=app.settings.risk.spot_staleness_halt_ms,
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
