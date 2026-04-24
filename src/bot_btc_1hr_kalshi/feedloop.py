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
from collections import deque
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
from bot_btc_1hr_kalshi.signal.registry import run_traps_cross_strike
from bot_btc_1hr_kalshi.signal.types import LiquidationPressure, MarketSnapshot

_log = structlog.get_logger("bot_btc_1hr_kalshi.feedloop")

# The trap-trigger TF (DESIGN.md §5). FeatureEngine is keyed by TF label;
# the snapshot builder here reads pct_b / ATR / regime at this TF and the
# Slice-8 HTF fields (1H RSI, 24h move) from their respective TFs. Traps
# consume all of them via `snap.features` — no parameterless reads remain.
_PRIMARY_TF: str = "5m"

# Rolling spot-range window used by the implied-basis-arb dead-spot gate.
# A raw ns bound is simpler than a per-bar aggregator and we only need
# max-min over the window; both are O(window) on the deque.
_SPOT_RANGE_WINDOW_NS: int = 60_000_000_000

# How many strikes of the current hourly series to maintain L2 books for.
# The WS is multiplexed — one connection, N subscriptions. 5 covers the
# ±$500 / ±$1000 / ATM neighborhood that dominates tradeable short-dated
# mean-reversion edges without bloating memory (each L2Book is ~a few kB
# of resting levels). All strikes share one settlement_ts_ns, so the
# correlation cap's identity remains well-defined. Bump cautiously: a
# larger universe inflates the cross-sectional evaluator's eval cost
# linearly per tick.
_MAX_STRIKES_TRACKED: int = 5


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
        books: dict[str, L2Book],
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
        strikes: dict[str, float] | None = None,
    ) -> None:
        if market_id not in books:
            raise ValueError(
                f"primary market_id {market_id!r} missing from books "
                f"{sorted(books)!r}"
            )
        # `strikes` carries the per-ticker strike for snapshot building
        # across the universe. Single-book callers may omit it; we
        # auto-fill from (market_id, strike_usd).
        resolved_strikes: dict[str, float] = dict(strikes) if strikes is not None else {}
        resolved_strikes.setdefault(market_id, strike_usd)
        missing = set(books) - set(resolved_strikes)
        if missing:
            raise ValueError(
                f"strikes missing entries for books: {sorted(missing)!r}"
            )
        self._app = app
        self._broker = broker
        self._books = books
        self._book = books[market_id]  # primary — kept for monitor evaluation
        self._kalshi_feed = kalshi_feed
        self._spot_oracle = spot_oracle
        self._features = feature_engine
        self._market_id = market_id
        self._strike_usd = strike_usd
        self._strikes = resolved_strikes
        self._settlement_ns = settlement_ts_ns
        self._clock = clock
        self._grace_ns = int(grace_sec * 1_000_000_000)
        self._pending: dict[str, _PendingEntry] = {}
        self._entry_guard = asyncio.Lock()
        self._mts_fn = minutes_to_settlement_fn(settlement_ts_ns)
        self._integrity = integrity
        self._spot_staleness_max_age_ms = spot_staleness_max_age_ms
        # Rolling 60s primary-spot history (ts_ns, price_usd) for the
        # implied-basis-arb dead-spot gate. Appended in _on_primary_tick
        # and read at snapshot build time — per-session lifetime matches
        # the other integrity state here.
        self._spot_history: deque[tuple[int, float]] = deque()

    async def run(self) -> None:
        for book in self._books.values():
            self._app.register_book(book)
            if isinstance(self._broker, PaperBroker):
                self._broker.register_book(book)

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
        self._spot_history.append((tick.ts_ns, tick.price_usd))
        cutoff = tick.ts_ns - _SPOT_RANGE_WINDOW_NS
        while self._spot_history and self._spot_history[0][0] < cutoff:
            self._spot_history.popleft()

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
            book = self._books.get(event.market_id)
            if book is None:
                _log.warning(
                    "feedloop.unknown_market_book_update",
                    market_id=event.market_id,
                    known=sorted(self._books),
                )
                return
            book.apply(event)
            # Hard rule #9 recovery: a seq gap leaves the book INVALID and
            # there is no in-band path back to VALID — Kalshi only ships an
            # `orderbook_snapshot` on subscribe. Force the WS to drop so
            # the existing reconnect+resubscribe loop pulls a fresh,
            # seq-aligned snapshot. REST snapshot interleaving was rejected
            # because there is no shared seq anchor between the REST body
            # and the WS delta stream — see KalshiFeed.force_reconnect.
            if (
                not book.valid
                and book.invalidation_reason is not None
                and book.invalidation_reason.startswith("seq_gap")
            ):
                _log.warning(
                    "feedloop.seq_gap_force_reconnect",
                    market_id=event.market_id,
                    reason=book.invalidation_reason,
                )
                await self._kalshi_feed.force_reconnect()
                return
        elif isinstance(event, TradeEvent):
            # Drive resting maker exits to fills using the public tape.
            # OMS delegates to broker.match_trade — PaperBroker simulates,
            # KalshiBroker / ShadowBroker no-op (live fills come back via
            # ack / WS order channel / reconciler instead).
            await self._app.oms.on_trade_event(event)
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
        # One-entry-in-flight across the whole session per pending lock.
        # Beyond pending, bail only when *both* sides of this hour have
        # already filled the correlation ladder — otherwise a rung might
        # still be available and we want to evaluate. risk.check is the
        # authoritative gate; this is just a perf short-circuit that
        # also keeps the decision journal free of correlation_cap rejects
        # on ticks where there's clearly no room.
        if self._pending or self._all_correlation_sides_capped():
            return
        snaps = self._build_snapshots()
        if not snaps:
            return
        result_pair = run_traps_cross_strike(
            snaps, settings=self._app.settings.signal
        )
        if result_pair is None:
            return
        chosen_snap, signal = result_pair

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

        _log.info(
            "feedloop.cross_strike_selected",
            chosen_market_id=chosen_snap.market_id,
            chosen_strike_usd=chosen_snap.strike_usd,
            trap=signal.trap,
            side=signal.side,
            edge_cents=signal.edge_cents,
            confidence=signal.confidence,
            candidates_evaluated=len(snaps),
        )

        # Serialize consider_entry so two feeds can't both trigger at once.
        async with self._entry_guard:
            if self._pending or self._all_correlation_sides_capped():
                return
            result: EntryResult = await self._app.oms.consider_entry(
                signal=signal,
                market_id=chosen_snap.market_id,
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

    def _all_correlation_sides_capped(self) -> bool:
        """True iff *both* YES and NO already hold `max_correlated_positions`
        rungs on this hour. Used by `_maybe_enter` to short-circuit before
        snapshotting when no rung remains on either side; risk.check is the
        authoritative per-side cap. With cap=1 (laddering disabled) this
        reduces to the legacy "bail when any open position exists" because
        opening one rung trivially fills that side and the cross-strike
        evaluator only emits one side per tick anyway."""
        cap = self._app.settings.risk.max_correlated_positions
        portfolio = self._app.portfolio
        yes_filled = portfolio.count_correlated_open(
            side="YES", settlement_ts_ns=self._settlement_ns
        )
        no_filled = portfolio.count_correlated_open(
            side="NO", settlement_ts_ns=self._settlement_ns
        )
        return yes_filled >= cap and no_filled >= cap

    async def _monitor_tick(self) -> None:
        if not self._app.portfolio.open_positions:
            return
        # Arb-basis-closed exit needs current spot + strike to recompute
        # fair value; other exits don't. Missing spot (stale oracle) just
        # disables that branch — the legacy priorities (early cashout /
        # theta / soft stop) still fire off the book alone.
        spot = self._spot_oracle.get_primary_or_none(
            max_age_ms=self._spot_staleness_max_age_ms
        )
        await self._app.monitor.evaluate(
            book=self._book,
            minutes_to_settlement=self._mts_fn(self._clock.now_ns()),
            regime_vol=self._features.regime_vol(_PRIMARY_TF),
            regime_trend=self._features.regime_trend(_PRIMARY_TF),
            spot_btc_usd=spot,
            strike_usd=self._strike_usd,
        )

    def _build_snapshots(self) -> list[MarketSnapshot]:
        """Build a MarketSnapshot for every book that's currently valid
        enough to trade.

        Feature values (pct_b, ATR, RSI, CVD, regime, 24h move) are
        spot-driven and shared across all strikes — they come from the
        App-scope FeatureEngine and do not vary per-book. Per-book state
        (validity, depth, spread) is pulled from each L2Book individually.
        A stale primary spot or cold FeatureEngine short-circuits the
        whole list (nothing can be snapshotted).
        """
        if not self._book.valid:
            return []
        # Hard LastSpot contract: if the primary spot is stale, sit this
        # tick out. `get_primary_or_none` returns None on both cold-start
        # and staleness, identical to refusing to trade — which is what
        # the contract demands.
        spot = self._spot_oracle.get_primary_or_none(
            max_age_ms=self._spot_staleness_max_age_ms
        )
        if spot is None:
            return []
        # Pass live spot so pct_b moves with the tape between bar closes
        # — bands are still anchored to the latest 5m close on this TF.
        pct_b = self._features.bollinger_pct_b(_PRIMARY_TF, live_price=spot)
        atr = self._features.atr(_PRIMARY_TF)
        if pct_b is None or atr is None:
            return []
        mts = self._mts_fn(self._clock.now_ns())
        regime_trend = self._features.regime_trend(_PRIMARY_TF)
        regime_vol = self._features.regime_vol(_PRIMARY_TF)
        rsi_5m = self._features.rsi("5m")
        rsi_1h = self._features.rsi("1h")
        move_24h = self._features.move_24h_pct()
        cvd_1m = self._features.cvd("1m", periods=CVD_ROLLING_PERIODS)
        spot_range_60s = self._spot_range_60s()
        latest_oi = self._app.latest_open_interest
        oi_usd = latest_oi.total_oi_usd if latest_oi is not None else None
        liq_pressure = self._build_liquidation_pressure(spot=spot)

        snaps: list[MarketSnapshot] = []
        for market_id, book in self._books.items():
            if not book.valid:
                continue
            spread = book.spread_cents
            if spread is None:
                continue
            strike = self._strikes.get(market_id)
            if strike is None:
                continue
            features = Features(
                regime_trend=regime_trend,
                regime_vol=regime_vol,
                signal_confidence=min(1.0, abs(pct_b)),
                bollinger_pct_b=pct_b,
                atr_cents=float(atr),
                book_depth_at_entry=book.book_depth(levels=5),
                spread_cents=spread,
                spot_btc_usd=spot,
                minutes_to_settlement=mts,
                rsi_5m=rsi_5m,
                rsi_1h=rsi_1h,
                move_24h_pct=move_24h,
                cvd_1m_usd=cvd_1m,
                spot_range_60s=spot_range_60s,
                open_interest_usd=oi_usd,
            )
            snaps.append(MarketSnapshot(
                market_id=market_id,
                book=book,
                features=features,
                spot_btc_usd=spot,
                minutes_to_settlement=mts,
                strike_usd=strike,
                open_interest=latest_oi,
                liquidation_pressure=liq_pressure,
            ))
        return snaps

    def _build_liquidation_pressure(
        self, *, spot: float
    ) -> LiquidationPressure:
        """Aggregate the FeatureEngine liquidation deque around `spot`.

        `FeatureEngine` is a required FeedLoop dependency (enforced at
        `run_forever` construction time), so this always returns a
        concrete sample. Both totals default to 0.0 on cold start; the
        trap gate compares against `total >= threshold`, so zeros fail
        the check cleanly without special-casing.
        """
        signal = self._app.settings.signal
        window_pct = signal.liquidation_window_pct
        lookback = signal.liquidation_lookback_sec
        now_ns = self._clock.now_ns()
        long_below = self._features.liquidation_usd_in_window(
            now_ns=now_ns,
            lookback_sec=lookback,
            side="long",
            price_min=spot * (1.0 - window_pct),
            price_max=spot,
        )
        short_above = self._features.liquidation_usd_in_window(
            now_ns=now_ns,
            lookback_sec=lookback,
            side="short",
            price_min=spot,
            price_max=spot * (1.0 + window_pct),
        )
        return LiquidationPressure(
            long_usd_below_spot=long_below,
            short_usd_above_spot=short_above,
        )

    def _spot_range_60s(self) -> float | None:
        """max(price) - min(price) across the last 60s of primary ticks.

        Returns None while the window is empty (cold start); the implied-
        basis-arb trap treats None as "unknown — fail-closed" (i.e. veto),
        which matches the intent: without a range reading we can't prove
        the spot is quiet enough to trust fair value.
        """
        if not self._spot_history:
            return None
        prices = [p for _, p in self._spot_history]
        return max(prices) - min(prices)

    def _snapshot(self) -> MarketSnapshot | None:
        """Legacy single-book snapshot (primary only). Kept for tests and
        back-compat callers that don't want the cross-strike loop.
        """
        snaps = self._build_snapshots()
        for s in snaps:
            if s.market_id == self._market_id:
                return s
        return None


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
            markets = await discovery.list_btc_hourly_markets(
                now_ns=clock.now_ns(),
                series_ticker=series_ticker,
                btc_spot_usd=btc_spot,
                max_markets=_MAX_STRIKES_TRACKED,
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

        primary = markets[0]
        _log.info(
            "feedloop.session_start",
            market_id=primary.ticker,
            strike_usd=primary.strike_usd,
            settlement_ts_ns=primary.settlement_ts_ns,
            tracked_tickers=[m.ticker for m in markets],
            tracked_count=len(markets),
            btc_spot_usd=btc_spot,
            strike_gap_usd=abs(primary.strike_usd - btc_spot),
        )
        try:
            await _run_one_session(
                app=app,
                broker=broker,
                clock=clock,
                markets=markets,
                kalshi_ws_url=kalshi_ws_url,
                spot_oracle=spot_oracle,
                ws_connect=ws_connect,
                on_book_invalidate=on_book_invalidate,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover — defensive
            _log.error("feedloop.session_error", error=str(exc), market=primary.ticker)
            await asyncio.sleep(2.0)


async def _run_one_session(
    *,
    app: App,
    broker: Broker,
    clock: Clock,
    markets: list[HourlyMarket],
    kalshi_ws_url: str,
    spot_oracle: SpotOracle,
    ws_connect: WSConnect,
    on_book_invalidate: Callable[[str], None] | None,
) -> None:
    if not markets:
        raise ValueError("_run_one_session: markets must be non-empty")
    primary = markets[0]
    books: dict[str, L2Book] = {m.ticker: L2Book(m.ticker) for m in markets}

    def _invalidate(reason: str) -> None:
        # On WS reconnect every book in the session lost its delta stream
        # together (single multiplexed socket). Mark them all INVALID so
        # downstream features refuse to fire until a fresh snapshot arrives
        # per ticker (hard rule #9).
        for b in books.values():
            b.invalidate(reason)
        if on_book_invalidate is not None:
            on_book_invalidate(reason)

    # One StalenessTracker for the whole session — all N strikes share one
    # WS, so a broken socket stales them together. Per-ticker trackers
    # would false-positive quiet strikes and fail-open on genuine socket
    # death (a ticker that never prints could never report stale).
    kalshi_staleness = StalenessTracker(
        name="kalshi",
        clock=clock,
        threshold_ms=app.settings.feeds.kalshi.staleness_halt_ms,
    )

    kalshi_feed = KalshiFeed(
        ws_url=kalshi_ws_url,
        market_tickers=[m.ticker for m in markets],
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
        books=books,
        kalshi_feed=kalshi_feed,
        spot_oracle=spot_oracle,
        feature_engine=feature_engine,
        market_id=primary.ticker,
        strike_usd=primary.strike_usd,
        strikes={m.ticker: m.strike_usd for m in markets},
        settlement_ts_ns=primary.settlement_ts_ns,
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
        # Drop book references so the next session can register fresh ones
        # under next-hour tickers. If an open position still exists we keep
        # its book (hard rule #10 — operator must flatten).
        open_market_ids = {p.market_id for p in app.portfolio.open_positions}
        for ticker in list(books):
            if ticker not in open_market_ids:
                app.books.pop(ticker, None)


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
