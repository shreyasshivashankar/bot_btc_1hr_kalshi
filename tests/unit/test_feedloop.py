"""Unit tests for the live feed loop.

We assert two things:
  * Every FeedEvent that arrives from Kalshi WS / spot WS is routed through
    App's mark_tick + the archive writer (if present) + the L2 book.
  * The session exits when the clock passes `settlement_ts_ns + grace`.
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import orjson
import pytest

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.feedloop import (
    FeedLoop,
    _watch_kalshi_staleness,
    minutes_to_settlement_fn,
)
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed, WSConnection
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    build_coinbase_subscribe,
    coinbase_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.activity import ActivityTracker
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import FeatureEngine

REPO_CONFIG = __import__("pathlib").Path(__file__).resolve().parents[2] / "config"


class FakeConn:
    """Yields prefab frames once, then blocks forever so the task stays alive
    until the feed loop cancels it at settlement."""

    def __init__(self, frames: list[bytes]) -> None:
        self._frames = list(frames)
        self.sent: list[bytes | str] = []
        self.closed = False

    async def send(self, data: bytes | str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[bytes]:
        for f in self._frames:
            yield f
        # Block forever so the consumer doesn't hit StopAsyncIteration and
        # trigger a reconnect mid-test; Event.wait() parks cheaply until the
        # outer task cancels us.
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True


def _kalshi_snapshot(seq: int) -> bytes:
    return orjson.dumps({
        "type": "orderbook_snapshot",
        "msg": {
            "market_ticker": "KXBTC-TEST",
            "seq": seq,
            "yes": [[40, 100]],
            "no": [[55, 100]],
        },
    })


def _coinbase_tick(price: float) -> bytes:
    return orjson.dumps({
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": str(price),
        "last_size": "0.01",
        "time": "2026-04-17T14:00:00.0Z",
    })


def _build_app(clock: ManualClock) -> tuple[App, PaperBroker]:
    settings = load_settings("paper", config_dir=REPO_CONFIG, env={
        "BOT_BTC_1HR_KALSHI_WS_URL": "wss://example/ws",
        "BOT_BTC_1HR_KALSHI_REST_URL": "https://example/rest",
    })
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=1000.0)
    broker = PaperBroker(clock=clock)
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)
    activity = ActivityTracker(boot_ns=clock.now_ns())
    return App(
        settings=settings, clock=clock, breakers=breakers, portfolio=portfolio,
        oms=oms, monitor=monitor, broker=broker, activity=activity,
    ), broker


@pytest.mark.asyncio
async def test_feedloop_routes_events_and_exits_at_settlement() -> None:
    start_ns = 1_800_000_000_000_000_000
    settlement_ns = start_ns + 2_000_000_000  # 2s into session
    clock = ManualClock(start_ns)
    app, broker = _build_app(clock)

    kalshi_conn = FakeConn([_kalshi_snapshot(1)])
    coinbase_conn = FakeConn([_coinbase_tick(60_000.0)])

    async def connect(url: str) -> WSConnection:
        if "ws-feed" in url or "coinbase" in url:
            return coinbase_conn
        return kalshi_conn

    kalshi_feed = KalshiFeed(
        ws_url="ws://kalshi/test",
        market_tickers=["KXBTC-TEST"],
        clock=clock,
        ws_connect=connect,
        staleness=StalenessTracker(name="kalshi", clock=clock, threshold_ms=2000),
    )
    coinbase_feed = SpotFeed(
        name="coinbase",
        ws_url="wss://ws-feed.exchange.coinbase.com",
        clock=clock,
        ws_connect=connect,
        staleness=StalenessTracker(name="coinbase", clock=clock, threshold_ms=2000),
        parse=coinbase_parser(clock),
        subscribe=build_coinbase_subscribe(["BTC-USD"]),
    )
    features = FeatureEngine(bollinger_period=20, bollinger_std_mult=2.0)
    book = L2Book("KXBTC-TEST")

    loop = FeedLoop(
        app=app,
        broker=broker,
        book=book,
        kalshi_feed=kalshi_feed,
        spot_feeds=[coinbase_feed],
        feature_engine=features,
        market_id="KXBTC-TEST",
        strike_usd=60_000.0,
        settlement_ts_ns=settlement_ns,
        clock=clock,
        grace_sec=0.1,
    )

    # Run the feed loop in the background; advance the clock past settlement
    # after giving it time to process the canned frames.
    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.3)  # let frames flow
    clock.set_ns(settlement_ns + 1_000_000_000)
    await asyncio.wait_for(run_task, timeout=5.0)

    # Book was rebuilt from the Kalshi snapshot.
    assert book.valid
    # Spot price made it into the feature engine.
    assert features.last_price == 60_000.0
    # Activity tracker saw at least one tick.
    snap = app.activity.snapshot(now_ns=clock.now_ns())  # type: ignore[union-attr]
    assert snap["last_tick_ns"] is not None
    # KalshiFeed subscribed.
    assert kalshi_conn.sent, "Kalshi feed should have sent subscribe frame"
    # Coinbase feed subscribed.
    assert coinbase_conn.sent, "Coinbase feed should have sent subscribe frame"


def test_minutes_to_settlement_fn_counts_down() -> None:
    fn = minutes_to_settlement_fn(settlement_ts_ns=60_000_000_000)
    assert fn(0) == 1.0
    assert fn(30_000_000_000) == pytest.approx(0.5)
    assert fn(60_000_000_000) == 0.0
    assert fn(70_000_000_000) == 0.0  # clamped to 0


@pytest.mark.asyncio
async def test_watch_kalshi_staleness_flips_breaker_on_transition() -> None:
    """Watchdog must trip feed_staleness when tracker reports stale, and
    release it when a fresh `mark()` makes the tracker non-stale again."""
    clock = ManualClock(0)
    breakers = BreakerState()
    tracker = StalenessTracker(name="kalshi", clock=clock, threshold_ms=100)
    tracker.mark()  # seed: not stale

    task = asyncio.create_task(
        _watch_kalshi_staleness(breakers=breakers, tracker=tracker, poll_sec=0.01)
    )
    try:
        # No staleness yet — breaker clear.
        await asyncio.sleep(0.03)
        assert not breakers.any_tripped(now_ns=clock.now_ns())

        # Advance clock past threshold; watchdog should trip the breaker.
        clock.advance_ns(200 * 1_000_000)
        await asyncio.sleep(0.05)
        assert breakers.any_tripped(now_ns=clock.now_ns())
        assert breakers.reason(now_ns=clock.now_ns()) == "feed_staleness"

        # Recovery: mark() makes the tracker non-stale; watchdog should clear.
        tracker.mark()
        await asyncio.sleep(0.05)
        assert not breakers.any_tripped(now_ns=clock.now_ns())
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
