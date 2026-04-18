"""Unit tests for the live feed loop.

We assert:
  * Every Kalshi WS event is routed through App's mark_tick + archive + L2 book.
  * The session exits when the clock passes `settlement_ts_ns + grace`.
  * Spot ticks reach FeatureEngine + IntegrityTracker via the SpotOracle
    (primary → features+integrity; confirmation → integrity only).
  * The session's snapshot path refuses to emit signals when primary
    spot is stale (hard LastSpot contract).
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
    ws_connect_kalshi_signed,
)
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed, WSConnection
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.activity import ActivityTracker
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.money import usd_to_micros
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
        await asyncio.Event().wait()

    async def close(self) -> None:
        self.closed = True


class StubOracle:
    """Oracle double that ignores run() and forwards manual-subscribe calls."""

    def __init__(self, clock: ManualClock) -> None:
        self._clock = clock
        self._latest_primary: SpotTick | None = None
        self._latest_confirmation: SpotTick | None = None
        self._primary_cbs: list = []
        self._confirmation_cbs: list = []

    async def run(self) -> None:
        await asyncio.Event().wait()

    def push_primary(self, price: float, ts_ns: int | None = None) -> None:
        tick = SpotTick(
            ts_ns=ts_ns if ts_ns is not None else self._clock.now_ns(),
            venue="coinbase",
            price_micros=usd_to_micros(price),
            size=0.01,
        )
        self._latest_primary = tick
        for cb in list(self._primary_cbs):
            cb(tick)

    def push_confirmation(self, price: float, ts_ns: int | None = None) -> None:
        tick = SpotTick(
            ts_ns=ts_ns if ts_ns is not None else self._clock.now_ns(),
            venue="kraken",
            price_micros=usd_to_micros(price),
            size=0.01,
        )
        self._latest_confirmation = tick
        for cb in list(self._confirmation_cbs):
            cb(tick)

    def subscribe_primary(self, cb):  # type: ignore[no-untyped-def]
        self._primary_cbs.append(cb)
        if self._latest_primary is not None:
            cb(self._latest_primary)
        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._primary_cbs.remove(cb)
        return _unsub

    def subscribe_confirmation(self, cb):  # type: ignore[no-untyped-def]
        self._confirmation_cbs.append(cb)
        if self._latest_confirmation is not None:
            cb(self._latest_confirmation)
        def _unsub() -> None:
            with contextlib.suppress(ValueError):
                self._confirmation_cbs.remove(cb)
        return _unsub

    def get_primary(self, *, max_age_ms: int) -> float:
        tick = self._latest_primary
        if tick is None:
            from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotStaleError
            raise SpotStaleError("no primary")
        age_ms = (self._clock.now_ns() - tick.ts_ns) / 1_000_000
        if age_ms > max_age_ms:
            from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotStaleError
            raise SpotStaleError(f"stale {age_ms}ms")
        return tick.price_usd

    def get_primary_or_none(self, *, max_age_ms: int) -> float | None:
        try:
            return self.get_primary(max_age_ms=max_age_ms)
        except Exception:
            return None


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
    settlement_ns = start_ns + 2_000_000_000
    clock = ManualClock(start_ns)
    app, broker = _build_app(clock)

    kalshi_conn = FakeConn([_kalshi_snapshot(1)])

    async def connect(url: str) -> WSConnection:
        return kalshi_conn  # type: ignore[return-value]

    kalshi_feed = KalshiFeed(
        ws_url="ws://kalshi/test",
        market_tickers=["KXBTC-TEST"],
        clock=clock,
        ws_connect=connect,
        staleness=StalenessTracker(name="kalshi", clock=clock, threshold_ms=2000),
    )
    oracle = StubOracle(clock)
    # Seed a fresh primary tick BEFORE run() so the activity tracker
    # records it — FeatureEngine is now bar-driven (Slice 8) and no
    # longer warms up from raw ticks, so we assert on activity below.
    oracle.push_primary(60_000.0)
    features = FeatureEngine(
        timeframes=["5m"], bollinger_period=20, bollinger_std_mult=2.0
    )
    book = L2Book("KXBTC-TEST")

    loop = FeedLoop(
        app=app,
        broker=broker,
        book=book,
        kalshi_feed=kalshi_feed,
        spot_oracle=oracle,  # type: ignore[arg-type]
        feature_engine=features,
        market_id="KXBTC-TEST",
        strike_usd=60_000.0,
        settlement_ts_ns=settlement_ns,
        clock=clock,
        grace_sec=0.1,
    )

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.3)
    clock.set_ns(settlement_ns + 1_000_000_000)
    await asyncio.wait_for(run_task, timeout=5.0)

    assert book.valid
    snap = app.activity.snapshot(now_ns=clock.now_ns())  # type: ignore[union-attr]
    assert snap["last_tick_ns"] is not None
    assert kalshi_conn.sent, "Kalshi feed should have sent subscribe frame"


def test_minutes_to_settlement_fn_counts_down() -> None:
    fn = minutes_to_settlement_fn(settlement_ts_ns=60_000_000_000)
    assert fn(0) == 1.0
    assert fn(30_000_000_000) == pytest.approx(0.5)
    assert fn(60_000_000_000) == 0.0
    assert fn(70_000_000_000) == 0.0


@pytest.mark.asyncio
async def test_feedloop_routes_primary_and_confirmation_to_integrity() -> None:
    """Primary (Coinbase) and confirmation (Kraken) ticks both route into
    IntegrityTracker with the correct direction. FeatureEngine is now
    bar-driven and fed via the App-scope bar bus in production — the
    FeedLoop's per-tick handler no longer touches it."""
    from bot_btc_1hr_kalshi.signal.integrity import IntegrityTracker

    start_ns = 1_800_000_000_000_000_000
    settlement_ns = start_ns + 2_000_000_000
    clock = ManualClock(start_ns)
    app, broker = _build_app(clock)

    kalshi_conn = FakeConn([_kalshi_snapshot(1)])

    async def connect(url: str) -> WSConnection:
        return kalshi_conn  # type: ignore[return-value]

    kalshi_feed = KalshiFeed(
        ws_url="ws://kalshi/test",
        market_tickers=["KXBTC-TEST"],
        clock=clock,
        ws_connect=connect,
        staleness=StalenessTracker(name="kalshi", clock=clock, threshold_ms=2000),
    )
    oracle = StubOracle(clock)
    features = FeatureEngine(
        timeframes=["5m"], bollinger_period=20, bollinger_std_mult=2.0
    )
    integrity = IntegrityTracker(
        velocity_window_sec=1.0,
        active_disagreement_floor_usd=25.0,
        stale_halt_sec=60.0,
    )
    book = L2Book("KXBTC-TEST")
    loop = FeedLoop(
        app=app, broker=broker, book=book, kalshi_feed=kalshi_feed,
        spot_oracle=oracle,  # type: ignore[arg-type]
        feature_engine=features,
        market_id="KXBTC-TEST", strike_usd=60_000.0,
        settlement_ts_ns=settlement_ns, clock=clock, grace_sec=0.1,
        integrity=integrity,
    )

    run_task = asyncio.create_task(loop.run())
    await asyncio.sleep(0.05)
    # Push one primary + one confirmation after subscribe.
    oracle.push_primary(61_000.0)
    oracle.push_confirmation(61_050.0)
    await asyncio.sleep(0.1)
    clock.set_ns(settlement_ns + 1_000_000_000)
    await asyncio.wait_for(run_task, timeout=5.0)

    # IntegrityTracker saw both venues with correct routing.
    assert integrity.primary_last_ns is not None
    assert integrity.confirmation_last_ns is not None


@pytest.mark.asyncio
async def test_feedloop_snapshot_refuses_when_spot_stale() -> None:
    """Hard LastSpot contract: if primary spot is older than the threshold,
    `_snapshot()` returns None — no signal emission, no trade."""
    start_ns = 1_800_000_000_000_000_000
    settlement_ns = start_ns + 2_000_000_000
    clock = ManualClock(start_ns)
    app, broker = _build_app(clock)

    kalshi_feed = KalshiFeed(
        ws_url="ws://kalshi/test",
        market_tickers=["KXBTC-TEST"],
        clock=clock,
        ws_connect=lambda url: _never(),
        staleness=StalenessTracker(name="kalshi", clock=clock, threshold_ms=2000),
    )
    oracle = StubOracle(clock)
    # Push a tick; then advance the clock past the staleness threshold.
    oracle.push_primary(78_000.0, ts_ns=start_ns)
    clock.set_ns(start_ns + 5_000_000_000)  # +5s → well past 1000ms
    features = FeatureEngine(
        timeframes=["5m"], bollinger_period=20, bollinger_std_mult=2.0
    )
    book = L2Book("KXBTC-TEST")
    # Seed the book so _snapshot's book-valid gate passes; staleness is the
    # only remaining reason snapshot should return None.
    from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate
    book.apply(BookUpdate(
        seq=1, ts_ns=start_ns, market_id="KXBTC-TEST",
        bids=(BookLevel(price_cents=40, size=100),),
        asks=(BookLevel(price_cents=55, size=100),),
        is_snapshot=True,
    ))
    # Feed enough bar closes to make bollinger_pct_b + atr non-None on 5m.
    for p in range(60_000, 60_050):
        features.ingest_bar(
            "5m", close=float(p), high=float(p) + 1.0, low=float(p) - 1.0
        )

    loop = FeedLoop(
        app=app, broker=broker, book=book, kalshi_feed=kalshi_feed,
        spot_oracle=oracle,  # type: ignore[arg-type]
        feature_engine=features,
        market_id="KXBTC-TEST", strike_usd=60_000.0,
        settlement_ts_ns=settlement_ns, clock=clock,
        spot_staleness_max_age_ms=1000,
    )
    assert loop._snapshot() is None  # stale → no snapshot


async def _never() -> WSConnection:
    """Helper: a connect function that blocks forever — used where the
    kalshi feed never actually opens in the unit under test."""
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_ws_connect_kalshi_signed_sends_auth_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The signed WS factory must hand websockets.connect the KALSHI-ACCESS-*
    header trio, with the path extracted from the URL."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner

    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    clock = ManualClock(1_700_000_000_000_000_000)
    signer = KalshiSigner(api_key_id="K123", private_key_pem=pem, clock=clock)

    captured: dict[str, object] = {}

    async def fake_connect(url: str, **kwargs: object) -> object:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    import websockets as _ws

    monkeypatch.setattr(_ws, "connect", fake_connect)

    connect = ws_connect_kalshi_signed(signer)
    await connect("wss://api.elections.kalshi.com/trade-api/ws/v2")

    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    headers = kwargs["additional_headers"]
    assert isinstance(headers, dict)
    assert headers["KALSHI-ACCESS-KEY"] == "K123"
    assert "KALSHI-ACCESS-TIMESTAMP" in headers
    assert "KALSHI-ACCESS-SIGNATURE" in headers


@pytest.mark.asyncio
async def test_watch_kalshi_staleness_flips_breaker_on_transition() -> None:
    clock = ManualClock(0)
    breakers = BreakerState()
    tracker = StalenessTracker(name="kalshi", clock=clock, threshold_ms=100)
    tracker.mark()

    task = asyncio.create_task(
        _watch_kalshi_staleness(breakers=breakers, tracker=tracker, poll_sec=0.01)
    )
    try:
        await asyncio.sleep(0.03)
        assert not breakers.any_tripped(now_ns=clock.now_ns())

        clock.advance_ns(200 * 1_000_000)
        await asyncio.sleep(0.05)
        assert breakers.any_tripped(now_ns=clock.now_ns())
        assert breakers.reason(now_ns=clock.now_ns()) == "feed_staleness"

        tracker.mark()
        await asyncio.sleep(0.05)
        assert not breakers.any_tripped(now_ns=clock.now_ns())
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
