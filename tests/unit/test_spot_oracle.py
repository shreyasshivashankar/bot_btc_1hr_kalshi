"""Unit tests for SpotOracle (Slice 6).

Covers:
  * cold start: get_primary raises SpotStaleError before any tick arrives
  * fresh tick: get_primary returns price; get_primary_or_none mirrors
  * staleness contract: age > max_age_ms → SpotStaleError
  * subscribe callback delivery (primary + confirmation)
  * subscribe-after-first-tick cold-catch-up semantics
  * unsubscribe removes the callback
  * run() consumes both feeds and routes primary/confirmation separately
"""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest

from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotOracle, SpotStaleError
from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.money import usd_to_micros


def _tick(ts_ns: int, venue: str, price: float = 60_000.0) -> SpotTick:
    return SpotTick(
        ts_ns=ts_ns,
        venue=venue,  # type: ignore[arg-type]
        price_micros=usd_to_micros(price),
        size=0.01,
    )


class FakeFeed:
    """Minimal SpotFeed stand-in: yields ticks from an injected list forever."""

    def __init__(self, ticks: list[SpotTick]) -> None:
        self._ticks = list(ticks)

    async def events(self) -> AsyncIterator[SpotTick]:
        for t in self._ticks:
            yield t
        # Park forever so the gather doesn't wind down mid-test.
        await asyncio.Event().wait()


def _oracle(clock: ManualClock, primary_ticks: list[SpotTick], confirm_ticks: list[SpotTick]) -> SpotOracle:
    # type: ignore[arg-type] — FakeFeed duck-types SpotFeed.events()
    return SpotOracle(
        primary=FakeFeed(primary_ticks),  # type: ignore[arg-type]
        confirmation=FakeFeed(confirm_ticks),  # type: ignore[arg-type]
        clock=clock,
    )


def test_get_primary_cold_start_raises() -> None:
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(clock, [], [])
    with pytest.raises(SpotStaleError, match="no primary"):
        oracle.get_primary(max_age_ms=1000)
    assert oracle.get_primary_or_none(max_age_ms=1000) is None


@pytest.mark.asyncio
async def test_get_primary_returns_fresh_price() -> None:
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(clock, [_tick(10_000_000_000, "coinbase", 78_123.5)], [])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert oracle.get_primary(max_age_ms=1000) == pytest.approx(78_123.5)
        assert oracle.get_primary_or_none(max_age_ms=1000) == pytest.approx(78_123.5)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_staleness_contract_raises_past_threshold() -> None:
    t0 = 10_000_000_000
    clock = ManualClock(t0)
    oracle = _oracle(clock, [_tick(t0, "coinbase", 78_000.0)], [])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        # Advance 2s — threshold is 1000ms.
        clock.advance_ns(2_000_000_000)
        with pytest.raises(SpotStaleError, match="stale"):
            oracle.get_primary(max_age_ms=1000)
        assert oracle.get_primary_or_none(max_age_ms=1000) is None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_primary_callback_delivery_and_routing() -> None:
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(
        clock,
        [_tick(10_000_000_000, "coinbase", 78_000.0)],
        [_tick(10_000_000_000, "kraken", 78_050.0)],
    )
    primary_seen: list[float] = []
    confirm_seen: list[float] = []
    oracle.subscribe_primary(lambda t: primary_seen.append(t.price_usd))
    oracle.subscribe_confirmation(lambda t: confirm_seen.append(t.price_usd))

    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert primary_seen == [pytest.approx(78_000.0)]
        assert confirm_seen == [pytest.approx(78_050.0)]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_subscribe_after_first_tick_gets_cold_catchup() -> None:
    """Late subscribers get the current cached tick immediately so the
    feature engine doesn't sit waiting for the next Coinbase print on
    hour boundaries."""
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(clock, [_tick(10_000_000_000, "coinbase", 78_000.0)], [])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        seen: list[float] = []
        oracle.subscribe_primary(lambda t: seen.append(t.price_usd))
        assert seen == [pytest.approx(78_000.0)]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_unsubscribe_stops_callbacks() -> None:
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(
        clock,
        [
            _tick(10_000_000_000, "coinbase", 78_000.0),
            _tick(10_000_000_001, "coinbase", 78_010.0),
        ],
        [],
    )
    seen: list[float] = []
    unsub = oracle.subscribe_primary(lambda t: seen.append(t.price_usd))
    unsub()  # immediate: second tick should NOT fire the cb
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert seen == []
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_oracle_end_to_end_with_real_spot_feeds() -> None:
    """Wire real SpotFeed instances with stubbed WS connections end-to-end
    through the oracle — catches shape mismatches between SpotFeed.events()
    and the oracle's consumer loop."""
    import orjson

    from bot_btc_1hr_kalshi.market_data.feeds.kalshi import WSConnection

    clock = ManualClock(1_700_000_000_000_000_000)

    class FakeConn:
        def __init__(self, frames: list[bytes]) -> None:
            self._frames = frames
            self.sent: list[object] = []

        async def send(self, data: object) -> None:
            self.sent.append(data)

        def __aiter__(self) -> AsyncIterator[bytes]:
            return self._iter()

        async def _iter(self) -> AsyncIterator[bytes]:
            for f in self._frames:
                yield f
            await asyncio.Event().wait()

        async def close(self) -> None:
            pass

    coinbase_conn = FakeConn([orjson.dumps({
        "type": "ticker", "product_id": "BTC-USD", "price": "78000.0",
        "last_size": "0.01", "time": "2026-04-17T14:00:00.0Z",
    })])
    kraken_conn = FakeConn([orjson.dumps({
        "channel": "trade", "type": "update",
        "data": [{"symbol": "BTC/USD", "price": 78_050.0, "qty": 0.01,
                  "timestamp": "2026-04-17T14:00:00Z"}],
    })])

    async def connect(url: str) -> WSConnection:
        if "kraken" in url:
            return kraken_conn  # type: ignore[return-value]
        return coinbase_conn  # type: ignore[return-value]

    coinbase_feed = SpotFeed(
        name="coinbase", ws_url="wss://coinbase",
        clock=clock, ws_connect=connect,
        staleness=StalenessTracker(name="coinbase", clock=clock, threshold_ms=2000),
        parse=coinbase_parser(clock),
        subscribe=build_coinbase_subscribe(["BTC-USD"]),
    )
    kraken_feed = SpotFeed(
        name="kraken", ws_url="wss://kraken",
        clock=clock, ws_connect=connect,
        staleness=StalenessTracker(name="kraken", clock=clock, threshold_ms=5000),
        parse=kraken_parser(clock),
        subscribe=build_kraken_subscribe(["BTC/USD"]),
    )
    oracle = SpotOracle(primary=coinbase_feed, confirmation=kraken_feed, clock=clock)

    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.1)
        # Oracle's internal clock sees the configured start_ns and the
        # tick's ts_ns was parsed from the ISO string (an older t); we
        # verify both sides arrived, not the exact age.
        assert oracle.latest_primary_tick is not None
        assert oracle.latest_confirmation_tick is not None
        assert oracle.latest_primary_tick.price_usd == pytest.approx(78_000.0)
        assert oracle.latest_confirmation_tick.price_usd == pytest.approx(78_050.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
