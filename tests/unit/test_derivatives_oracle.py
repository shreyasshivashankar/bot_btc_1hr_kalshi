"""Unit tests for DerivativesOracle (PR-A + PR-B).

Mirrors the SpotOracle test suite — fail-closed accessor, subscriber
delivery, cold-catch-up semantics, unsubscribe, multi-feed run(). PR-B
adds liquidation-stream coverage: subscriber dispatch, no-warm-start
(discrete events, not snapshots), and concurrent OI+liq run().
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

import pytest

from bot_btc_1hr_kalshi.market_data.derivatives_oracle import (
    DerivativesOracle,
    DerivativesStaleError,
)
from bot_btc_1hr_kalshi.market_data.types import LiquidationEvent, OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _sample(ts_ns: int, *, oi: float = 1_000_000_000.0, source: str = "hyperliquid") -> OpenInterestSample:
    return OpenInterestSample(
        ts_ns=ts_ns,
        symbol="BTC",
        total_oi_usd=oi,
        exchanges_count=1,
        source=source,
    )


class FakeFeed:
    """Minimal `DerivativesFeed` stand-in: yields samples, then parks."""

    def __init__(self, samples: list[OpenInterestSample]) -> None:
        self._samples = list(samples)

    async def events(self) -> AsyncIterator[OpenInterestSample]:
        for s in self._samples:
            yield s
        # Block forever so gather() inside oracle.run() doesn't unwind.
        await asyncio.Event().wait()


def _oracle(clock: ManualClock, *feed_samples: list[OpenInterestSample]) -> DerivativesOracle:
    feeds = tuple(FakeFeed(s) for s in feed_samples)
    # type: ignore[arg-type] — FakeFeed duck-types DerivativesFeed.events()
    return DerivativesOracle(oi_feeds=feeds, clock=clock)  # type: ignore[arg-type]


def test_get_open_interest_cold_start_raises() -> None:
    clock = ManualClock(10_000_000_000)
    oracle = _oracle(clock)
    with pytest.raises(DerivativesStaleError, match="no derivatives"):
        oracle.get_open_interest(max_age_ms=1000)
    assert oracle.get_open_interest_or_none(max_age_ms=1000) is None
    assert oracle.latest_open_interest is None


def test_run_with_zero_feeds_returns_immediately() -> None:
    """Empty oracle is a valid configuration (dev/test). `run()` must not
    hang on an empty gather, otherwise startup wedges."""
    clock = ManualClock(0)
    oracle = _oracle(clock)
    asyncio.run(asyncio.wait_for(oracle.run(), timeout=0.5))


@pytest.mark.asyncio
async def test_get_open_interest_returns_fresh_sample() -> None:
    clock = ManualClock(10_000_000_000)
    sample = _sample(10_000_000_000, oi=42_000_000_000.0)
    oracle = _oracle(clock, [sample])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        got = oracle.get_open_interest(max_age_ms=1000)
        assert got.total_oi_usd == pytest.approx(42_000_000_000.0)
        assert oracle.get_open_interest_or_none(max_age_ms=1000) is got
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_staleness_contract_raises_past_threshold() -> None:
    t0 = 10_000_000_000
    clock = ManualClock(t0)
    oracle = _oracle(clock, [_sample(t0)])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        # 500ms later — well within 1s threshold
        clock.advance_ns(500_000_000)
        oracle.get_open_interest(max_age_ms=1000)
        # 2s later — past 1s threshold
        clock.advance_ns(2_000_000_000)
        with pytest.raises(DerivativesStaleError, match="stale"):
            oracle.get_open_interest(max_age_ms=1000)
        assert oracle.get_open_interest_or_none(max_age_ms=1000) is None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_subscribe_delivers_each_sample() -> None:
    clock = ManualClock(0)
    samples = [_sample(1_000), _sample(2_000), _sample(3_000)]
    oracle = _oracle(clock, samples)
    received: list[OpenInterestSample] = []
    oracle.subscribe_open_interest(received.append)
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert [s.ts_ns for s in received] == [1_000, 2_000, 3_000]
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_subscribe_after_first_sample_warm_starts_immediately() -> None:
    """A late subscriber gets the latest known sample on registration so
    it doesn't have to wait for the next push to warm up."""
    clock = ManualClock(0)
    oracle = _oracle(clock, [_sample(123)])
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        received: list[OpenInterestSample] = []
        oracle.subscribe_open_interest(received.append)
        assert len(received) == 1
        assert received[0].ts_ns == 123
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_unsubscribe_stops_delivery() -> None:
    clock = ManualClock(0)
    # Two samples spaced enough that the unsubscribe between them is observable.
    samples = [_sample(1_000), _sample(2_000)]

    received: list[OpenInterestSample] = []

    class GatedFeed:
        def __init__(self) -> None:
            self.gate = asyncio.Event()

        async def events(self) -> AsyncIterator[OpenInterestSample]:
            yield samples[0]
            await self.gate.wait()
            yield samples[1]
            await asyncio.Event().wait()

    feed = GatedFeed()
    oracle = DerivativesOracle(oi_feeds=(feed,), clock=clock)  # type: ignore[arg-type]
    unsub = oracle.subscribe_open_interest(received.append)
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert [s.ts_ns for s in received] == [1_000]
        unsub()
        feed.gate.set()
        await asyncio.sleep(0.05)
        # The second sample arrived but was NOT delivered (we unsubscribed).
        assert [s.ts_ns for s in received] == [1_000]
        assert oracle.latest_open_interest is not None
        assert oracle.latest_open_interest.ts_ns == 2_000
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_run_consumes_multiple_feeds_concurrently() -> None:
    """Two feeds (e.g. Hyperliquid + future Bybit) — both update the
    `latest_open_interest` slot; most-recent wins by virtue of arrival
    order. Both subscribers receive both feeds' samples."""
    clock = ManualClock(0)
    feed_a = [_sample(1_000, source="hyperliquid")]
    feed_b = [_sample(2_000, source="bybit")]
    oracle = _oracle(clock, feed_a, feed_b)
    received: list[OpenInterestSample] = []
    oracle.subscribe_open_interest(received.append)
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        sources_seen = {s.source for s in received}
        assert sources_seen == {"hyperliquid", "bybit"}
        # Latest-wins for the slot; both should have been written, the
        # final value is one of them — order is not deterministic across
        # concurrent feeds, just verify the slot is populated.
        assert oracle.latest_open_interest is not None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


# --- PR-B: liquidation-stream coverage --------------------------------------


def _liq(ts_ns: int, *, side: str = "long", price: float = 70_000.0,
         size_usd: float = 50_000.0) -> LiquidationEvent:
    return LiquidationEvent(
        ts_ns=ts_ns,
        symbol="BTC",
        side="long" if side == "long" else "short",
        price_usd=price,
        size_usd=size_usd,
        source="bybit",
    )


class FakeLiqFeed:
    def __init__(self, events: list[LiquidationEvent]) -> None:
        self._events = list(events)

    async def events(self) -> AsyncIterator[LiquidationEvent]:
        for e in self._events:
            yield e
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_subscribe_liquidations_delivers_each_event() -> None:
    clock = ManualClock(0)
    events = [_liq(1_000, side="long"), _liq(2_000, side="short")]
    feed = FakeLiqFeed(events)
    oracle = DerivativesOracle(liq_feeds=(feed,), clock=clock)  # type: ignore[arg-type]
    received: list[LiquidationEvent] = []
    oracle.subscribe_liquidations(received.append)
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert [(e.ts_ns, e.side) for e in received] == [(1_000, "long"), (2_000, "short")]
        assert oracle.latest_liquidation is not None
        assert oracle.latest_liquidation.ts_ns == 2_000
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_liquidation_subscribe_does_not_warm_start() -> None:
    """Liquidations are discrete events, not snapshots — a late
    subscriber must NOT receive replays of pre-subscribe events. The
    warm-start contract that applies to OI samples deliberately does
    not apply here, otherwise the FeatureEngine deque would double-
    count events on registration."""
    clock = ManualClock(0)
    feed = FakeLiqFeed([_liq(1_000), _liq(2_000)])
    oracle = DerivativesOracle(liq_feeds=(feed,), clock=clock)  # type: ignore[arg-type]
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        late: list[LiquidationEvent] = []
        oracle.subscribe_liquidations(late.append)
        # No replay — late subscriber starts empty even though latest_liquidation is populated.
        assert late == []
        assert oracle.latest_liquidation is not None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


@pytest.mark.asyncio
async def test_run_consumes_oi_and_liq_feeds_concurrently() -> None:
    """The oracle runs OI feeds and liquidation feeds in the same
    gather() — neither stream blocks the other."""
    clock = ManualClock(0)
    oi_feed = FakeFeed([_sample(1_000)])
    liq_feed = FakeLiqFeed([_liq(2_000)])
    oracle = DerivativesOracle(
        oi_feeds=(oi_feed,),  # type: ignore[arg-type]
        liq_feeds=(liq_feed,),  # type: ignore[arg-type]
        clock=clock,
    )
    oi_received: list[OpenInterestSample] = []
    liq_received: list[LiquidationEvent] = []
    oracle.subscribe_open_interest(oi_received.append)
    oracle.subscribe_liquidations(liq_received.append)
    task = asyncio.create_task(oracle.run())
    try:
        await asyncio.sleep(0.05)
        assert len(oi_received) == 1
        assert len(liq_received) == 1
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def test_unsubscribe_liquidations_stops_delivery() -> None:
    """Same unsubscribe contract as OI — returned callable removes the
    handler."""
    clock = ManualClock(0)
    oracle = DerivativesOracle(clock=clock)
    received: list[LiquidationEvent] = []
    unsub = oracle.subscribe_liquidations(received.append)
    # Manually fan out (no feed → no event loop needed).
    oracle._liq_cbs[0](_liq(1_000))  # type: ignore[attr-defined]
    unsub()
    # After unsub, the callback list is empty.
    assert oracle._liq_cbs == []  # type: ignore[attr-defined]
    assert len(received) == 1
