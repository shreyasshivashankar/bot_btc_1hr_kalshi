"""Unit tests for DerivativesOracle (PR-A).

Mirrors the SpotOracle test suite — fail-closed accessor, subscriber
delivery, cold-catch-up semantics, unsubscribe, multi-feed run().
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
from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
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
    return DerivativesOracle(feeds=feeds, clock=clock)  # type: ignore[arg-type]


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
    oracle = DerivativesOracle(feeds=(feed,), clock=clock)  # type: ignore[arg-type]
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
