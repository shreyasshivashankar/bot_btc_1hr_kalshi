from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, MockFeed, SpotTick
from bot_btc_1hr_kalshi.obs import ManualClock
from bot_btc_1hr_kalshi.obs.money import usd_to_micros


def _book(seq: int, ts_ns: int) -> BookUpdate:
    return BookUpdate(
        seq=seq,
        ts_ns=ts_ns,
        market_id="BTC-1H",
        bids=(BookLevel(40, 1),),
        asks=(BookLevel(42, 1),),
        is_snapshot=True,
    )


@pytest.mark.asyncio
async def test_mock_feed_yields_events_in_order() -> None:
    events = [_book(1, 1_000), _book(2, 2_000), _book(3, 3_000)]
    feed = MockFeed(events)
    received = [ev async for ev in feed.events()]
    assert [e.seq for e in received] == [1, 2, 3]  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_mock_feed_advances_injected_clock() -> None:
    clock = ManualClock(start_ns=0)
    events = [_book(1, 1_000), _book(2, 5_000)]
    feed = MockFeed(events, clock=clock)
    async for ev in feed.events():
        assert clock.now_ns() == ev.ts_ns  # clock matches event time


@pytest.mark.asyncio
async def test_mock_feed_mixed_event_types() -> None:
    events = [
        _book(1, 1_000),
        SpotTick(ts_ns=2_000, venue="coinbase", price_micros=usd_to_micros(65_000.0), size=0.1),
        _book(2, 3_000),
    ]
    feed = MockFeed(events)
    received = [ev async for ev in feed.events()]
    assert len(received) == 3
    assert isinstance(received[1], SpotTick)
