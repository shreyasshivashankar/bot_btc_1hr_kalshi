"""MockFeed: deterministic replay of a pre-built event list.

Used by unit and integration tests, and by the replay-mode backtest driver
before real WS adapters land. Honors the injected clock: before yielding each
event, the feed advances the clock to `event.ts_ns` so downstream code sees
monotonic timestamps matching the data.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence

from bot_btc_1hr_kalshi.market_data.types import FeedEvent
from bot_btc_1hr_kalshi.obs import ManualClock


class MockFeed:
    def __init__(self, events: Sequence[FeedEvent], clock: ManualClock | None = None) -> None:
        self._events: list[FeedEvent] = list(events)
        self._clock = clock

    async def events(self) -> AsyncIterator[FeedEvent]:
        for ev in self._events:
            if self._clock is not None:
                ts = ev.ts_ns
                if ts >= self._clock.now_ns():
                    self._clock.set_ns(ts)
            yield ev
