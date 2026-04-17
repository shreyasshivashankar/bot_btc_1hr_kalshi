"""Feed protocol. Any feed is an async iterator of FeedEvent."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from bot_btc_1hr_kalshi.market_data.types import FeedEvent


class Feed(Protocol):
    def events(self) -> AsyncIterator[FeedEvent]:
        """Stream market events. Implementations handle reconnect/staleness internally."""
        ...
