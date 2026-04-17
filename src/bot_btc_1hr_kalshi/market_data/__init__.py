"""Market data: Kalshi WS + Coinbase/Binance spot feeds, L2 book, RTI, seq-gap detector.

On WS sequence gap, book-derived features are marked INVALID until a REST snapshot
rebuilds the book (hard rule #9). Staleness >2s on the primary feed halts trading.
"""

from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.feeds import Feed, MockFeed
from bot_btc_1hr_kalshi.market_data.types import (
    AggressorSide,
    BookLevel,
    BookUpdate,
    FeedEvent,
    SpotTick,
    TradeEvent,
    Venue,
)

__all__ = [
    "AggressorSide",
    "BookLevel",
    "BookUpdate",
    "Feed",
    "FeedEvent",
    "L2Book",
    "MockFeed",
    "SpotTick",
    "TradeEvent",
    "Venue",
]
