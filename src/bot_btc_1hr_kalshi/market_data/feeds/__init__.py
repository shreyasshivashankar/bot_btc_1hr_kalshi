"""Feed handlers. Currently: MockFeed (test/replay). Kalshi/Coinbase/Binance WS feeds land in slice 2."""

from bot_btc_1hr_kalshi.market_data.feeds.base import Feed
from bot_btc_1hr_kalshi.market_data.feeds.mock import MockFeed

__all__ = ["Feed", "MockFeed"]
