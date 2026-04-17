"""Feed handlers: MockFeed for tests/replay, KalshiFeed for live L2/trades,
SpotFeed for Coinbase/Binance tickers."""

from bot_btc_1hr_kalshi.market_data.feeds.base import Feed
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed
from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import KalshiParseError, parse_frame
from bot_btc_1hr_kalshi.market_data.feeds.mock import MockFeed
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    SpotParseError,
    binance_parser,
    build_coinbase_subscribe,
    coinbase_parser,
    parse_binance,
    parse_coinbase,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker

__all__ = [
    "Feed",
    "KalshiFeed",
    "KalshiParseError",
    "MockFeed",
    "SpotFeed",
    "SpotParseError",
    "StalenessTracker",
    "binance_parser",
    "build_coinbase_subscribe",
    "coinbase_parser",
    "parse_binance",
    "parse_coinbase",
    "parse_frame",
]
