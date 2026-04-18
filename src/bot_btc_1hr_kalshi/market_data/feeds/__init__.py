"""Feed handlers: MockFeed for tests/replay, KalshiFeed for live L2/trades,
SpotFeed for Coinbase (primary) / Kraken (confirmation) tickers."""

from bot_btc_1hr_kalshi.market_data.feeds.base import Feed
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed
from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import KalshiParseError, parse_frame
from bot_btc_1hr_kalshi.market_data.feeds.mock import MockFeed
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    SpotParseError,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
    parse_coinbase,
    parse_kraken,
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
    "build_coinbase_subscribe",
    "build_kraken_subscribe",
    "coinbase_parser",
    "kraken_parser",
    "parse_coinbase",
    "parse_frame",
    "parse_kraken",
]
