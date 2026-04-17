"""Traps: predicate functions that turn a MarketSnapshot into a TrapSignal."""

from bot_btc_1hr_kalshi.signal.traps.ceiling import detect_ceiling_reversion
from bot_btc_1hr_kalshi.signal.traps.floor import detect_floor_reversion
from bot_btc_1hr_kalshi.signal.traps.lag import detect_cross_venue_lag

__all__ = [
    "detect_ceiling_reversion",
    "detect_cross_venue_lag",
    "detect_floor_reversion",
]
