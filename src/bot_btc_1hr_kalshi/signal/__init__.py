"""Signal: regime detection, traps (floor/ceiling/lag), feature engineering.

Edge hypothesis: structural mean-reversion at Bollinger extremes + opportunistic
cross-venue price lag. Every trap emits a DecisionRecord (hard rule #6).
"""

from bot_btc_1hr_kalshi.signal.features import FeatureEngine
from bot_btc_1hr_kalshi.signal.registry import run_traps, run_traps_cross_strike
from bot_btc_1hr_kalshi.signal.traps import (
    detect_ceiling_reversion,
    detect_cross_venue_lag,
    detect_floor_reversion,
)
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

__all__ = [
    "FeatureEngine",
    "MarketSnapshot",
    "TrapSignal",
    "detect_ceiling_reversion",
    "detect_cross_venue_lag",
    "detect_floor_reversion",
    "run_traps",
    "run_traps_cross_strike",
]
