"""Traps: predicate functions that turn a MarketSnapshot into a TrapSignal.

Slice 1 ships floor_reversion. Ceiling-reversion and cross-venue-lag land in slice 2.
"""

from bot_btc_1hr_kalshi.signal.traps.floor import detect_floor_reversion

__all__ = ["detect_floor_reversion"]
