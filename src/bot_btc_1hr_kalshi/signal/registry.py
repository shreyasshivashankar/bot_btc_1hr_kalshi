"""Trap registry: runs every registered trap against a snapshot and returns the
highest-confidence signal, or None if no trap fires."""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.traps import (
    detect_ceiling_reversion,
    detect_cross_venue_lag,
    detect_floor_reversion,
)
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal


def run_traps(snap: MarketSnapshot, *, min_confidence: float) -> TrapSignal | None:
    candidates: list[TrapSignal] = []

    floor = detect_floor_reversion(snap, min_confidence=min_confidence)
    if floor is not None:
        candidates.append(floor)

    ceiling = detect_ceiling_reversion(snap, min_confidence=min_confidence)
    if ceiling is not None:
        candidates.append(ceiling)

    lag = detect_cross_venue_lag(snap, min_confidence=min_confidence)
    if lag is not None:
        candidates.append(lag)

    if not candidates:
        return None
    return max(candidates, key=lambda c: c.confidence)
