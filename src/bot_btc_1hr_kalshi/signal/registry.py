"""Trap registry: runs every registered trap against a snapshot and returns the
highest-confidence signal, or None if no trap fires."""

from __future__ import annotations

from bot_btc_1hr_kalshi.config.settings import SignalSettings
from bot_btc_1hr_kalshi.signal.traps import (
    detect_ceiling_reversion,
    detect_cross_venue_lag,
    detect_floor_reversion,
)
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal


def run_traps(snap: MarketSnapshot, *, settings: SignalSettings) -> TrapSignal | None:
    """Run every registered trap and return the highest-confidence signal.

    HTF RSI veto thresholds, the Runaway Train cap, and the CVD tape-
    reader veto all live on `SignalSettings` (Slices 8 and 9); gates are
    enforced inside each trap so rejected candidates never enter the
    decision journal.
    """
    candidates: list[TrapSignal] = []
    min_confidence = settings.min_signal_confidence

    floor = detect_floor_reversion(
        snap,
        min_confidence=min_confidence,
        htf_bearish_veto_rsi=settings.htf_bearish_veto_rsi,
        cvd_1m_veto_threshold_usd=settings.cvd_1m_veto_threshold_usd,
    )
    if floor is not None:
        candidates.append(floor)

    ceiling = detect_ceiling_reversion(
        snap,
        min_confidence=min_confidence,
        htf_bullish_veto_rsi=settings.htf_bullish_veto_rsi,
        runaway_train_halt_pct=settings.runaway_train_halt_pct,
        cvd_1m_veto_threshold_usd=settings.cvd_1m_veto_threshold_usd,
    )
    if ceiling is not None:
        candidates.append(ceiling)

    lag = detect_cross_venue_lag(snap, min_confidence=min_confidence)
    if lag is not None:
        candidates.append(lag)

    if not candidates:
        return None
    return max(candidates, key=lambda c: c.confidence)
