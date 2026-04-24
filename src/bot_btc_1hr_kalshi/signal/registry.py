"""Trap registry: runs every registered trap against a snapshot and returns the
highest-confidence signal, or None if no trap fires.

`run_traps` evaluates a single MarketSnapshot — same-book, multi-trap
ranking uses `confidence` as the sole tiebreak (all candidates price
the same strike, so `edge_cents` is comparable 1:1).

`run_traps_cross_strike` picks the best opportunity across a set of
snapshots drawn from N strikes of the same hourly settlement. It ranks
by `edge_cents * confidence` — classic expected-edge weighting. Two
strikes on the same session are NOT priced the same, so raw edge_cents
comparison would systematically over-weight low-delta deep-OTM strikes
(where a 5c move from 8c→13c is a 60%+ return but the "edge" also rests
on thin liquidity). Scaling by confidence pulls that back toward setups
the trap actually trusts.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.config.settings import SignalSettings
from bot_btc_1hr_kalshi.signal.traps import (
    detect_ceiling_reversion,
    detect_cross_venue_lag,
    detect_floor_reversion,
    detect_implied_arb,
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
        enable_microstructure_gating=settings.enable_microstructure_gating,
        liquidation_cascade_threshold_usd=settings.liquidation_cascade_threshold_usd,
        oi_compression_threshold_usd=settings.oi_compression_threshold_usd,
    )
    if floor is not None:
        candidates.append(floor)

    ceiling = detect_ceiling_reversion(
        snap,
        min_confidence=min_confidence,
        htf_bullish_veto_rsi=settings.htf_bullish_veto_rsi,
        runaway_train_halt_pct=settings.runaway_train_halt_pct,
        cvd_1m_veto_threshold_usd=settings.cvd_1m_veto_threshold_usd,
        enable_microstructure_gating=settings.enable_microstructure_gating,
        liquidation_cascade_threshold_usd=settings.liquidation_cascade_threshold_usd,
        oi_compression_threshold_usd=settings.oi_compression_threshold_usd,
    )
    if ceiling is not None:
        candidates.append(ceiling)

    lag = detect_cross_venue_lag(snap, min_confidence=min_confidence)
    if lag is not None:
        candidates.append(lag)

    arb = detect_implied_arb(
        snap,
        basis_threshold_cents=settings.arb_basis_threshold_cents,
        dead_spot_range_usd=settings.arb_dead_spot_range_usd,
    )
    if arb is not None:
        candidates.append(arb)

    if not candidates:
        return None
    return max(candidates, key=lambda c: c.confidence)


def run_traps_cross_strike(
    snapshots: list[MarketSnapshot], *, settings: SignalSettings
) -> tuple[MarketSnapshot, TrapSignal] | None:
    """Evaluate every snapshot, return the best `(snapshot, signal)` pair.

    Best := max over all fired candidates of `edge_cents * confidence`.
    Ties break on (higher confidence, lower |strike - spot|, ticker) so
    the result is deterministic against candidate ordering.

    Returns None if no trap fires on any snapshot. All snapshots are
    expected to share the same `settlement_ts_ns` — mixing settlements
    would violate the correlation cap's identity; callers (the feed
    loop) enforce that upstream in market discovery.
    """
    best: tuple[MarketSnapshot, TrapSignal] | None = None
    best_key: tuple[float, float, float] | None = None
    # Iterate in stable ticker order so that when scores/confidence/gap
    # all tie, the alphabetically-earliest ticker wins via strict `>`.
    # Input-order independence makes journal diffs deterministic across
    # replays regardless of how discovery ordered its candidates.
    for snap in sorted(snapshots, key=lambda s: s.market_id):
        sig = run_traps(snap, settings=settings)
        if sig is None:
            continue
        score = sig.edge_cents * sig.confidence
        key = (
            score,
            sig.confidence,
            -abs(snap.strike_usd - snap.spot_btc_usd),
        )
        if best_key is None or key > best_key:
            best_key = key
            best = (snap, sig)
    return best
