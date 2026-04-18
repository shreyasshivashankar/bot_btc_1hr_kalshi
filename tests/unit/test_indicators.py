"""Unit tests for the pure indicator accumulators (Slice 8, Phase 1).

Covers:
  * RSI: warmup threshold, saturation at 100 (monotonic up), convergence
    toward 0 (monotonic down), plausible mid-range value, RMA seed/switch
    parity against a hand-computed reference.
  * Bollinger: warmup None → bands, pct_b zero-width convention,
    population-stddev formula parity, window-eviction consistency.
  * ATR: warmup, constant-range convergence, |high - prev_close| TR
    component (gap up/down).
  * Validation: rejects period < 2 and std_mult <= 0.
"""

from __future__ import annotations

import math

import pytest

from bot_btc_1hr_kalshi.signal.indicators import (
    ATRAccumulator,
    BollingerAccumulator,
    RSIAccumulator,
)

# ------------------------------- RSI ---------------------------------------


def test_rsi_none_until_period_plus_one_ingests() -> None:
    """period=14 deltas need 15 closes."""
    rsi = RSIAccumulator(period=14)
    for i in range(14):
        out = rsi.ingest(100.0 + i)
        assert out is None
    # 15th close produces the 14th delta → leaves warmup.
    out = rsi.ingest(114.0)
    assert out is not None
    assert rsi.value is not None


def test_rsi_monotonic_rise_saturates_at_100() -> None:
    rsi = RSIAccumulator(period=14)
    for i in range(30):
        rsi.ingest(100.0 + i)
    assert rsi.value == pytest.approx(100.0)


def test_rsi_monotonic_fall_converges_to_zero() -> None:
    rsi = RSIAccumulator(period=14)
    for i in range(30):
        rsi.ingest(1_000.0 - i)
    v = rsi.value
    assert v is not None and v < 1.0


def test_rsi_alternating_small_moves_near_50() -> None:
    rsi = RSIAccumulator(period=14)
    base = 78_000.0
    for i in range(60):
        px = base + (1.0 if i % 2 == 0 else 0.0)
        rsi.ingest(px)
    v = rsi.value
    assert v is not None
    assert 40.0 < v < 60.0


def test_rsi_matches_wilder_rma_formula_hand_computed() -> None:
    """Hand-compute Wilder RMA for a 3-period toy series to pin the math.

    Closes: 100, 102, 101, 104, 103, 106 (5 deltas).
    Deltas:       +2, -1, +3, -1, +3

    Seed phase (deltas 1..3, n = 1..period):
      After Δ=+2  → avg_gain=2.0, avg_loss=0.0
      After Δ=-1  → avg_gain=1.0, avg_loss=0.5
      After Δ=+3  → avg_gain=(1.0*2 + 3)/3 = 5/3, avg_loss=(0.5*2 + 0)/3 = 1/3

    RMA phase (Δ=-1, then Δ=+3):
      avg_gain = (5/3 * 2 + 0) / 3 = 10/9
      avg_loss = (1/3 * 2 + 1) / 3 = 5/9
      → RS = 2.0, RSI = 100 - 100/3 = 66.666...

      avg_gain = (10/9 * 2 + 3) / 3 = (20/9 + 27/9) / 3 = 47/27
      avg_loss = (5/9 * 2 + 0) / 3 = 10/27
      → RS = 4.7, RSI = 100 - 100/5.7 ≈ 82.4561
    """
    rsi = RSIAccumulator(period=3)
    for px in (100.0, 102.0, 101.0, 104.0):
        rsi.ingest(px)
    # After 4 closes → 3 deltas → exactly leaves warmup.
    v = rsi.value
    assert v is not None
    # avg_gain = 5/3, avg_loss = 1/3, RS = 5, RSI = 100 - 100/6 = 83.333...
    assert v == pytest.approx(100.0 - 100.0 / 6.0, rel=1e-9)

    rsi.ingest(103.0)  # Δ = -1 — first RMA step
    v = rsi.value
    assert v is not None
    assert v == pytest.approx(100.0 - 100.0 / 3.0, rel=1e-9)

    rsi.ingest(106.0)  # Δ = +3 — second RMA step
    v = rsi.value
    assert v is not None
    assert v == pytest.approx(100.0 - 100.0 / 5.7, rel=1e-9)


def test_rsi_all_gains_gives_100_exactly() -> None:
    """avg_loss == 0 branch: RSI is defined as 100.0."""
    rsi = RSIAccumulator(period=5)
    for px in (100.0, 101.0, 102.0, 103.0, 104.0, 105.0):
        rsi.ingest(px)
    assert rsi.value == 100.0


def test_rsi_ingest_returns_same_as_value_property() -> None:
    rsi = RSIAccumulator(period=5)
    for px in (100.0, 101.0, 99.0, 102.0, 98.0):
        rsi.ingest(px)
    ret = rsi.ingest(104.0)
    assert ret == rsi.value


def test_rsi_rejects_period_below_two() -> None:
    with pytest.raises(ValueError, match="period must be >= 2"):
        RSIAccumulator(period=1)


# --------------------------- Bollinger -------------------------------------


def test_bollinger_warmup_then_bands() -> None:
    bb = BollingerAccumulator(period=5, std_mult=2.0)
    for i in range(4):
        assert bb.ingest(100.0 + i) is None
    bands = bb.ingest(104.0)
    assert bands is not None
    lower, mid, upper = bands
    assert lower < mid < upper


def test_bollinger_population_stddev_matches_hand_formula() -> None:
    """Population (divide by N), not sample (N-1), per TradingView."""
    bb = BollingerAccumulator(period=5, std_mult=2.0)
    prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    for p in prices:
        bb.ingest(p)
    mean = 3.0
    pop_var = sum((p - mean) ** 2 for p in prices) / 5.0
    pop_std = math.sqrt(pop_var)
    bands = bb.bands
    assert bands is not None
    lower, mid, upper = bands
    assert mid == pytest.approx(mean)
    assert upper == pytest.approx(mean + 2.0 * pop_std)
    assert lower == pytest.approx(mean - 2.0 * pop_std)


def test_bollinger_constant_window_zero_width() -> None:
    bb = BollingerAccumulator(period=10, std_mult=2.0)
    for _ in range(15):
        bb.ingest(78_000.0)
    bands = bb.bands
    assert bands is not None
    lower, mid, upper = bands
    assert lower == pytest.approx(mid) == pytest.approx(upper)
    assert bb.pct_b(78_000.0) == pytest.approx(0.5)


def test_bollinger_pct_b_regions() -> None:
    bb = BollingerAccumulator(period=10, std_mult=2.0)
    for p in [100.0, 101.0, 99.0, 100.0, 101.0, 100.0, 99.0, 100.0, 101.0, 100.0]:
        bb.ingest(p)
    mid = bb.bands
    assert mid is not None
    _, midband, upper = mid
    assert bb.pct_b(midband) == pytest.approx(0.5, abs=1e-9)
    # Price far above upper band → pct_b > 1.
    assert bb.pct_b(upper + 5.0) is not None
    assert bb.pct_b(upper + 5.0) > 1.0  # type: ignore[operator]
    # Price far below lower band → pct_b < 0.
    assert bb.pct_b(50.0) is not None
    assert bb.pct_b(50.0) < 0  # type: ignore[operator]


def test_bollinger_sliding_window_stays_accurate() -> None:
    """After evictions, the running sum/sum_sq must match a fresh
    two-pass reference over the current window (within float tolerance)."""
    import random

    rng = random.Random(17)
    period = 50
    bb = BollingerAccumulator(period=period, std_mult=2.0)
    prices = [60_000.0 + rng.uniform(-500.0, 500.0) for _ in range(500)]
    for p in prices:
        bb.ingest(p)

    window = prices[-period:]
    ref_mean = sum(window) / period
    ref_var = sum((p - ref_mean) ** 2 for p in window) / period
    ref_std = math.sqrt(ref_var)

    bands = bb.bands
    assert bands is not None
    _lower, mid, upper = bands
    assert mid == pytest.approx(ref_mean, rel=1e-9)
    assert upper - mid == pytest.approx(2.0 * ref_std, rel=1e-6)


def test_bollinger_rejects_bad_args() -> None:
    with pytest.raises(ValueError, match="period must be >= 2"):
        BollingerAccumulator(period=1, std_mult=2.0)
    with pytest.raises(ValueError, match="std_mult must be > 0"):
        BollingerAccumulator(period=5, std_mult=0.0)
    with pytest.raises(ValueError, match="std_mult must be > 0"):
        BollingerAccumulator(period=5, std_mult=-1.0)


# ------------------------------ ATR ----------------------------------------


def test_atr_warmup_then_value() -> None:
    atr = ATRAccumulator(period=3)
    # First bar seeds prev_close only — no TR sample.
    assert atr.ingest(100.0, 100.0, 100.0) is None
    # Bars 2-3 give 2 TR samples — still warmup (need 3).
    assert atr.ingest(110.0, 90.0, 100.0) is None
    assert atr.ingest(110.0, 90.0, 100.0) is None
    # Bar 4 → 3 TRs → leaves warmup.
    v = atr.ingest(110.0, 90.0, 100.0)
    assert v is not None
    assert v == pytest.approx(20.0)


def test_atr_gap_up_uses_high_minus_prev_close() -> None:
    """Gap-up bars: high - prev_close dominates the TR max."""
    atr = ATRAccumulator(period=2)
    atr.ingest(100.0, 100.0, 100.0)  # seed prev_close = 100
    atr.ingest(105.0, 102.0, 104.0)  # TR = max(3, |105-100|, |102-100|) = 5
    v = atr.ingest(108.0, 105.0, 107.0)  # TR = max(3, |108-104|, |105-104|) = 4
    assert v is not None
    assert v == pytest.approx((5 + 4) / 2.0)


def test_atr_rma_phase_matches_hand_formula() -> None:
    """TR sequence under period=2: seed with 5, 3 → avg = 4; then RMA."""
    atr = ATRAccumulator(period=2)
    atr.ingest(100.0, 100.0, 100.0)  # seed, no TR
    atr.ingest(105.0, 100.0, 100.0)  # TR = 5 (high-low=5, |5|, |0|) — n=1, avg=5
    atr.ingest(103.0, 100.0, 100.0)  # TR = 3 — n=2, avg = (5+3)/2 = 4
    assert atr.value == pytest.approx(4.0)
    atr.ingest(102.0, 100.0, 100.0)  # TR = 2 — RMA: (4*1 + 2)/2 = 3
    assert atr.value == pytest.approx(3.0)


def test_atr_rejects_period_below_two() -> None:
    with pytest.raises(ValueError, match="period must be >= 2"):
        ATRAccumulator(period=1)
