from __future__ import annotations

import math

import pytest

from bot_btc_1hr_kalshi.signal import FeatureEngine


def _engine(period: int = 5, std: float = 2.0, atr: int = 3) -> FeatureEngine:
    return FeatureEngine(bollinger_period=period, bollinger_std_mult=std, atr_period=atr)


def test_features_are_none_until_warmed_up() -> None:
    fe = _engine(period=5)
    fe.update_spot(100.0)
    fe.update_spot(101.0)
    assert fe.sma() is None
    assert fe.bollinger_bands() is None
    assert fe.bollinger_pct_b() is None


def test_sma_correct_when_warm() -> None:
    fe = _engine(period=5)
    for p in [100, 101, 102, 103, 104]:
        fe.update_spot(p)
    assert fe.sma() == pytest.approx(102.0)


def test_bollinger_bands_symmetric_around_sma() -> None:
    fe = _engine(period=5, std=2.0)
    for p in [100, 101, 102, 103, 104]:
        fe.update_spot(p)
    bands = fe.bollinger_bands()
    assert bands is not None
    lower, mid, upper = bands
    assert mid == pytest.approx(102.0)
    assert upper - mid == pytest.approx(mid - lower)


def test_bollinger_pct_b_near_midband_when_price_tracks_mean() -> None:
    # A tight, symmetric history around 102; the new tick lands close to mean,
    # so pct_b should be in the middle of [0, 1].
    fe = _engine(period=5, std=2.0)
    for p in [101.0, 102.0, 103.0, 102.0, 102.0]:
        fe.update_spot(p)
    fe.update_spot(102.0)
    pct_b = fe.bollinger_pct_b()
    assert pct_b is not None
    assert 0.3 < pct_b < 0.7


def test_bollinger_pct_b_flat_series_returns_midband() -> None:
    # std==0 path — `upper == lower`, so pct_b defaults to 0.5.
    fe = _engine(period=5, std=2.0)
    for _ in range(5):
        fe.update_spot(100.0)
    assert fe.bollinger_pct_b() == pytest.approx(0.5)


def test_bollinger_pct_b_below_lower_band_is_negative() -> None:
    # Large window so one outlier can't widen the band enough to contain itself.
    fe = _engine(period=10, std=2.0)
    for p in [100.0, 101.0, 99.0, 100.0, 101.0, 100.0, 99.0, 100.0, 101.0, 100.0]:
        fe.update_spot(p)
    fe.update_spot(50.0)  # outlier far below recent mean
    pct_b = fe.bollinger_pct_b()
    assert pct_b is not None
    assert pct_b < 0


def test_atr_warmup_and_compute() -> None:
    fe = _engine(atr=3)
    fe.update_spot(100.0)
    assert fe.atr() is None  # no true ranges yet
    fe.update_spot(103.0)  # tr = 3
    fe.update_spot(101.0)  # tr = 2
    assert fe.atr() is None  # need atr_period samples
    fe.update_spot(105.0)  # tr = 4 — now 3 tr samples
    assert fe.atr() == pytest.approx(3.0)


def test_regime_vol_classifies_by_atr() -> None:
    fe = FeatureEngine(
        bollinger_period=2,
        bollinger_std_mult=2.0,
        atr_period=3,
        atr_high_threshold_usd=50.0,
        atr_low_threshold_usd=5.0,
    )
    # Build a high-ATR sequence
    prices = [100.0, 200.0, 100.0, 200.0]
    for p in prices:
        fe.update_spot(p)
    assert fe.regime_vol() == "high"

    # Low-ATR sequence
    fe = FeatureEngine(
        bollinger_period=2,
        bollinger_std_mult=2.0,
        atr_period=3,
        atr_high_threshold_usd=50.0,
        atr_low_threshold_usd=5.0,
    )
    for p in [100.0, 100.5, 101.0, 100.5]:
        fe.update_spot(p)
    assert fe.regime_vol() == "low"


def test_regime_trend_up_down_flat() -> None:
    fe = _engine(period=10)
    for p in [100, 100, 100, 100, 100, 200, 200, 200, 200, 200]:
        fe.update_spot(float(p))
    assert fe.regime_trend(flat_threshold_usd=25.0) == "up"

    fe = _engine(period=10)
    for p in [200, 200, 200, 200, 200, 100, 100, 100, 100, 100]:
        fe.update_spot(float(p))
    assert fe.regime_trend(flat_threshold_usd=25.0) == "down"

    fe = _engine(period=10)
    for p in [100, 101, 100, 102, 99, 100, 101, 100, 99, 100]:
        fe.update_spot(float(p))
    assert fe.regime_trend(flat_threshold_usd=25.0) == "flat"


def test_update_spot_rejects_nonpositive() -> None:
    fe = _engine()
    with pytest.raises(ValueError):
        fe.update_spot(0.0)
    with pytest.raises(ValueError):
        fe.update_spot(-1.0)


def test_init_validates_args() -> None:
    with pytest.raises(ValueError):
        FeatureEngine(bollinger_period=1, bollinger_std_mult=2.0)
    with pytest.raises(ValueError):
        FeatureEngine(bollinger_period=5, bollinger_std_mult=0.0)
    with pytest.raises(ValueError):
        FeatureEngine(bollinger_period=5, bollinger_std_mult=2.0, atr_period=1)


def test_std_dev_matches_population_formula() -> None:
    fe = _engine(period=5)
    prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    for p in prices:
        fe.update_spot(p)
    expected_mean = 3.0
    expected_var = sum((p - expected_mean) ** 2 for p in prices) / len(prices)
    expected_std = math.sqrt(expected_var)
    assert fe.stddev() == pytest.approx(expected_std)


def test_running_sums_stay_in_sync_after_many_evictions() -> None:
    """Regression: the O(1) stddev relies on running sums that track the
    sliding window. A full sweep of prices well past `bollinger_period`
    must still match a fresh two-pass computation over the current window
    (within float tolerance)."""
    import random

    rng = random.Random(17)
    period = 200
    fe = _engine(period=period, std=2.0)
    all_prices = [60_000.0 + rng.uniform(-500.0, 500.0) for _ in range(5_000)]
    for p in all_prices:
        fe.update_spot(p)

    window = all_prices[-period:]
    ref_mean = sum(window) / period
    ref_std = math.sqrt(sum((p - ref_mean) ** 2 for p in window) / period)

    assert fe.sma() == pytest.approx(ref_mean, rel=1e-9)
    assert fe.stddev() == pytest.approx(ref_std, rel=1e-6)
