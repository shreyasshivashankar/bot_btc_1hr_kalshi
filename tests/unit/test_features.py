"""Unit tests for the bar-driven FeatureEngine (Slice 8, Phase 2).

Covers the TF-keyed public surface — construction validation, per-TF
accumulator routing, `regime_vol`/`regime_trend` classification from the
rolling window, `move_24h_pct` on the 1h deque, and `attach(bus)`
end-to-end through `MultiTimeframeBus`.
"""

from __future__ import annotations

import math

import pytest

from bot_btc_1hr_kalshi.market_data.bars import MultiTimeframeBus
from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.obs.money import usd_to_micros
from bot_btc_1hr_kalshi.signal import FeatureEngine
from bot_btc_1hr_kalshi.signal.features import MOVE_24H_WINDOW_BARS


def _engine(
    *,
    timeframes: list[str] | None = None,
    bb_period: int = 5,
    std: float = 2.0,
    atr_period: int = 3,
    rsi_period: int = 14,
    atr_hi: float = 150.0,
    atr_lo: float = 30.0,
) -> FeatureEngine:
    return FeatureEngine(
        timeframes=timeframes if timeframes is not None else ["5m"],
        bollinger_period=bb_period,
        bollinger_std_mult=std,
        atr_period=atr_period,
        rsi_period=rsi_period,
        atr_high_threshold_usd=atr_hi,
        atr_low_threshold_usd=atr_lo,
    )


def _push_closes(fe: FeatureEngine, tf: str, closes: list[float]) -> None:
    """Helper: ingest closes with zero-range bars (high=low=close)."""
    for c in closes:
        fe.ingest_bar(tf, close=c, high=c, low=c)


# ------------------------ construction guards -----------------------------


def test_rejects_empty_timeframes() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        FeatureEngine(timeframes=[], bollinger_period=5, bollinger_std_mult=2.0)


def test_rejects_duplicate_timeframes() -> None:
    with pytest.raises(ValueError, match="must be unique"):
        FeatureEngine(
            timeframes=["5m", "5m"], bollinger_period=5, bollinger_std_mult=2.0
        )


def test_rejects_unknown_timeframe_label() -> None:
    with pytest.raises(ValueError, match="unknown timeframe"):
        FeatureEngine(
            timeframes=["7m"], bollinger_period=5, bollinger_std_mult=2.0
        )


def test_ingest_bar_rejects_unconfigured_tf() -> None:
    fe = _engine(timeframes=["5m"])
    with pytest.raises(KeyError, match="not configured"):
        fe.ingest_bar("1h", close=100.0, high=100.0, low=100.0)


# ------------------------------ warmup ------------------------------------


def test_all_readers_return_none_during_warmup() -> None:
    fe = _engine(bb_period=5, atr_period=3, rsi_period=14)
    fe.ingest_bar("5m", close=100.0, high=101.0, low=99.0)
    fe.ingest_bar("5m", close=100.5, high=101.0, low=100.0)
    assert fe.rsi("5m") is None
    assert fe.bollinger_bands("5m") is None
    assert fe.bollinger_pct_b("5m") is None
    assert fe.atr("5m") is None


def test_readers_none_for_unknown_tf_no_keyerror() -> None:
    """Unknown-TF queries fall through to None, never raise."""
    fe = _engine(timeframes=["5m"])
    assert fe.rsi("1h") is None
    assert fe.bollinger_bands("1h") is None
    assert fe.bollinger_pct_b("1h") is None
    assert fe.atr("1h") is None
    assert fe.last_close("1h") is None
    assert fe.regime_vol("1h") == "normal"
    assert fe.regime_trend("1h") == "flat"


# ------------------------- bollinger on closes -----------------------------


def test_bollinger_bands_symmetric_around_sma() -> None:
    fe = _engine(bb_period=5, std=2.0)
    _push_closes(fe, "5m", [100.0, 101.0, 102.0, 103.0, 104.0])
    bands = fe.bollinger_bands("5m")
    assert bands is not None
    lower, mid, upper = bands
    assert mid == pytest.approx(102.0)
    assert upper - mid == pytest.approx(mid - lower)


def test_bollinger_pct_b_flat_series_returns_midband() -> None:
    fe = _engine(bb_period=5, std=2.0)
    _push_closes(fe, "5m", [100.0] * 6)
    assert fe.bollinger_pct_b("5m") == pytest.approx(0.5)


def test_bollinger_pct_b_below_band_is_negative() -> None:
    fe = _engine(bb_period=10, std=2.0)
    _push_closes(
        fe,
        "5m",
        [100.0, 101.0, 99.0, 100.0, 101.0, 100.0, 99.0, 100.0, 101.0, 100.0],
    )
    # Outlier close far below the rolling mean — pct_b drops below 0.
    fe.ingest_bar("5m", close=50.0, high=50.0, low=50.0)
    pct_b = fe.bollinger_pct_b("5m")
    assert pct_b is not None and pct_b < 0


def test_bollinger_pct_b_uses_live_price_when_provided() -> None:
    """Regression for the bar-close lag: penetration must follow live spot.

    Without a `live_price`, pct_b is frozen at the last bar's close — so
    a price move into / out of the bands inside the bar window is invisible
    to the floor / ceiling / lag traps until the next 5m close. Passing
    `live_price=spot` lets the same bands score the live tape instead.
    """
    fe = _engine(bb_period=5, std=2.0)
    # Closes hug 100 → bands tight around 100. Last close = 100.
    _push_closes(fe, "5m", [100.0, 100.0, 100.0, 100.0, 100.0])
    bands = fe.bollinger_bands("5m")
    assert bands is not None
    # Constant series → zero-width bands → pct_b == 0.5 by convention,
    # regardless of which price we score against. Push a close to widen.
    _push_closes(fe, "5m", [99.0, 101.0, 99.0, 101.0, 100.0])
    bands = fe.bollinger_bands("5m")
    assert bands is not None
    lower, _mid, upper = bands
    assert upper > lower

    bar_close_pct_b = fe.bollinger_pct_b("5m")
    above_band_pct_b = fe.bollinger_pct_b("5m", live_price=upper + 1.0)
    below_band_pct_b = fe.bollinger_pct_b("5m", live_price=lower - 1.0)

    assert bar_close_pct_b is not None
    assert above_band_pct_b is not None and above_band_pct_b > 1.0
    assert below_band_pct_b is not None and below_band_pct_b < 0.0
    # Live price moved away from the bar close, so the score must change.
    assert above_band_pct_b != bar_close_pct_b
    assert below_band_pct_b != bar_close_pct_b


def test_bollinger_pct_b_falls_back_to_last_close_when_no_live_price() -> None:
    """The live-price overload is opt-in. Backtest replay and unit tests
    that don't have a live source must still get the bar-close-anchored
    value — silently switching to a different default would break replay
    determinism."""
    fe = _engine(bb_period=5, std=2.0)
    _push_closes(fe, "5m", [100.0, 101.0, 99.0, 102.0, 98.0])
    no_arg = fe.bollinger_pct_b("5m")
    explicit_none = fe.bollinger_pct_b("5m", live_price=None)
    assert no_arg is not None
    assert no_arg == explicit_none


# ------------------------------- ATR ---------------------------------------


def test_atr_from_bar_highs_and_lows() -> None:
    fe = _engine(atr_period=3)
    # First bar seeds prev_close — no TR sample.
    fe.ingest_bar("5m", close=100.0, high=100.0, low=100.0)
    # TR each bar = 20 (high-low spans 20 around close 100).
    for _ in range(3):
        fe.ingest_bar("5m", close=100.0, high=110.0, low=90.0)
    assert fe.atr("5m") == pytest.approx(20.0)


# -------------------------- regime classifiers -----------------------------


def test_regime_vol_classifies_by_atr() -> None:
    fe = _engine(atr_period=3, atr_hi=50.0, atr_lo=5.0)
    fe.ingest_bar("5m", close=100.0, high=100.0, low=100.0)
    for _ in range(4):
        fe.ingest_bar("5m", close=100.0, high=200.0, low=100.0)
    assert fe.regime_vol("5m") == "high"

    fe = _engine(atr_period=3, atr_hi=50.0, atr_lo=5.0)
    fe.ingest_bar("5m", close=100.0, high=100.0, low=100.0)
    for _ in range(4):
        fe.ingest_bar("5m", close=100.0, high=100.5, low=99.5)
    assert fe.regime_vol("5m") == "low"


def test_regime_trend_up_down_flat() -> None:
    fe = _engine(bb_period=10)
    _push_closes(fe, "5m", [100.0] * 5 + [200.0] * 5)
    assert fe.regime_trend("5m", flat_threshold_usd=25.0) == "up"

    fe = _engine(bb_period=10)
    _push_closes(fe, "5m", [200.0] * 5 + [100.0] * 5)
    assert fe.regime_trend("5m", flat_threshold_usd=25.0) == "down"

    fe = _engine(bb_period=10)
    _push_closes(fe, "5m", [100.0, 101.0, 100.0, 102.0, 99.0,
                            100.0, 101.0, 100.0, 99.0, 100.0])
    assert fe.regime_trend("5m", flat_threshold_usd=25.0) == "flat"


# ----------------------------- RSI routing --------------------------------


def test_rsi_warmup_then_value_per_tf() -> None:
    fe = _engine(timeframes=["5m", "1h"], bb_period=5, rsi_period=14)
    # Push 14 closes to 5m only — 1h should stay None.
    _push_closes(fe, "5m", [78_000.0 + i for i in range(14)])
    assert fe.rsi("5m") is None  # only 13 deltas from 14 closes
    fe.ingest_bar("5m", close=78_014.0, high=78_014.0, low=78_014.0)
    assert fe.rsi("5m") is not None
    assert fe.rsi("1h") is None  # no 1h closes yet


# --------------------------- 24h rolling move -----------------------------


def test_move_24h_warmup_none_then_signed_fraction() -> None:
    fe = _engine(timeframes=["1h"], bb_period=5)
    # Fewer than 25 1h closes → warmup.
    for i in range(MOVE_24H_WINDOW_BARS - 1):
        fe.ingest_bar("1h", close=78_000.0 + i, high=0.0, low=0.0)
    assert fe.move_24h_pct() is None

    # Complete the window: 25 closes from 78_000 → 82_000.
    fe = _engine(timeframes=["1h"], bb_period=5)
    step = 4_000.0 / (MOVE_24H_WINDOW_BARS - 1)
    for i in range(MOVE_24H_WINDOW_BARS):
        fe.ingest_bar("1h", close=78_000.0 + step * i, high=0.0, low=0.0)
    move = fe.move_24h_pct()
    assert move is not None
    assert move == pytest.approx(4_000.0 / 78_000.0, rel=1e-6)


def test_move_24h_only_consumes_1h_closes() -> None:
    """5m closes must not leak into the 24h-move deque."""
    fe = _engine(timeframes=["5m", "1h"], bb_period=5)
    for i in range(50):
        fe.ingest_bar("5m", close=90_000.0 + i, high=0.0, low=0.0)
    assert fe.move_24h_pct() is None


# --------------------------- bus integration ------------------------------


def test_attach_routes_bus_closes_by_tf() -> None:
    bus = MultiTimeframeBus(tf_secs=[60, 300])
    fe = FeatureEngine(
        timeframes=["1m", "5m"], bollinger_period=5, bollinger_std_mult=2.0
    )
    fe.attach(bus)

    t0 = 1_713_312_000_000_000_000  # midnight-aligned
    # 2-second-spaced ticks → 1m fires once per 30 ticks, 5m once per 150 ticks.
    for i in range(400):
        bus.ingest(
            SpotTick(
                ts_ns=t0 + i * 2_000_000_000,
                venue="coinbase",
                price_micros=usd_to_micros(78_000.0 + i % 50),
                size=0.01,
            )
        )
    bus.flush()

    # 1m got enough closes to warm up the 5-period Bollinger.
    assert fe.bollinger_bands("1m") is not None
    assert fe.last_close("1m") is not None
    # 5m closes arrived but may or may not be past BB warmup depending on
    # alignment — assert only that we saw at least one close.
    assert fe.last_close("5m") is not None


def test_attach_rejects_tf_not_on_bus() -> None:
    bus = MultiTimeframeBus(tf_secs=[60])
    fe = FeatureEngine(
        timeframes=["5m"], bollinger_period=5, bollinger_std_mult=2.0
    )
    with pytest.raises(ValueError, match="not registered on bus"):
        fe.attach(bus)


# ------------------------- population-stddev parity -----------------------


def test_bollinger_stddev_matches_population_formula() -> None:
    fe = _engine(bb_period=5, std=2.0)
    prices = [1.0, 2.0, 3.0, 4.0, 5.0]
    _push_closes(fe, "5m", prices)
    mean = 3.0
    pop_var = sum((p - mean) ** 2 for p in prices) / 5.0
    pop_std = math.sqrt(pop_var)
    bands = fe.bollinger_bands("5m")
    assert bands is not None
    lower, mid, upper = bands
    assert mid == pytest.approx(mean)
    assert upper == pytest.approx(mean + 2.0 * pop_std)
    assert lower == pytest.approx(mean - 2.0 * pop_std)


# --------------------------- rolling CVD (Slice 9) ------------------------


def test_cvd_warmup_returns_none_until_periods_bars_closed() -> None:
    fe = _engine(timeframes=["1m"])
    assert fe.cvd("1m", periods=5) is None  # no bars closed
    for _ in range(4):
        fe.ingest_bar_flows("1m", buy_volume_usd=1000.0, sell_volume_usd=0.0)
    assert fe.cvd("1m", periods=5) is None  # 4 < 5
    fe.ingest_bar_flows("1m", buy_volume_usd=1000.0, sell_volume_usd=0.0)
    assert fe.cvd("1m", periods=5) == pytest.approx(5_000.0)


def test_cvd_rolling_sum_signed() -> None:
    fe = _engine(timeframes=["1m"])
    # 5 bars: +2000, -1500, +500, -800, +300 → net +500
    bars = [
        (2_000.0, 0.0),
        (0.0, 1_500.0),
        (500.0, 0.0),
        (0.0, 800.0),
        (300.0, 0.0),
    ]
    for buy, sell in bars:
        fe.ingest_bar_flows("1m", buy_volume_usd=buy, sell_volume_usd=sell)
    assert fe.cvd("1m", periods=5) == pytest.approx(500.0)


def test_cvd_drops_oldest_bar_on_rolling_advance() -> None:
    """A new bar should evict the oldest — window is strictly rolling."""
    fe = _engine(timeframes=["1m"])
    # First 5 bars all +1000 buy → rolling CVD = +5000.
    for _ in range(5):
        fe.ingest_bar_flows("1m", buy_volume_usd=1_000.0, sell_volume_usd=0.0)
    assert fe.cvd("1m", periods=5) == pytest.approx(5_000.0)
    # Sixth bar is a hard sell → oldest buy evicts, net = +4000 - 3000 = +1000.
    fe.ingest_bar_flows("1m", buy_volume_usd=0.0, sell_volume_usd=3_000.0)
    assert fe.cvd("1m", periods=5) == pytest.approx(1_000.0)


def test_cvd_shorter_periods_slice_is_correct() -> None:
    fe = _engine(timeframes=["1m"])
    # Last 3 bars: +500, +500, -2000 → rolling-3 = -1000; rolling-5 = -1000
    # as well because first 2 bars are 0,0.
    for buy, sell in [
        (0.0, 0.0),
        (0.0, 0.0),
        (500.0, 0.0),
        (500.0, 0.0),
        (0.0, 2_000.0),
    ]:
        fe.ingest_bar_flows("1m", buy_volume_usd=buy, sell_volume_usd=sell)
    assert fe.cvd("1m", periods=3) == pytest.approx(-1_000.0)
    assert fe.cvd("1m", periods=5) == pytest.approx(-1_000.0)


def test_cvd_rejects_invalid_periods() -> None:
    fe = _engine(timeframes=["1m"])
    with pytest.raises(ValueError, match="periods must be > 0"):
        fe.cvd("1m", periods=0)
    with pytest.raises(ValueError, match="periods must be > 0"):
        fe.cvd("1m", periods=-1)
    with pytest.raises(ValueError, match="exceeds CVD deque capacity"):
        # deque capacity is CVD_WINDOW_BARS = 10
        fe.cvd("1m", periods=11)


def test_cvd_returns_none_for_unconfigured_tf() -> None:
    fe = _engine(timeframes=["1m"])
    # 15m isn't configured on this engine — reader is non-throwing for
    # "unconfigured TF" the same way rsi/bollinger are (returns None).
    assert fe.cvd("15m", periods=3) is None


def test_cvd_populates_from_bus_path() -> None:
    """End-to-end: a `Bar` with `buy_volume_usd`/`sell_volume_usd` fired via
    the bus subscription should be consumed by the CVD deque (no need to
    call `ingest_bar_flows` explicitly)."""
    bus = MultiTimeframeBus(tf_secs=[60])
    fe = FeatureEngine(
        timeframes=["1m"], bollinger_period=5, bollinger_std_mult=2.0
    )
    fe.attach(bus)
    t0 = 1_713_312_000_000_000_000  # UTC-aligned, from test_bars.T0

    # 6 bars, 60s each, each with a single aggressive buy print for $1k USD.
    for i in range(7):
        bus.ingest(SpotTick(
            ts_ns=t0 + i * 60_000_000_000,
            venue="coinbase",
            price_micros=usd_to_micros(100_000.0),
            size=0.01,  # 100k * 0.01 = $1k
            aggressor="buy",
        ))
    # After the 7th tick, 6 bars closed; last 5 all +$1k buy → CVD = +$5k.
    assert fe.cvd("1m", periods=5) == pytest.approx(5_000.0)
