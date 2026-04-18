"""Unit tests for TimeframeAggregator + MultiTimeframeBus (Slice 7).

Covers:
  * OHLCV correctness within a single bar (open/high/low/close/volume/count)
  * Boundary alignment to UTC (1m at :MM:00, 1h at top, 1d at midnight)
  * Close-on-next-bar semantics (close fires when next-bar tick arrives)
  * Gap handling (empty intermediate bars are skipped, not synthesized)
  * Backward-tick drop (out-of-order print cannot corrupt OHLC)
  * Unsubscribe stops delivery
  * flush() force-closes the open bar
  * MultiTimeframeBus fans one tick to all aggregators
  * Nested alignment (5m close co-fires with a 1m close at :00, :05, …)
"""
from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.market_data.bars import (
    Bar,
    MultiTimeframeBus,
    TimeframeAggregator,
)
from bot_btc_1hr_kalshi.market_data.types import AggressorSide, SpotTick
from bot_btc_1hr_kalshi.obs.money import usd_to_micros

# Midnight UTC on 2026-04-17 (chosen so 1m / 1h / 1d boundaries all fall on it).
T0 = 1_713_312_000_000_000_000


def _tick(
    offset_ns: int,
    price: float,
    size: float = 0.01,
    aggressor: AggressorSide | None = None,
) -> SpotTick:
    return SpotTick(
        ts_ns=T0 + offset_ns,
        venue="coinbase",
        price_micros=usd_to_micros(price),
        size=size,
        aggressor=aggressor,
    )


def test_single_bar_ohlcv_aggregation() -> None:
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    # Four ticks inside the [T0, T0+60s) bar — confirm O/H/L/C/V/count.
    agg.ingest(_tick(1_000_000_000, 78_000.0, size=0.10))  # open
    agg.ingest(_tick(10_000_000_000, 78_150.0, size=0.20))  # new high
    agg.ingest(_tick(30_000_000_000, 77_900.0, size=0.05))  # new low
    agg.ingest(_tick(59_000_000_000, 78_050.0, size=0.15))  # close

    # Bar hasn't closed yet — the next-bar tick triggers close.
    agg.ingest(_tick(60_000_000_000, 78_075.0, size=0.01))
    assert len(closed) == 1
    bar = closed[0]
    assert bar.tf_sec == 60
    assert bar.ts_open_ns == T0
    assert bar.ts_close_ns == T0 + 60_000_000_000
    assert bar.open_usd == pytest.approx(78_000.0)
    assert bar.high_usd == pytest.approx(78_150.0)
    assert bar.low_usd == pytest.approx(77_900.0)
    assert bar.close_usd == pytest.approx(78_050.0)
    assert bar.volume == pytest.approx(0.50)
    assert bar.trade_count == 4


def test_boundary_alignment_hourly() -> None:
    """Hourly bar opens at HH:00:00 even when ticks straddle the minute."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=3600)
    agg.subscribe(closed.append)

    # Tick at 01:23 UTC → bar should open at 01:00 UTC.
    offset_ns = 3600_000_000_000 + 23 * 60_000_000_000  # 01:23:00
    agg.ingest(SpotTick(ts_ns=T0 + offset_ns, venue="coinbase",
                        price_micros=usd_to_micros(78_000.0), size=0.01))
    # Next hour arrives → close fires.
    agg.ingest(SpotTick(ts_ns=T0 + 2 * 3600_000_000_000, venue="coinbase",
                        price_micros=usd_to_micros(78_500.0), size=0.01))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.ts_open_ns == T0 + 3600_000_000_000  # 01:00 UTC, not 01:23
    assert bar.ts_close_ns == T0 + 2 * 3600_000_000_000  # 02:00 UTC


def test_gap_skips_empty_bars() -> None:
    """Sparse ticks produce sparse bars — no synthetic zero-volume bars."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    agg.ingest(_tick(5_000_000_000, 78_000.0))  # in [0, 60s)
    # Jump forward by 5 minutes — 4 intermediate minutes have no ticks.
    agg.ingest(_tick(305_000_000_000, 78_500.0))  # in [300s, 360s)

    # Exactly one close fires — the [0, 60s) bar. Empty bars are NOT synthesized.
    assert len(closed) == 1
    assert closed[0].ts_open_ns == T0
    assert closed[0].close_usd == pytest.approx(78_000.0)


def test_backward_tick_is_dropped() -> None:
    """A tick whose timestamp precedes the current bar's open is dropped."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    agg.ingest(_tick(30_000_000_000, 78_000.0))  # opens [0, 60s) bar
    # Now inject an out-of-order tick that belongs to a prior bar.
    agg.ingest(SpotTick(
        ts_ns=T0 - 120_000_000_000,  # 2 min before T0 — way before current bar
        venue="coinbase",
        price_micros=usd_to_micros(70_000.0),  # would smash the low
        size=1.0,
    ))

    # Close the bar and confirm the backward tick didn't leak into OHLC.
    agg.ingest(_tick(60_000_000_000, 78_100.0))
    assert len(closed) == 1
    bar = closed[0]
    assert bar.low_usd == pytest.approx(78_000.0)  # NOT 70_000
    assert bar.high_usd == pytest.approx(78_000.0)
    assert bar.trade_count == 1  # the backward tick was dropped


def test_unsubscribe_stops_delivery() -> None:
    seen: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    unsub = agg.subscribe(seen.append)

    agg.ingest(_tick(1_000_000_000, 78_000.0))  # opens bar 0
    unsub()
    agg.ingest(_tick(61_000_000_000, 78_050.0))  # would close bar 0

    assert seen == []


def test_flush_emits_open_bar() -> None:
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    agg.ingest(_tick(10_000_000_000, 78_000.0))
    agg.ingest(_tick(20_000_000_000, 78_050.0))
    assert closed == []  # bar still open

    agg.flush()
    assert len(closed) == 1
    assert closed[0].close_usd == pytest.approx(78_050.0)

    # Second flush is a no-op (no open bar).
    agg.flush()
    assert len(closed) == 1


def test_invalid_tf_sec_raises() -> None:
    with pytest.raises(ValueError, match="tf_sec must be > 0"):
        TimeframeAggregator(tf_sec=0)
    with pytest.raises(ValueError, match="tf_sec must be > 0"):
        TimeframeAggregator(tf_sec=-60)


def test_multi_tf_bus_fans_to_all_aggregators() -> None:
    closes: dict[int, list[Bar]] = {60: [], 300: [], 3600: []}
    bus = MultiTimeframeBus(tf_secs=[60, 300, 3600])
    for tf in (60, 300, 3600):
        bus.subscribe(tf_sec=tf, cb=closes[tf].append)

    # First tick at T0 → opens all three bars.
    bus.ingest(_tick(0, 78_000.0))
    # After 61s → 1m bar rolls; 5m/1h still open.
    bus.ingest(_tick(61_000_000_000, 78_100.0))
    assert len(closes[60]) == 1
    assert len(closes[300]) == 0
    assert len(closes[3600]) == 0

    # After 301s from T0 → 1m rolls again AND 5m closes.
    bus.ingest(_tick(301_000_000_000, 78_200.0))
    assert len(closes[60]) == 2
    assert len(closes[300]) == 1
    assert len(closes[3600]) == 0

    # After 3601s → 1h closes (and 1m closes again).
    bus.ingest(_tick(3601_000_000_000, 78_300.0))
    assert len(closes[3600]) == 1


def test_multi_tf_bus_rejects_duplicates_and_empty() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        MultiTimeframeBus(tf_secs=[])
    with pytest.raises(ValueError, match="must be unique"):
        MultiTimeframeBus(tf_secs=[60, 60, 300])


def test_multi_tf_bus_subscribe_unknown_tf_raises() -> None:
    bus = MultiTimeframeBus(tf_secs=[60, 300])
    with pytest.raises(ValueError, match="tf_sec=900 not registered"):
        bus.subscribe(tf_sec=900, cb=lambda _b: None)


def test_multi_tf_bus_nested_boundaries_cofire() -> None:
    """At the top of an hour, 1m / 5m / 1h bars all close on the same tick."""
    closes: dict[int, list[Bar]] = {60: [], 300: [], 3600: []}
    bus = MultiTimeframeBus(tf_secs=[60, 300, 3600])
    for tf in (60, 300, 3600):
        bus.subscribe(tf_sec=tf, cb=closes[tf].append)

    # Warm all aggregators with a tick inside the final minute of the hour.
    bus.ingest(_tick(3599_000_000_000, 78_000.0))
    # Next tick lands at the next hour top → all three bars roll together.
    bus.ingest(_tick(3600_000_000_000, 78_050.0))
    assert len(closes[60]) == 1
    assert len(closes[300]) == 1
    assert len(closes[3600]) == 1
    # All three closes share the same ts_close_ns (top of the next hour).
    assert closes[60][0].ts_close_ns == T0 + 3600_000_000_000
    assert closes[300][0].ts_close_ns == T0 + 3600_000_000_000
    assert closes[3600][0].ts_close_ns == T0 + 3600_000_000_000


def test_bar_signed_usd_volume_routes_by_aggressor() -> None:
    """Slice 9: each tick's USD notional is routed to buy/sell lane via
    `tick.aggressor`. Untagged ticks update OHLC but contribute to neither
    lane — CVD counts only verified taker prints."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    # Tick 1: aggressor=buy @ $80k x 0.10 BTC -> 8,000 USD into buy lane.
    agg.ingest(_tick(1_000_000_000, 80_000.0, size=0.10, aggressor="buy"))
    # Tick 2: aggressor=sell @ $80k x 0.05 BTC -> 4,000 USD into sell lane.
    agg.ingest(_tick(10_000_000_000, 80_000.0, size=0.05, aggressor="sell"))
    # Tick 3: untagged (None) @ $80k x 0.01 -> NO signed accumulation, but
    # `volume` (unsigned BTC) still grows by 0.01.
    agg.ingest(_tick(20_000_000_000, 80_000.0, size=0.01, aggressor=None))
    # Close the bar.
    agg.ingest(_tick(60_000_000_000, 80_000.0, size=0.001, aggressor="buy"))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.buy_volume_usd == pytest.approx(8_000.0)
    assert bar.sell_volume_usd == pytest.approx(4_000.0)
    assert bar.cvd_usd == pytest.approx(4_000.0)
    # Unsigned BTC volume sums all three prints (0.10 + 0.05 + 0.01 = 0.16).
    assert bar.volume == pytest.approx(0.16)
    assert bar.trade_count == 3


def test_bar_signed_usd_volume_zero_when_bar_opened_by_untagged_tick() -> None:
    """A bar opened by an aggressor=None tick must start both signed lanes
    at 0, not at the tick's notional. Regression guard on `_start_bar`."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    # Opener is untagged → both lanes start at 0.
    agg.ingest(_tick(1_000_000_000, 80_000.0, size=0.10, aggressor=None))
    # Close the bar.
    agg.ingest(_tick(60_000_000_000, 80_000.0, size=0.001, aggressor=None))

    assert len(closed) == 1
    bar = closed[0]
    assert bar.buy_volume_usd == 0.0
    assert bar.sell_volume_usd == 0.0
    assert bar.cvd_usd == 0.0
    # Unsigned volume still captures the untagged prints.
    assert bar.volume == pytest.approx(0.10)


def test_bar_cvd_sign_at_price_extremes_is_price_weighted() -> None:
    """CVD is USD-denominated: a sell at $100k carries more weight than a
    buy at $40k for the same coin-size. Regime-robustness smoke test."""
    closed: list[Bar] = []
    agg = TimeframeAggregator(tf_sec=60)
    agg.subscribe(closed.append)

    # Buy 1 BTC @ 40k ($40k). Sell 0.5 BTC @ 100k ($50k). Net CVD should be
    # -$10k even though coin flow (+1 vs -0.5) is net positive.
    agg.ingest(_tick(1_000_000_000, 40_000.0, size=1.0, aggressor="buy"))
    agg.ingest(_tick(30_000_000_000, 100_000.0, size=0.5, aggressor="sell"))
    agg.ingest(_tick(60_000_000_000, 100_000.0, size=0.001))  # close

    assert len(closed) == 1
    assert closed[0].cvd_usd == pytest.approx(-10_000.0)


def test_multi_tf_bus_flush_propagates() -> None:
    closes: list[Bar] = []
    bus = MultiTimeframeBus(tf_secs=[60, 300])
    bus.subscribe(tf_sec=60, cb=closes.append)
    bus.subscribe(tf_sec=300, cb=closes.append)

    bus.ingest(_tick(10_000_000_000, 78_000.0))
    bus.flush()
    # Both timeframes flush their open bars — 2 total.
    assert len(closes) == 2
