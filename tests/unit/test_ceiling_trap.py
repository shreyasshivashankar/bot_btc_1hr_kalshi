from __future__ import annotations

from typing import Literal

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.market_data.types import (
    LiquidationHeatmapSample,
    OpenInterestSample,
)
from bot_btc_1hr_kalshi.obs.schemas import Features, RegimeVol
from bot_btc_1hr_kalshi.signal import MarketSnapshot, detect_ceiling_reversion


def _book(*, yes_ask: int, yes_bid: int, valid: bool = True) -> L2Book:
    """Build a YES-space book so that NO best ask == 100 - yes_bid.

    For the ceiling trap we want NO to be "cheap" (NO_ask <= 40), which means
    YES is rich, i.e. yes_bid >= 60.
    """
    b = L2Book("BTC-1H")
    if valid:
        b.apply(
            BookUpdate(
                seq=1,
                ts_ns=1,
                market_id="BTC-1H",
                bids=(BookLevel(yes_bid, 100),),
                asks=(BookLevel(yes_ask, 100),),
                is_snapshot=True,
            )
        )
    return b


def _features(
    *,
    pct_b: float = 0.5,
    regime_vol: RegimeVol = "normal",
    regime_trend: Literal["up", "down", "flat"] = "flat",
    rsi_5m: float | None = None,
    rsi_1h: float | None = None,
    move_24h_pct: float | None = None,
    cvd_1m_usd: float | None = None,
) -> Features:
    return Features(
        regime_trend=regime_trend,
        regime_vol=regime_vol,
        signal_confidence=0.5,
        bollinger_pct_b=pct_b,
        atr_cents=10.0,
        book_depth_at_entry=200.0,
        spread_cents=2,
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
        rsi_5m=rsi_5m,
        rsi_1h=rsi_1h,
        move_24h_pct=move_24h_pct,
        cvd_1m_usd=cvd_1m_usd,
    )


def _snap(
    *,
    yes_ask: int = 80,
    yes_bid: int = 78,
    pct_b: float = 0.8,
    regime_vol: RegimeVol = "normal",
    valid: bool = True,
    spot: float = 60_000.0,
    strike: float = 60_000.0,
    rsi_5m: float | None = None,
    rsi_1h: float | None = None,
    move_24h_pct: float | None = None,
    cvd_1m_usd: float | None = None,
    open_interest: OpenInterestSample | None = None,
    liquidation_heatmap: LiquidationHeatmapSample | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(yes_ask=yes_ask, yes_bid=yes_bid, valid=valid),
        features=_features(
            pct_b=pct_b,
            regime_vol=regime_vol,
            rsi_5m=rsi_5m,
            rsi_1h=rsi_1h,
            move_24h_pct=move_24h_pct,
            cvd_1m_usd=cvd_1m_usd,
        ),
        spot_btc_usd=spot,
        minutes_to_settlement=30.0,
        strike_usd=strike,
        open_interest=open_interest,
        liquidation_heatmap=liquidation_heatmap,
    )


def _heatmap(peak_price: float, total: float = 1_000_000.0) -> LiquidationHeatmapSample:
    return LiquidationHeatmapSample(
        ts_ns=1,
        symbol="BTCUSDT",
        total_liquidation_usd=total,
        peak_cluster_price_usd=peak_price,
        peak_cluster_liquidation_usd=total,
    )


def _oi(total_usd: float) -> OpenInterestSample:
    return OpenInterestSample(
        ts_ns=1,
        symbol="BTCUSDT",
        total_oi_usd=total_usd,
    )


def test_fires_on_clear_ceiling_setup() -> None:
    # NO ask = 100 - yes_bid = 100 - 78 = 22 ≤ 40 → cheap NO.
    # NO bid = 100 - yes_ask = 100 - 80 = 20.
    sig = detect_ceiling_reversion(_snap(yes_ask=80, yes_bid=78, pct_b=0.8), min_confidence=0.3)
    assert sig is not None
    assert sig.trap == "ceiling_reversion"
    assert sig.side == "NO"
    assert sig.entry_price_cents == 20  # NO best bid = 100 - YES best ask
    assert 0.0 < sig.confidence <= 1.0
    assert sig.edge_cents > 0


def test_rejects_when_book_invalid() -> None:
    b = L2Book("BTC-1H")  # never applied snapshot
    snap = MarketSnapshot(
        market_id="BTC-1H",
        book=b,
        features=_features(pct_b=0.8),
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
        strike_usd=60_000.0,
    )
    assert detect_ceiling_reversion(snap, min_confidence=0.3) is None


def test_rejects_when_no_ask_above_threshold() -> None:
    # yes_bid=50 → NO_ask = 50 > 40 → NO not cheap.
    assert (
        detect_ceiling_reversion(_snap(yes_ask=55, yes_bid=50, pct_b=0.9), min_confidence=0.3)
        is None
    )


def test_rejects_when_pct_b_nonpos() -> None:
    assert (
        detect_ceiling_reversion(_snap(yes_ask=80, yes_bid=78, pct_b=-0.1), min_confidence=0.3)
        is None
    )


def test_rejects_in_high_vol_regime() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, regime_vol="high"),
        min_confidence=0.3,
    )
    assert sig is None


def test_rejects_below_min_confidence() -> None:
    assert (
        detect_ceiling_reversion(_snap(yes_ask=80, yes_bid=78, pct_b=0.2), min_confidence=0.3)
        is None
    )


def test_edge_scales_with_confidence_and_discount() -> None:
    # cheaper entry (lower NO bid) → larger edge_cents.
    # yes_ask=90 → NO_bid=10; yes_ask=65 → NO_bid=35.
    cheap = detect_ceiling_reversion(
        _snap(yes_ask=90, yes_bid=88, pct_b=0.9), min_confidence=0.3
    )
    less_cheap = detect_ceiling_reversion(
        _snap(yes_ask=65, yes_bid=63, pct_b=0.9), min_confidence=0.3
    )
    assert cheap is not None and less_cheap is not None
    assert cheap.edge_cents > less_cheap.edge_cents


# ---- HTF alignment (Slice 8) -------------------------------------------------


def test_htf_veto_rejects_short_when_1h_rsi_bullish() -> None:
    # 1H RSI 60 > 55 → macro declared bullish; SHORT trap must not fire.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, rsi_1h=60.0, rsi_5m=75.0),
        min_confidence=0.3,
    )
    assert sig is None


def test_htf_veto_passes_when_1h_rsi_neutral_or_bearish() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, rsi_1h=50.0),
        min_confidence=0.3,
    )
    assert sig is not None


def test_htf_veto_fails_open_during_warmup() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, rsi_1h=None),
        min_confidence=0.3,
    )
    assert sig is not None


def test_rsi_5m_weight_deep_overbought_keeps_full_confidence() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.8, rsi_5m=75.0),
        min_confidence=0.3,
    )
    assert sig is not None
    # rsi_5m >= 65 → weight 1.0 → confidence == |pct_b|.
    assert sig.confidence == 0.8


def test_rsi_5m_weight_neutral_halves_confidence() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.8, rsi_5m=50.0),
        min_confidence=0.3,
    )
    assert sig is not None
    assert sig.confidence == 0.4


def test_htf_veto_rsi_threshold_is_configurable() -> None:
    # Override to 65 → RSI 60 (previously vetoed at default 55) should pass.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, rsi_1h=60.0),
        min_confidence=0.3,
        htf_bullish_veto_rsi=65.0,
    )
    assert sig is not None


# ---- Runaway Train (Slice 8 Phase 5) -----------------------------------------


def test_runaway_train_blocks_on_parabolic_rally() -> None:
    # +6% in 24h > 5% default threshold — shorting a parabolic run has no edge.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=0.06),
        min_confidence=0.3,
    )
    assert sig is None


def test_runaway_train_blocks_on_capitulation() -> None:
    # Symmetric: a -6% crash also blocks the ceiling trap (magnitude-based).
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=-0.06),
        min_confidence=0.3,
    )
    assert sig is None


def test_runaway_train_passes_below_threshold() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=0.03),
        min_confidence=0.3,
    )
    assert sig is not None


def test_runaway_train_fails_open_during_warmup() -> None:
    # move_24h_pct=None (25 1h closes not yet accumulated) must not block.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=None),
        min_confidence=0.3,
    )
    assert sig is not None


def test_runaway_train_threshold_is_configurable() -> None:
    # Default 5% would veto a 6% move. Loosen to 10% and the trap fires.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=0.06),
        min_confidence=0.3,
        runaway_train_halt_pct=0.10,
    )
    assert sig is not None


def test_runaway_train_boundary_at_exact_threshold_is_blocking() -> None:
    # >= is a blocking comparison — the threshold value itself blocks.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, move_24h_pct=0.05),
        min_confidence=0.3,
        runaway_train_halt_pct=0.05,
    )
    assert sig is None


# ---- Tape Reader / CVD veto (Slice 9) ---------------------------------------


def test_cvd_veto_blocks_on_heavy_aggressor_buying() -> None:
    # Rolling-5m net aggressor flow +$10M >= +$5M default threshold — the
    # "pump" is a breakout driven by taker buying, not a reversion candidate.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=10_000_000.0),
        min_confidence=0.3,
    )
    assert sig is None


def test_cvd_veto_passes_on_balanced_or_negative_flow() -> None:
    # Net aggressor selling during a pump is exactly the reversion setup.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=-2_000_000.0),
        min_confidence=0.3,
    )
    assert sig is not None


def test_cvd_veto_fails_open_during_warmup() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=None),
        min_confidence=0.3,
    )
    assert sig is not None


def test_cvd_veto_threshold_is_configurable() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=2_000_000.0),
        min_confidence=0.3,
        cvd_1m_veto_threshold_usd=1_000_000.0,
    )
    assert sig is None


def test_cvd_veto_boundary_at_exact_threshold_is_blocking() -> None:
    # cvd >= +threshold is the comparison — the exact threshold blocks.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=5_000_000.0),
        min_confidence=0.3,
        cvd_1m_veto_threshold_usd=5_000_000.0,
    )
    assert sig is None


def test_cvd_veto_does_not_block_on_negative_flow_of_equal_magnitude() -> None:
    # Symmetric safeguard: a -$10M sell-side flow must NOT block the ceiling
    # trap — only aggressor buying into the pump indicates a breakout.
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, cvd_1m_usd=-10_000_000.0),
        min_confidence=0.3,
    )
    assert sig is not None


# ---- Microstructure shadow gate (Slice 11 P3) --------------------------------


def test_microstructure_off_still_emits_signal_and_tags_reason() -> None:
    # Spot 60_000, cluster at 60_180 → gap_frac = 180/60_000 = 0.003,
    # which is <= default 0.005 → adverse cluster ABOVE spot for the
    # short. Gating OFF: signal still emits with shadow tag.
    sig = detect_ceiling_reversion(
        _snap(
            yes_ask=80,
            yes_bid=78,
            pct_b=0.9,
            liquidation_heatmap=_heatmap(peak_price=60_180.0),
        ),
        min_confidence=0.3,
    )
    assert sig is not None
    assert sig.features.shadow_veto_reason == "heatmap_adverse_cluster_above"


def test_microstructure_on_rejects_when_cluster_above_spot() -> None:
    sig = detect_ceiling_reversion(
        _snap(
            yes_ask=80,
            yes_bid=78,
            pct_b=0.9,
            liquidation_heatmap=_heatmap(peak_price=60_180.0),
        ),
        min_confidence=0.3,
        enable_microstructure_gating=True,
    )
    assert sig is None


def test_microstructure_ignores_cluster_below_spot_for_short() -> None:
    # Cluster below spot is in our favor for a short — no veto, no tag.
    sig = detect_ceiling_reversion(
        _snap(
            yes_ask=80,
            yes_bid=78,
            pct_b=0.9,
            liquidation_heatmap=_heatmap(peak_price=59_700.0),
        ),
        min_confidence=0.3,
    )
    assert sig is not None
    assert sig.features.shadow_veto_reason is None


def test_microstructure_ignores_distant_cluster() -> None:
    # Cluster 62_000 is > 0.5% above spot 60_000 — outside adverse band.
    sig = detect_ceiling_reversion(
        _snap(
            yes_ask=80,
            yes_bid=78,
            pct_b=0.9,
            liquidation_heatmap=_heatmap(peak_price=62_000.0),
        ),
        min_confidence=0.3,
    )
    assert sig is not None
    assert sig.features.shadow_veto_reason is None


def test_microstructure_oi_compression_tags_off_rejects_on() -> None:
    snap = _snap(yes_ask=80, yes_bid=78, pct_b=0.9, open_interest=_oi(total_usd=1_000_000.0))
    shadow = detect_ceiling_reversion(
        snap,
        min_confidence=0.3,
        oi_compression_threshold_usd=5_000_000.0,
    )
    assert shadow is not None
    assert shadow.features.shadow_veto_reason == "oi_compression"

    gated = detect_ceiling_reversion(
        snap,
        min_confidence=0.3,
        enable_microstructure_gating=True,
        oi_compression_threshold_usd=5_000_000.0,
    )
    assert gated is None


def test_microstructure_oi_threshold_zero_is_disabled() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9, open_interest=_oi(total_usd=0.0)),
        min_confidence=0.3,
        enable_microstructure_gating=True,
    )
    assert sig is not None
    assert sig.features.shadow_veto_reason is None


def test_microstructure_absent_feeds_fail_open() -> None:
    sig = detect_ceiling_reversion(
        _snap(yes_ask=80, yes_bid=78, pct_b=0.9),
        min_confidence=0.3,
        enable_microstructure_gating=True,
    )
    assert sig is not None
    assert sig.features.shadow_veto_reason is None
