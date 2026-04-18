from __future__ import annotations

from typing import Literal

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features, RegimeVol
from bot_btc_1hr_kalshi.signal import MarketSnapshot, detect_floor_reversion


def _book(ask_price: int, *, valid: bool = True) -> L2Book:
    b = L2Book("BTC-1H")
    if valid:
        b.apply(
            BookUpdate(
                seq=1,
                ts_ns=1,
                market_id="BTC-1H",
                bids=(BookLevel(ask_price - 2, 100),),
                asks=(BookLevel(ask_price, 100),),
                is_snapshot=True,
            )
        )
    return b


def _features(
    *,
    pct_b: float = -0.5,
    regime_vol: RegimeVol = "normal",
    regime_trend: Literal["up", "down", "flat"] = "flat",
    rsi_5m: float | None = None,
    rsi_1h: float | None = None,
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
        cvd_1m_usd=cvd_1m_usd,
    )


def _snap(
    ask_price: int = 35,
    *,
    spot: float = 60_100.0,
    strike: float = 60_000.0,
    **kwargs: object,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(ask_price=ask_price, valid=bool(kwargs.pop("valid", True))),
        features=_features(**kwargs),  # type: ignore[arg-type]
        spot_btc_usd=spot,
        minutes_to_settlement=30.0,
        strike_usd=strike,
    )


def test_fires_on_clear_floor_setup() -> None:
    sig = detect_floor_reversion(_snap(ask_price=25, pct_b=-0.8), min_confidence=0.3)
    assert sig is not None
    assert sig.trap == "floor_reversion"
    assert sig.side == "YES"
    # entry is at best_bid (ask_price - 2 per _book helper), not at the ask —
    # maker-only entry means we post at the bid.
    assert sig.entry_price_cents == 23
    assert 0.0 < sig.confidence <= 1.0
    assert sig.edge_cents > 0


def test_rejects_when_book_invalid() -> None:
    b = L2Book("BTC-1H")  # never applied snapshot
    snap = MarketSnapshot(
        market_id="BTC-1H",
        book=b,
        features=_features(pct_b=-0.8),
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
        strike_usd=60_000.0,
    )
    assert detect_floor_reversion(snap, min_confidence=0.3) is None


def test_rejects_when_ask_above_floor_threshold() -> None:
    assert detect_floor_reversion(_snap(ask_price=50, pct_b=-0.9), min_confidence=0.3) is None


def test_rejects_when_pct_b_nonneg() -> None:
    assert detect_floor_reversion(_snap(ask_price=20, pct_b=0.1), min_confidence=0.3) is None


def test_rejects_in_high_vol_regime() -> None:
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, regime_vol="high"),
        min_confidence=0.3,
    )
    assert sig is None


def test_rejects_below_min_confidence() -> None:
    # pct_b=-0.2 -> confidence=0.2 < 0.3 threshold
    assert detect_floor_reversion(_snap(ask_price=20, pct_b=-0.2), min_confidence=0.3) is None


def test_edge_scales_with_confidence_and_discount() -> None:
    cheap = detect_floor_reversion(_snap(ask_price=10, pct_b=-0.9), min_confidence=0.3)
    less_cheap = detect_floor_reversion(_snap(ask_price=35, pct_b=-0.9), min_confidence=0.3)
    assert cheap is not None and less_cheap is not None
    assert cheap.edge_cents > less_cheap.edge_cents


# ---- HTF alignment (Slice 8) -------------------------------------------------


def test_htf_veto_rejects_long_when_1h_rsi_bearish() -> None:
    # 1H RSI 40 < 45 (default bearish veto) — trap must not fire even with
    # a strong pct_b and confirming 5m oversold reading.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, rsi_1h=40.0, rsi_5m=25.0),
        min_confidence=0.3,
    )
    assert sig is None


def test_htf_veto_passes_when_1h_rsi_neutral_or_bullish() -> None:
    # RSI 50 >= 45 threshold — macro not bearish → trap fires.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, rsi_1h=50.0),
        min_confidence=0.3,
    )
    assert sig is not None


def test_htf_veto_fails_open_during_warmup() -> None:
    # rsi_1h=None (warmup) must not block the trap — matches pre-Slice-8 behavior.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, rsi_1h=None),
        min_confidence=0.3,
    )
    assert sig is not None


def test_rsi_5m_weight_deep_oversold_keeps_full_confidence() -> None:
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.8, rsi_5m=25.0),
        min_confidence=0.3,
    )
    assert sig is not None
    # 5m RSI <=35 → weight 1.0 → confidence == |pct_b|.
    assert sig.confidence == 0.8


def test_rsi_5m_weight_neutral_halves_confidence() -> None:
    # pct_b -0.8 weighted by rsi_5m=50 gives 0.8 * 0.5 = 0.4.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.8, rsi_5m=50.0),
        min_confidence=0.3,
    )
    assert sig is not None
    assert sig.confidence == 0.4


def test_rsi_5m_weight_can_drop_below_min_confidence() -> None:
    # pct_b=-0.7, rsi_5m=50 → weighted confidence 0.35. min_confidence=0.4 blocks.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.7, rsi_5m=50.0),
        min_confidence=0.4,
    )
    assert sig is None


def test_htf_veto_rsi_threshold_is_configurable() -> None:
    # Override to 40 → RSI 42 (previously vetoed at default 45) should pass.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, rsi_1h=42.0),
        min_confidence=0.3,
        htf_bearish_veto_rsi=40.0,
    )
    assert sig is not None


# ---- Tape Reader / CVD veto (Slice 9) ---------------------------------------


def test_cvd_veto_blocks_on_heavy_aggressor_selling() -> None:
    # Rolling-5m net aggressor flow -$10M <= -$5M default threshold — the
    # "dip" is a cascade driven by taker selling, not a reversion candidate.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=-10_000_000.0),
        min_confidence=0.3,
    )
    assert sig is None


def test_cvd_veto_passes_on_balanced_or_positive_flow() -> None:
    # Net aggressor buying during a dip is exactly the reversion setup.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=2_000_000.0),
        min_confidence=0.3,
    )
    assert sig is not None


def test_cvd_veto_fails_open_during_warmup() -> None:
    # cvd None (fewer than CVD_ROLLING_PERIODS 1m bars accumulated) must
    # not block the trap — pre-Slice-9 behavior preserved on cold start.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=None),
        min_confidence=0.3,
    )
    assert sig is not None


def test_cvd_veto_threshold_is_configurable() -> None:
    # Tighten to $1M — a -$2M flow that would pass at default now blocks.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=-2_000_000.0),
        min_confidence=0.3,
        cvd_1m_veto_threshold_usd=1_000_000.0,
    )
    assert sig is None


def test_cvd_veto_boundary_at_exact_threshold_is_blocking() -> None:
    # cvd <= -threshold is the comparison — the exact threshold blocks.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=-5_000_000.0),
        min_confidence=0.3,
        cvd_1m_veto_threshold_usd=5_000_000.0,
    )
    assert sig is None


def test_cvd_veto_does_not_block_on_positive_flow_of_equal_magnitude() -> None:
    # Symmetric safeguard: a +$10M buy-side flow must NOT block the floor
    # trap — only aggressor selling into the dip indicates a cascade.
    sig = detect_floor_reversion(
        _snap(ask_price=20, pct_b=-0.9, cvd_1m_usd=10_000_000.0),
        min_confidence=0.3,
    )
    assert sig is not None
