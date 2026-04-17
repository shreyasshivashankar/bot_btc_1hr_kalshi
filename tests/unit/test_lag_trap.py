from __future__ import annotations

from typing import Literal

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features, RegimeVol
from bot_btc_1hr_kalshi.signal import MarketSnapshot, detect_cross_venue_lag


def _book(*, yes_ask: int, yes_bid: int, valid: bool = True) -> L2Book:
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
    pct_b: float,
    regime_vol: RegimeVol = "normal",
    regime_trend: Literal["up", "down", "flat"] = "flat",
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
    )


def _snap(
    *,
    yes_ask: int = 50,
    yes_bid: int = 48,
    pct_b: float = 2.0,
    regime_vol: RegimeVol = "normal",
    valid: bool = True,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(yes_ask=yes_ask, yes_bid=yes_bid, valid=valid),
        features=_features(pct_b=pct_b, regime_vol=regime_vol),
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def test_fires_long_yes_on_extreme_positive_pct_b() -> None:
    # YES ask=50 is in neutral [40,60]; spot ripped up (pct_b=+2) → buy YES.
    sig = detect_cross_venue_lag(_snap(yes_ask=50, yes_bid=48, pct_b=2.0), min_confidence=0.3)
    assert sig is not None
    assert sig.trap == "cross_venue_lag"
    assert sig.side == "YES"
    assert sig.entry_price_cents == 48  # YES best bid
    assert sig.confidence == 1.0  # min(1.0, 2.0/2.0)


def test_fires_long_no_on_extreme_negative_pct_b() -> None:
    # YES ask=50, yes_bid=48 → NO ask = 100-48 = 52 (in [40,60]); spot crashed → buy NO.
    # NO bid = 100 - yes_ask = 50.
    sig = detect_cross_venue_lag(_snap(yes_ask=50, yes_bid=48, pct_b=-2.0), min_confidence=0.3)
    assert sig is not None
    assert sig.side == "NO"
    assert sig.entry_price_cents == 50


def test_rejects_when_book_invalid() -> None:
    b = L2Book("BTC-1H")
    snap = MarketSnapshot(
        market_id="BTC-1H",
        book=b,
        features=_features(pct_b=2.0),
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )
    assert detect_cross_venue_lag(snap, min_confidence=0.3) is None


def test_rejects_when_pct_b_not_extreme_enough() -> None:
    # |pct_b| = 1.2 < LAG_PCT_B_MIN (1.5).
    assert detect_cross_venue_lag(_snap(pct_b=1.2), min_confidence=0.3) is None


def test_rejects_in_high_vol_regime() -> None:
    assert detect_cross_venue_lag(_snap(pct_b=2.0, regime_vol="high"), min_confidence=0.3) is None


def test_rejects_when_kalshi_already_priced_the_move() -> None:
    # Spot rallied (pct_b=+2) but YES ask=75 is outside neutral [40,60] —
    # Kalshi already reflects the move, no convergence edge left.
    assert (
        detect_cross_venue_lag(_snap(yes_ask=75, yes_bid=73, pct_b=2.0), min_confidence=0.3)
        is None
    )


def test_rejects_below_min_confidence() -> None:
    # pct_b=+1.5 → confidence = min(1.0, 1.5/2.0) = 0.75. min_confidence=0.9 → reject.
    assert detect_cross_venue_lag(_snap(pct_b=1.5), min_confidence=0.9) is None
