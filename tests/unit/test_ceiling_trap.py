from __future__ import annotations

from typing import Literal

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
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
    yes_ask: int = 80,
    yes_bid: int = 78,
    pct_b: float = 0.8,
    regime_vol: RegimeVol = "normal",
    valid: bool = True,
    spot: float = 60_000.0,
    strike: float = 60_000.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(yes_ask=yes_ask, yes_bid=yes_bid, valid=valid),
        features=_features(pct_b=pct_b, regime_vol=regime_vol),
        spot_btc_usd=spot,
        minutes_to_settlement=30.0,
        strike_usd=strike,
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
