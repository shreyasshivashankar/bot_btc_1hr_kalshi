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


def _snap(ask_price: int = 35, **kwargs: object) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(ask_price=ask_price, valid=bool(kwargs.pop("valid", True))),
        features=_features(**kwargs),  # type: ignore[arg-type]
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def test_fires_on_clear_floor_setup() -> None:
    sig = detect_floor_reversion(_snap(ask_price=25, pct_b=-0.8), min_confidence=0.3)
    assert sig is not None
    assert sig.trap == "floor_reversion"
    assert sig.side == "YES"
    assert sig.entry_price_cents == 25
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
