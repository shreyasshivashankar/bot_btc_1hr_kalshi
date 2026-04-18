from __future__ import annotations

from bot_btc_1hr_kalshi.config.settings import SignalSettings
from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.signal import MarketSnapshot, run_traps


def _signal_settings(min_confidence: float = 0.3) -> SignalSettings:
    return SignalSettings(
        bollinger_period_bars=20,
        bollinger_std_mult=2.0,
        min_signal_confidence=min_confidence,
    )


def _snap(pct_b: float = -0.8) -> MarketSnapshot:
    book = L2Book("BTC-1H")
    book.apply(
        BookUpdate(
            seq=1,
            ts_ns=1,
            market_id="BTC-1H",
            bids=(BookLevel(18, 100),),
            asks=(BookLevel(20, 100),),
            is_snapshot=True,
        )
    )
    return MarketSnapshot(
        market_id="BTC-1H",
        book=book,
        features=Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=0.5,
            bollinger_pct_b=pct_b,
            atr_cents=10.0,
            book_depth_at_entry=200.0,
            spread_cents=2,
            spot_btc_usd=60_100.0,
            minutes_to_settlement=30.0,
        ),
        spot_btc_usd=60_100.0,
        minutes_to_settlement=30.0,
        strike_usd=60_000.0,
    )


def test_registry_returns_none_when_no_trap_fires() -> None:
    assert run_traps(_snap(pct_b=0.1), settings=_signal_settings()) is None


def test_registry_returns_floor_signal_when_present() -> None:
    sig = run_traps(_snap(pct_b=-0.9), settings=_signal_settings())
    assert sig is not None
    assert sig.trap == "floor_reversion"
