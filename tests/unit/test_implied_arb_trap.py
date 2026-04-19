from __future__ import annotations

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.signal import MarketSnapshot, detect_implied_arb


def _book(best_bid: int, best_ask: int, *, valid: bool = True) -> L2Book:
    b = L2Book("BTC-1H")
    if valid:
        b.apply(
            BookUpdate(
                seq=1,
                ts_ns=1,
                market_id="BTC-1H",
                bids=(BookLevel(best_bid, 200),),
                asks=(BookLevel(best_ask, 200),),
                is_snapshot=True,
            )
        )
    return b


def _features(
    *,
    spot_range_60s: float | None = 5.0,
    atr_cents: float = 10.0,
    spot_btc_usd: float = 60_000.0,
) -> Features:
    return Features(
        regime_trend="flat",
        regime_vol="normal",
        signal_confidence=0.5,
        bollinger_pct_b=0.1,
        atr_cents=atr_cents,
        book_depth_at_entry=400.0,
        spread_cents=2,
        spot_btc_usd=spot_btc_usd,
        minutes_to_settlement=30.0,
        spot_range_60s=spot_range_60s,
    )


def _snap(
    *,
    best_bid: int,
    best_ask: int,
    spot: float = 60_000.0,
    strike: float = 60_000.0,
    spot_range_60s: float | None = 5.0,
    atr_cents: float = 10.0,
    valid: bool = True,
) -> MarketSnapshot:
    return MarketSnapshot(
        market_id="BTC-1H",
        book=_book(best_bid, best_ask, valid=valid),
        features=_features(
            spot_range_60s=spot_range_60s,
            atr_cents=atr_cents,
            spot_btc_usd=spot,
        ),
        spot_btc_usd=spot,
        minutes_to_settlement=30.0,
        strike_usd=strike,
    )


def test_fires_when_yes_ask_is_meaningfully_below_fair() -> None:
    # spot == strike so q_yes ≈ 0.5 → fair_value ≈ 50c. YES ask at 30c →
    # basis = 50-30 = 20c, well above the 15c threshold.
    sig = detect_implied_arb(
        _snap(best_bid=28, best_ask=30),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    )
    assert sig is not None
    assert sig.trap == "implied_basis_arb"
    assert sig.side == "YES"
    # Maker-only entry: we post at the YES best bid, not cross the ask.
    assert sig.entry_price_cents == 28
    assert sig.edge_cents > 0
    assert 0.5 < sig.confidence <= 1.0


def test_fires_when_yes_bid_is_meaningfully_above_fair_via_no_side() -> None:
    # YES bid at 70c, fair ~50c → basis = 20c (YES expensive, NO cheap).
    # Enter on NO at NO best bid = 100 - YES best ask = 100 - 72 = 28c.
    sig = detect_implied_arb(
        _snap(best_bid=70, best_ask=72),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    )
    assert sig is not None
    assert sig.trap == "implied_basis_arb"
    assert sig.side == "NO"
    assert sig.entry_price_cents == 28  # 100 - 72
    assert sig.edge_cents > 0


def test_rejects_within_threshold() -> None:
    # YES ask at 40, fair ~50 → basis = 10c, under 15c threshold.
    assert detect_implied_arb(
        _snap(best_bid=38, best_ask=40),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    ) is None


def test_dead_spot_veto_sweep() -> None:
    # Same juicy 20c basis setup, but the underlying has ranged $25 in
    # the last 60s — fail-closed on the sweep regardless.
    assert detect_implied_arb(
        _snap(best_bid=28, best_ask=30, spot_range_60s=25.0),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    ) is None


def test_dead_spot_veto_unknown_window_is_fail_closed() -> None:
    # Cold-start: no spot_range measurement yet → cannot prove the tape is
    # quiet → trap refuses to fire.
    assert detect_implied_arb(
        _snap(best_bid=28, best_ask=30, spot_range_60s=None),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    ) is None


def test_rejects_when_book_invalid() -> None:
    assert detect_implied_arb(
        _snap(best_bid=28, best_ask=30, valid=False),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    ) is None


def test_confidence_scales_with_excess_basis() -> None:
    # 15c basis -> confidence floor 0.5
    weak = detect_implied_arb(
        _snap(best_bid=33, best_ask=35),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    )
    # 25c basis -> confidence saturates at 1.0
    strong = detect_implied_arb(
        _snap(best_bid=23, best_ask=25),
        basis_threshold_cents=15,
        dead_spot_range_usd=20.0,
    )
    assert weak is not None
    assert strong is not None
    assert weak.confidence == 0.5
    assert strong.confidence == 1.0


def test_threshold_is_configurable() -> None:
    # 15c basis under a stricter 20c threshold → no fire.
    assert detect_implied_arb(
        _snap(best_bid=33, best_ask=35),
        basis_threshold_cents=20,
        dead_spot_range_usd=20.0,
    ) is None
    # Same setup loosened to 10c → fires.
    sig = detect_implied_arb(
        _snap(best_bid=33, best_ask=35),
        basis_threshold_cents=10,
        dead_spot_range_usd=20.0,
    )
    assert sig is not None
