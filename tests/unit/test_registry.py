from __future__ import annotations

from bot_btc_1hr_kalshi.config.settings import SignalSettings
from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.signal import MarketSnapshot, run_traps, run_traps_cross_strike


def _signal_settings(min_confidence: float = 0.3) -> SignalSettings:
    return SignalSettings(
        bollinger_period_bars=20,
        bollinger_std_mult=2.0,
        min_signal_confidence=min_confidence,
    )


def _snap(
    pct_b: float = -0.8,
    *,
    market_id: str = "BTC-1H",
    strike_usd: float = 60_000.0,
    spot_btc_usd: float = 60_100.0,
    bid_price: int = 18,
    ask_price: int = 20,
) -> MarketSnapshot:
    book = L2Book(market_id)
    book.apply(
        BookUpdate(
            seq=1,
            ts_ns=1,
            market_id=market_id,
            bids=(BookLevel(bid_price, 100),),
            asks=(BookLevel(ask_price, 100),),
            is_snapshot=True,
        )
    )
    return MarketSnapshot(
        market_id=market_id,
        book=book,
        features=Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=0.5,
            bollinger_pct_b=pct_b,
            atr_cents=10.0,
            book_depth_at_entry=200.0,
            spread_cents=2,
            spot_btc_usd=spot_btc_usd,
            minutes_to_settlement=30.0,
        ),
        spot_btc_usd=spot_btc_usd,
        minutes_to_settlement=30.0,
        strike_usd=strike_usd,
    )


def test_registry_returns_none_when_no_trap_fires() -> None:
    assert run_traps(_snap(pct_b=0.1), settings=_signal_settings()) is None


def test_registry_returns_floor_signal_when_present() -> None:
    sig = run_traps(_snap(pct_b=-0.9), settings=_signal_settings())
    assert sig is not None
    assert sig.trap == "floor_reversion"


def test_cross_strike_returns_none_when_no_strike_fires() -> None:
    snaps = [_snap(pct_b=0.1, market_id="S1"), _snap(pct_b=0.2, market_id="S2")]
    assert run_traps_cross_strike(snaps, settings=_signal_settings()) is None


def test_cross_strike_picks_highest_edge_times_confidence() -> None:
    """The cross-sectional evaluator must return the `(snap, signal)` pair
    maximizing `edge_cents * confidence`. A stronger-deviation strike wins
    over a weaker one even if both fire on the same trap at different
    confidence levels."""
    weak = _snap(pct_b=-0.85, market_id="STRIKE-WEAK")
    strong = _snap(pct_b=-0.99, market_id="STRIKE-STRONG")

    result = run_traps_cross_strike([weak, strong], settings=_signal_settings())
    assert result is not None
    chosen_snap, sig = result
    assert chosen_snap.market_id == "STRIKE-STRONG"
    assert sig.trap == "floor_reversion"


def test_cross_strike_ignores_strikes_with_no_trap() -> None:
    """A strike that doesn't fire any trap is simply dropped from
    consideration — a strike that DOES fire wins by default."""
    dud = _snap(pct_b=0.05, market_id="STRIKE-DUD")
    hit = _snap(pct_b=-0.9, market_id="STRIKE-HIT")

    result = run_traps_cross_strike([dud, hit], settings=_signal_settings())
    assert result is not None
    chosen_snap, _ = result
    assert chosen_snap.market_id == "STRIKE-HIT"


def test_cross_strike_deterministic_tiebreak_by_market_id() -> None:
    """Two snapshots with identical scores (same strike, same book, same
    features — only the ticker differs) must resolve deterministically so
    ops can diff successive runs against replay. Alphabetically earliest
    ticker wins, regardless of input order."""
    a = _snap(pct_b=-0.9, market_id="STRIKE-A", strike_usd=60_000.0, spot_btc_usd=60_000.0)
    b = _snap(pct_b=-0.9, market_id="STRIKE-B", strike_usd=60_000.0, spot_btc_usd=60_000.0)

    r1 = run_traps_cross_strike([a, b], settings=_signal_settings())
    r2 = run_traps_cross_strike([b, a], settings=_signal_settings())
    assert r1 is not None and r2 is not None
    assert r1[0].market_id == "STRIKE-A"
    assert r2[0].market_id == "STRIKE-A"  # same answer regardless of input order


def test_cross_strike_prefers_higher_edge_strike() -> None:
    """Different strikes produce different `q_yes` → different edge_cents.
    For a floor-reversion YES buy, a strike further BELOW spot has higher
    settlement probability, and therefore higher edge. The cross-sectional
    evaluator should pick it even when ticker sort would go the other way."""
    # For YES buy: STRIKE-HIGH_Q (strike=59_000 with spot=60_000) has q_yes >> 0.5
    # STRIKE-LOW_Q (strike=60_500 with spot=60_000) has q_yes < 0.5.
    high_q = _snap(pct_b=-0.9, market_id="STRIKE-ALPHA", strike_usd=59_000.0, spot_btc_usd=60_000.0)
    low_q = _snap(pct_b=-0.9, market_id="STRIKE-BETA", strike_usd=60_500.0, spot_btc_usd=60_000.0)

    result = run_traps_cross_strike([high_q, low_q], settings=_signal_settings())
    assert result is not None
    # STRIKE-ALPHA also happens to alpha-sort first, but the edge_cents
    # difference is what drives the choice here. Reverse-order call also
    # picks the same winner:
    assert result[0].market_id == "STRIKE-ALPHA"
    reversed_result = run_traps_cross_strike([low_q, high_q], settings=_signal_settings())
    assert reversed_result is not None
    assert reversed_result[0].market_id == "STRIKE-ALPHA"


def test_cross_strike_empty_input_returns_none() -> None:
    assert run_traps_cross_strike([], settings=_signal_settings()) is None
