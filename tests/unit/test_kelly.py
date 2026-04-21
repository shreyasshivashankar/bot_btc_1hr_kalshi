from __future__ import annotations

from bot_btc_1hr_kalshi.risk import kelly_contracts


def test_zero_edge_sizes_zero() -> None:
    assert (
        kelly_contracts(
            edge_cents=0.0,
            entry_price_cents=30,
            kelly_fraction=0.25,
            bankroll_usd=1000.0,
            max_notional_usd=100.0,
        )
        == 0
    )


def test_negative_edge_sizes_zero() -> None:
    assert (
        kelly_contracts(
            edge_cents=-5.0,
            entry_price_cents=30,
            kelly_fraction=0.25,
            bankroll_usd=1000.0,
            max_notional_usd=100.0,
        )
        == 0
    )


def test_positive_edge_produces_positive_contracts() -> None:
    n = kelly_contracts(
        edge_cents=10.0,
        entry_price_cents=30,
        kelly_fraction=0.25,
        bankroll_usd=1000.0,
        max_notional_usd=100.0,
    )
    assert n > 0


def test_contract_notional_never_exceeds_cap() -> None:
    # huge bankroll, tiny cap → sizing should floor at the cap
    n = kelly_contracts(
        edge_cents=30.0,
        entry_price_cents=30,
        kelly_fraction=0.25,
        bankroll_usd=10_000_000.0,
        max_notional_usd=50.0,
    )
    notional = n * (30 / 100.0)
    assert notional <= 50.0 + 0.30  # at most 1 contract's rounding slop


def test_invalid_price_returns_zero() -> None:
    for price in (0, 100, -1, 101):
        assert (
            kelly_contracts(
                edge_cents=10.0,
                entry_price_cents=price,
                kelly_fraction=0.25,
                bankroll_usd=1000.0,
                max_notional_usd=100.0,
            )
            == 0
        )


def test_higher_kelly_fraction_sizes_larger() -> None:
    kwargs = {
        "edge_cents": 10.0,
        "entry_price_cents": 30,
        "bankroll_usd": 1000.0,
        "max_notional_usd": 10_000.0,
    }
    quarter = kelly_contracts(kelly_fraction=0.25, **kwargs)  # type: ignore[arg-type]
    half = kelly_contracts(kelly_fraction=0.5, **kwargs)  # type: ignore[arg-type]
    assert half > quarter


def test_zero_bankroll_or_cap_sizes_zero() -> None:
    assert (
        kelly_contracts(
            edge_cents=10.0,
            entry_price_cents=30,
            kelly_fraction=0.25,
            bankroll_usd=0.0,
            max_notional_usd=100.0,
        )
        == 0
    )
    assert (
        kelly_contracts(
            edge_cents=10.0,
            entry_price_cents=30,
            kelly_fraction=0.25,
            bankroll_usd=1000.0,
            max_notional_usd=0.0,
        )
        == 0
    )


def test_inverted_risk_clip_defaults_are_noop() -> None:
    """Without opting in, sizing is unchanged — callers that predate the clip
    (existing tests, any external call site) keep their prior behavior."""
    kwargs = {
        "edge_cents": 10.0,
        "kelly_fraction": 0.25,
        "bankroll_usd": 1000.0,
        "max_notional_usd": 10_000.0,
    }
    baseline = kelly_contracts(entry_price_cents=60, **kwargs)  # type: ignore[arg-type]
    with_disabled_clip = kelly_contracts(
        entry_price_cents=60,
        inverted_risk_threshold_cents=100,
        inverted_risk_kelly_multiplier=1.0,
        **kwargs,  # type: ignore[arg-type]
    )
    assert baseline == with_disabled_clip


def test_inverted_risk_clip_halves_sizing_at_threshold() -> None:
    """At/above threshold, the fractional Kelly is multiplied by the
    multiplier — so 0.5 multiplier should roughly halve contract count in
    the cap-unconstrained regime. Use a bankroll large enough that the
    per-position notional cap doesn't distort the comparison."""
    kwargs = {
        "edge_cents": 10.0,
        "entry_price_cents": 60,
        "kelly_fraction": 0.25,
        "bankroll_usd": 1000.0,
        "max_notional_usd": 10_000.0,  # deliberately large — leave Kelly unconstrained
    }
    unclipped = kelly_contracts(
        inverted_risk_threshold_cents=100,
        inverted_risk_kelly_multiplier=1.0,
        **kwargs,  # type: ignore[arg-type]
    )
    clipped = kelly_contracts(
        inverted_risk_threshold_cents=50,
        inverted_risk_kelly_multiplier=0.5,
        **kwargs,  # type: ignore[arg-type]
    )
    # Halving the allocation fraction halves the dollar bet which halves
    # contracts (within ±1 for integer rounding at the bet_usd / p_price step).
    assert clipped * 2 >= unclipped - 1
    assert clipped * 2 <= unclipped + 1


def test_inverted_risk_clip_inactive_below_threshold() -> None:
    """At a price strictly below the threshold, the clip multiplier must
    not apply — else the "at or above" boundary is meaningless and low-
    price bets get doubly penalized."""
    kwargs = {
        "edge_cents": 10.0,
        "entry_price_cents": 49,
        "kelly_fraction": 0.25,
        "bankroll_usd": 1000.0,
        "max_notional_usd": 10_000.0,
    }
    without_clip = kelly_contracts(
        inverted_risk_threshold_cents=100,
        inverted_risk_kelly_multiplier=1.0,
        **kwargs,  # type: ignore[arg-type]
    )
    with_clip_config = kelly_contracts(
        inverted_risk_threshold_cents=50,
        inverted_risk_kelly_multiplier=0.5,
        **kwargs,  # type: ignore[arg-type]
    )
    assert without_clip == with_clip_config


def test_inverted_risk_clip_fires_exactly_at_threshold() -> None:
    """Boundary is inclusive (>=): a 50¢ entry with threshold=50 must be
    clipped. Off-by-one here would silently exempt exactly-even-money
    bets, which are the central case the clip is designed to size down."""
    at_threshold = kelly_contracts(
        edge_cents=10.0,
        entry_price_cents=50,
        kelly_fraction=0.25,
        bankroll_usd=1000.0,
        max_notional_usd=10_000.0,
        inverted_risk_threshold_cents=50,
        inverted_risk_kelly_multiplier=0.5,
    )
    unclipped = kelly_contracts(
        edge_cents=10.0,
        entry_price_cents=50,
        kelly_fraction=0.25,
        bankroll_usd=1000.0,
        max_notional_usd=10_000.0,
    )
    assert at_threshold < unclipped
