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
