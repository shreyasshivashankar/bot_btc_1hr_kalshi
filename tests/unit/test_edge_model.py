from __future__ import annotations

import math

from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes


def test_at_the_money_is_half() -> None:
    q = settlement_prob_yes(
        spot_usd=60_000.0,
        strike_usd=60_000.0,
        sigma_per_minute_usd=10.0,
        minutes_to_settlement=30.0,
    )
    assert math.isclose(q, 0.5, abs_tol=1e-9)


def test_spot_far_above_strike_collapses_toward_one() -> None:
    q = settlement_prob_yes(
        spot_usd=61_000.0,
        strike_usd=60_000.0,
        sigma_per_minute_usd=5.0,
        minutes_to_settlement=10.0,
    )
    # 61000 - 60000 = 1000; sigma_total = 5*sqrt(10) ~= 15.81 -> z ~= 63 -> prob clipped to 0.999
    assert q >= 0.99


def test_spot_far_below_strike_collapses_toward_zero() -> None:
    q = settlement_prob_yes(
        spot_usd=59_000.0,
        strike_usd=60_000.0,
        sigma_per_minute_usd=5.0,
        minutes_to_settlement=10.0,
    )
    assert q <= 0.01


def test_is_clipped_away_from_endpoints() -> None:
    q_lo = settlement_prob_yes(
        spot_usd=1.0,
        strike_usd=60_000.0,
        sigma_per_minute_usd=1.0,
        minutes_to_settlement=1.0,
    )
    q_hi = settlement_prob_yes(
        spot_usd=60_000.0,
        strike_usd=1.0,
        sigma_per_minute_usd=1.0,
        minutes_to_settlement=1.0,
    )
    assert 0.0 < q_lo < 0.01
    assert 0.99 < q_hi < 1.0


def test_zero_time_or_zero_sigma_returns_deterministic_bounds() -> None:
    q_up = settlement_prob_yes(
        spot_usd=60_010.0, strike_usd=60_000.0,
        sigma_per_minute_usd=0.0, minutes_to_settlement=10.0,
    )
    q_down = settlement_prob_yes(
        spot_usd=59_990.0, strike_usd=60_000.0,
        sigma_per_minute_usd=10.0, minutes_to_settlement=0.0,
    )
    assert q_up == 0.999
    assert q_down == 0.001


def test_edge_cents_yes_when_fair_above_entry() -> None:
    # q_yes = 0.7 -> fair price = 70c. Entry at 40 -> edge = 30.
    assert math.isclose(edge_cents(side="YES", entry_price_cents=40, q_yes=0.7), 30.0)


def test_edge_cents_no_flips_probability() -> None:
    # q_yes = 0.7 -> q_no = 0.3 -> fair NO = 30c. Entry at 20 -> edge = 10.
    assert math.isclose(edge_cents(side="NO", entry_price_cents=20, q_yes=0.7), 10.0)


def test_edge_cents_clips_at_zero() -> None:
    # q_yes = 0.3, entry 50 -> raw = -20 -> clipped to 0.
    assert edge_cents(side="YES", entry_price_cents=50, q_yes=0.3) == 0.0
