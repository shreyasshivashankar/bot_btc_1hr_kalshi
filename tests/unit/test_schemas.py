from __future__ import annotations

import pytest
from pydantic import ValidationError

from bot_btc_1hr_kalshi.obs import (
    BetOutcome,
    DecisionRecord,
    Features,
    Position,
    Sizing,
)


def _features() -> Features:
    return Features(
        regime_trend="flat",
        regime_vol="normal",
        signal_confidence=0.7,
        bollinger_pct_b=-0.05,
        atr_cents=12.0,
        book_depth_at_entry=4200.0,
        spread_cents=1,
        spot_btc_usd=65_000.0,
        minutes_to_settlement=45.0,
    )


def _sizing() -> Sizing:
    return Sizing(
        kelly_fraction=0.25,
        edge_cents=4.2,
        variance_estimate=0.09,
        notional_usd=50.0,
        contracts=10,
    )


def test_features_happy_path() -> None:
    f = _features()
    assert f.signal_confidence == 0.7
    # HTF + CVD fields default to None during warmup — existing callers that
    # omit them must still validate.
    assert f.rsi_5m is None
    assert f.rsi_1h is None
    assert f.move_24h_pct is None
    assert f.cvd_1m_usd is None


def test_features_cvd_field_populated_and_signed() -> None:
    """cvd_1m_usd is signed (buy minus sell); both negative and positive
    values must round-trip unchanged — no stray `ge=0` constraint."""
    pos = Features(
        regime_trend="up", regime_vol="normal",
        signal_confidence=0.5, bollinger_pct_b=0.0, atr_cents=1.0,
        book_depth_at_entry=1.0, spread_cents=1, spot_btc_usd=1.0,
        minutes_to_settlement=1.0,
        cvd_1m_usd=12_345.67,
    )
    neg = pos.model_copy(update={"cvd_1m_usd": -9_876.54})
    assert pos.cvd_1m_usd == 12_345.67
    assert neg.cvd_1m_usd == -9_876.54


def test_features_htf_fields_populated() -> None:
    f = Features(
        regime_trend="up",
        regime_vol="normal",
        signal_confidence=0.5,
        bollinger_pct_b=0.0,
        atr_cents=1.0,
        book_depth_at_entry=1.0,
        spread_cents=1,
        spot_btc_usd=1.0,
        minutes_to_settlement=1.0,
        rsi_5m=62.5,
        rsi_1h=48.0,
        move_24h_pct=-0.034,
    )
    assert f.rsi_5m == 62.5
    assert f.rsi_1h == 48.0
    assert f.move_24h_pct == -0.034


def test_features_rsi_bounds() -> None:
    with pytest.raises(ValidationError):
        Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=0.5,
            bollinger_pct_b=0.0,
            atr_cents=1.0,
            book_depth_at_entry=1.0,
            spread_cents=1,
            spot_btc_usd=1.0,
            minutes_to_settlement=1.0,
            rsi_5m=101.0,
        )


def test_features_confidence_bounds() -> None:
    with pytest.raises(ValidationError):
        Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=1.2,
            bollinger_pct_b=0.0,
            atr_cents=1.0,
            book_depth_at_entry=1.0,
            spread_cents=1,
            spot_btc_usd=1.0,
            minutes_to_settlement=1.0,
        )


def test_features_frozen() -> None:
    f = _features()
    with pytest.raises(ValidationError):
        f.signal_confidence = 0.9  # type: ignore[misc]


def test_features_forbid_extra() -> None:
    with pytest.raises(ValidationError):
        Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=0.5,
            bollinger_pct_b=0.0,
            atr_cents=1.0,
            book_depth_at_entry=1.0,
            spread_cents=1,
            spot_btc_usd=1.0,
            minutes_to_settlement=1.0,
            extra_field="oops",  # type: ignore[call-arg]
        )


def test_decision_record_happy_path() -> None:
    dr = DecisionRecord(
        decision_id="d-1",
        ts_ns=1_700_000_000_000_000_000,
        market_id="BTC-1H-2026-04-16-21",
        trap="floor_reversion",
        side="YES",
        entry_price_cents=43,
        features=_features(),
        sizing=_sizing(),
        approved=True,
    )
    assert dr.approved is True
    assert dr.reject_reason is None


def test_decision_record_price_bounds() -> None:
    with pytest.raises(ValidationError):
        DecisionRecord(
            decision_id="d-1",
            ts_ns=1,
            market_id="m",
            trap="floor_reversion",
            side="YES",
            entry_price_cents=101,
            features=_features(),
            sizing=_sizing(),
            approved=True,
        )


def test_position_requires_positive_contracts() -> None:
    with pytest.raises(ValidationError):
        Position(
            position_id="p-1",
            decision_id="d-1",
            market_id="m",
            side="YES",
            entry_price_cents=40,
            contracts=0,
            opened_at_ns=1,
        )


def test_bet_outcome_full_round_trip() -> None:
    bo = BetOutcome(
        bet_id="b-1",
        decision_id="d-1",
        market_id="BTC-1H-2026-04-16-21",
        trap="floor_reversion",
        side="YES",
        opened_at_ns=1_000_000,
        closed_at_ns=2_000_000,
        hold_duration_sec=1.0,
        entry_price_cents=43,
        exit_price_cents=55,
        contracts=10,
        gross_pnl_usd=1.20,
        fees_usd=0.04,
        net_pnl_usd=1.16,
        counterfactual_held_pnl_usd=5.7,
        exit_reason="early_cashout_99",
        features_at_entry=_features(),
    )
    assert bo.net_pnl_usd == 1.16
    # frozen
    with pytest.raises(ValidationError):
        bo.net_pnl_usd = 2.0  # type: ignore[misc]

    # JSON round-trip preserves values
    restored = BetOutcome.model_validate_json(bo.model_dump_json())
    assert restored == bo
