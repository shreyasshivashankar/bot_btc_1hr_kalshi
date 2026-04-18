"""Tests for research.divergence decision-stream comparator."""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.schemas import DecisionRecord, Features, Sizing
from bot_btc_1hr_kalshi.research.divergence import compare_decisions


def _features() -> Features:
    return Features(
        regime_trend="flat", regime_vol="normal",
        signal_confidence=0.7, bollinger_pct_b=-0.8,
        atr_cents=10.0, book_depth_at_entry=600.0,
        spread_cents=4, spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def _sizing(contracts: int = 5) -> Sizing:
    return Sizing(
        kelly_fraction=0.25, edge_cents=3.0, variance_estimate=0.24,
        notional_usd=contracts * 0.4, contracts=contracts,
    )


def _rec(
    decision_id: str,
    *,
    trap: str = "floor_reversion",
    side: str = "YES",
    price: int = 40,
    approved: bool = True,
    reject_reason: str | None = None,
    contracts: int = 5,
) -> DecisionRecord:
    return DecisionRecord(
        decision_id=decision_id, ts_ns=1, market_id="M",
        trap=trap,  # type: ignore[arg-type]
        side=side,  # type: ignore[arg-type]
        entry_price_cents=price,
        features=_features(), sizing=_sizing(contracts),
        approved=approved, reject_reason=reject_reason,
    )


def test_identical_streams_have_no_divergence() -> None:
    a = [_rec("d1"), _rec("d2")]
    # Different UUIDs, same decisions — reflects what we expect across runs.
    b = [_rec("x1"), _rec("x2")]
    r = compare_decisions(a, b)
    assert not r.diverged
    assert r.mismatches == []


def test_approve_reject_flip_is_caught() -> None:
    a = [_rec("d1", approved=True, reject_reason=None)]
    b = [_rec("x1", approved=False, reject_reason="confidence_below_min")]
    r = compare_decisions(a, b)
    assert r.diverged
    fields = {m.field for m in r.mismatches}
    assert "approved" in fields
    assert "reject_reason" in fields


def test_contract_count_mismatch_is_caught() -> None:
    a = [_rec("d1", contracts=5)]
    b = [_rec("x1", contracts=7)]
    r = compare_decisions(a, b)
    assert any(m.field == "sizing.contracts" for m in r.mismatches)


def test_length_mismatch_marks_diverged_even_with_matching_prefix() -> None:
    a = [_rec("d1"), _rec("d2")]
    b = [_rec("x1")]
    r = compare_decisions(a, b)
    assert r.diverged
    assert r.n_a == 2
    assert r.n_b == 1
    # Only the overlap is compared for field-level mismatches.
    assert r.mismatches == []
