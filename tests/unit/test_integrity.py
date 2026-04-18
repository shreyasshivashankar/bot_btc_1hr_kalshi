"""Tests for the Primary/Confirmation integrity gate.

Pins the three rules documented in signal/integrity.py:
  * Fail-closed on long confirmation silence.
  * Silence <= stale_halt_sec with no velocity datapoints → approve (trust primary).
  * Active directional disagreement → veto; concurring directions → approve.
"""

from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.signal.integrity import IntegrityTracker

SEC = 1_000_000_000


def _make(
    *,
    window_sec: float = 1.0,
    floor_usd: float = 25.0,
    stale_sec: float = 60.0,
) -> IntegrityTracker:
    return IntegrityTracker(
        velocity_window_sec=window_sec,
        active_disagreement_floor_usd=floor_usd,
        stale_halt_sec=stale_sec,
    )


def test_rejects_before_confirmation_ever_connects() -> None:
    t = _make()
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=500_000_000, price_usd=60_200.0)
    decision = t.check_entry(now_ns=1_000_000_000)
    assert not decision.approved
    assert "confirmation_never_connected" in decision.reason


def test_flat_primary_always_approved() -> None:
    """Primary velocity within the noise floor → nothing to confirm."""
    t = _make()
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=500_000_000, price_usd=60_005.0)  # +$5, below $25 floor
    t.record_confirmation(ts_ns=100_000_000, price_usd=59_800.0)
    # Confirmation price is $200 BELOW primary — pure basis divergence.
    # Must NOT veto: primary isn't *moving*, so there's nothing to disagree on.
    decision = t.check_entry(now_ns=1_000_000_000)
    assert decision.approved


def test_silence_on_confirmation_trusts_primary() -> None:
    """Confirmation printed recently (under stale threshold) but nothing in
    the velocity window → silence ≠ veto."""
    t = _make(stale_sec=60.0)
    # One confirmation tick ~10s ago. Fresh enough to not be "stale" but not
    # in the 1s velocity window.
    t.record_confirmation(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=10 * SEC, price_usd=60_000.0)
    t.record_primary(ts_ns=10 * SEC + 500_000_000, price_usd=60_200.0)
    decision = t.check_entry(now_ns=11 * SEC)
    assert decision.approved


def test_active_disagreement_vetoes_entry() -> None:
    """Primary dropped $200; confirmation RALLIED $100 in the same second.
    Opposite signs + both above floor → veto."""
    t = _make()
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=900_000_000, price_usd=59_800.0)  # -$200
    t.record_confirmation(ts_ns=0, price_usd=60_000.0)
    t.record_confirmation(ts_ns=900_000_000, price_usd=60_100.0)  # +$100
    decision = t.check_entry(now_ns=1_000_000_000)
    assert not decision.approved
    assert "active_disagreement" in decision.reason


def test_concurring_direction_approves() -> None:
    """Primary dropped $200; confirmation also dropped $150. Both moved
    the same direction → approve. (Basis blew out but that's fine.)"""
    t = _make()
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=900_000_000, price_usd=59_800.0)
    t.record_confirmation(ts_ns=0, price_usd=60_000.0)
    t.record_confirmation(ts_ns=900_000_000, price_usd=59_850.0)
    decision = t.check_entry(now_ns=1_000_000_000)
    assert decision.approved


def test_small_counter_velocity_below_floor_approves() -> None:
    """Primary crashed $200; confirmation drifted up $5 (noise). Not a veto."""
    t = _make(floor_usd=25.0)
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=900_000_000, price_usd=59_800.0)
    t.record_confirmation(ts_ns=0, price_usd=60_000.0)
    t.record_confirmation(ts_ns=900_000_000, price_usd=60_005.0)
    assert t.check_entry(now_ns=1_000_000_000).approved


def test_stale_confirmation_vetoes() -> None:
    """Confirmation hasn't printed in 90s; threshold is 60s → fail-closed."""
    t = _make(stale_sec=60.0)
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_confirmation(ts_ns=0, price_usd=60_000.0)
    # Window trim will drop the old datapoint but the *last-seen* timestamp
    # is what drives the stale check.
    decision = t.check_entry(now_ns=90 * SEC)
    assert not decision.approved
    assert "confirmation_stale" in decision.reason


def test_window_trim_drops_out_of_range_datapoints() -> None:
    """Old samples outside the 1s velocity window must not inflate velocity."""
    t = _make(window_sec=1.0)
    # Seed: $200 drop that's 5s ago — outside the window.
    t.record_primary(ts_ns=0, price_usd=60_000.0)
    t.record_primary(ts_ns=100_000_000, price_usd=59_800.0)
    # Recent flat primary + fresh confirmation trailing by $200 (basis, not velocity).
    t.record_primary(ts_ns=5 * SEC, price_usd=59_800.0)
    t.record_primary(ts_ns=5 * SEC + 500_000_000, price_usd=59_800.0)
    t.record_confirmation(ts_ns=5 * SEC, price_usd=59_600.0)
    t.record_confirmation(ts_ns=5 * SEC + 500_000_000, price_usd=59_600.0)
    assert t.check_entry(now_ns=6 * SEC).approved


def test_constructor_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError):
        IntegrityTracker(
            velocity_window_sec=0.0,
            active_disagreement_floor_usd=25.0,
            stale_halt_sec=60.0,
        )
    with pytest.raises(ValueError):
        IntegrityTracker(
            velocity_window_sec=1.0,
            active_disagreement_floor_usd=0.0,
            stale_halt_sec=60.0,
        )
    with pytest.raises(ValueError):
        IntegrityTracker(
            velocity_window_sec=1.0,
            active_disagreement_floor_usd=25.0,
            stale_halt_sec=0.0,
        )
