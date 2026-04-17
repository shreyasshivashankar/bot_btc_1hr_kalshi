from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.obs import Clock, ManualClock, SystemClock


def test_system_clock_is_monotonic_nondecreasing() -> None:
    c = SystemClock()
    a = c.now_ns()
    b = c.now_ns()
    assert b >= a


def test_system_clock_satisfies_protocol() -> None:
    assert isinstance(SystemClock(), Clock)


def test_manual_clock_starts_at_zero_by_default() -> None:
    c = ManualClock()
    assert c.now_ns() == 0


def test_manual_clock_advance() -> None:
    c = ManualClock(start_ns=1_000)
    c.advance_ns(500)
    assert c.now_ns() == 1_500


def test_manual_clock_rejects_backward_advance() -> None:
    c = ManualClock()
    with pytest.raises(ValueError, match="monotonic"):
        c.advance_ns(-1)


def test_manual_clock_set_allows_forward() -> None:
    c = ManualClock(start_ns=100)
    c.set_ns(200)
    assert c.now_ns() == 200


def test_manual_clock_rejects_backward_set() -> None:
    c = ManualClock(start_ns=500)
    with pytest.raises(ValueError, match="monotonic"):
        c.set_ns(400)


def test_manual_clock_rejects_negative_start() -> None:
    with pytest.raises(ValueError):
        ManualClock(start_ns=-1)


def test_manual_clock_satisfies_protocol() -> None:
    assert isinstance(ManualClock(), Clock)
