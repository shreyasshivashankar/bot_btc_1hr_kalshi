from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.risk import BreakerState


def test_no_breaker_starts_tripped() -> None:
    b = BreakerState()
    assert not b.any_tripped(now_ns=1_000)
    assert b.reason(now_ns=1_000) == "none"


def test_drawdown_freeze_lasts_its_duration() -> None:
    b = BreakerState()
    b.trip_drawdown(now_ns=1_000_000_000, duration_sec=60)
    assert b.any_tripped(now_ns=1_000_000_000)
    assert b.reason(now_ns=1_000_000_000) == "drawdown_60min_freeze"
    # still frozen just inside the window
    assert b.any_tripped(now_ns=1_000_000_000 + 59 * 1_000_000_000)
    # expires after its duration
    assert not b.any_tripped(now_ns=1_000_000_000 + 61 * 1_000_000_000)


def test_drawdown_rejects_nonpositive_duration() -> None:
    b = BreakerState()
    with pytest.raises(ValueError):
        b.trip_drawdown(now_ns=1, duration_sec=0)


def test_feed_halt_and_clear() -> None:
    b = BreakerState()
    b.set_feed_halt(halted=True)
    assert b.any_tripped(now_ns=0)
    assert b.reason(now_ns=0) == "feed_staleness"
    b.set_feed_halt(halted=False)
    assert not b.any_tripped(now_ns=0)


def test_drawdown_takes_precedence_over_other_breakers() -> None:
    b = BreakerState()
    b.trip_drawdown(now_ns=0, duration_sec=60)
    b.set_feed_halt(halted=True)
    assert b.reason(now_ns=0) == "drawdown_60min_freeze"
