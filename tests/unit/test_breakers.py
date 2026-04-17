from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.risk import BreakerState
from bot_btc_1hr_kalshi.risk.breaker_store import JsonFileBreakerStore


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


def test_drawdown_freeze_survives_restart_via_json_store(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Hard rule #3: a 60min lockout must survive a process restart. If the
    breaker state is purely in-memory, a crash mid-freeze would re-open the
    gate. The JSON file store keeps the deadline on disk."""
    path = tmp_path / "breaker.json"
    trip_ns = 1_000_000_000_000
    freeze_sec = 3600

    # Process A: trip the breaker.
    b1 = BreakerState(store=JsonFileBreakerStore(path))
    b1.trip_drawdown(now_ns=trip_ns, duration_sec=freeze_sec)
    assert b1.is_drawdown_frozen(now_ns=trip_ns)

    # Process B: fresh state object reads the same file and honors the freeze.
    b2 = BreakerState(store=JsonFileBreakerStore(path))
    assert b2.is_drawdown_frozen(now_ns=trip_ns + 1)
    assert b2.is_drawdown_frozen(now_ns=trip_ns + (freeze_sec - 1) * 1_000_000_000)
    # …and lets it expire at the original deadline.
    assert not b2.is_drawdown_frozen(now_ns=trip_ns + (freeze_sec + 1) * 1_000_000_000)


def test_breaker_store_tolerates_corrupt_file(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """A corrupt/partial JSON should not crash the boot path — just ignore it
    (operator will see the unexpected startup in logs)."""
    path = tmp_path / "breaker.json"
    path.write_text("{not json", encoding="utf-8")
    b = BreakerState(store=JsonFileBreakerStore(path))
    assert not b.any_tripped(now_ns=0)
