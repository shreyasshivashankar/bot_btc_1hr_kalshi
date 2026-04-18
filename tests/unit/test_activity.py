"""Tests for ActivityTracker (watchdog heartbeat state)."""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.activity import ActivityTracker


def test_snapshot_is_none_when_never_marked() -> None:
    t = ActivityTracker(boot_ns=1_000_000_000)
    snap = t.snapshot(now_ns=2_000_000_000)
    assert snap["last_tick_ns"] is None
    assert snap["last_decision_ns"] is None
    assert snap["seconds_since_last_tick"] is None
    assert snap["seconds_since_last_decision"] is None
    assert snap["uptime_seconds"] == 1.0


def test_snapshot_reports_seconds_since_mark() -> None:
    t = ActivityTracker(boot_ns=0)
    t.mark_tick(1_000_000_000)            # t=1s
    t.mark_decision(2_500_000_000)        # t=2.5s
    snap = t.snapshot(now_ns=5_000_000_000)
    assert snap["seconds_since_last_tick"] == 4.0
    assert snap["seconds_since_last_decision"] == 2.5
    assert snap["uptime_seconds"] == 5.0


def test_mark_overrides_previous() -> None:
    t = ActivityTracker(boot_ns=0)
    t.mark_tick(1_000)
    t.mark_tick(2_000)
    snap = t.snapshot(now_ns=2_000)
    assert snap["last_tick_ns"] == 2_000
    assert snap["seconds_since_last_tick"] == 0.0


def test_clock_skew_never_produces_negative_seconds() -> None:
    # If monotonicity is ever violated (shouldn't happen with injected clocks,
    # but the watchdog endpoint shouldn't return negative freshness regardless).
    t = ActivityTracker(boot_ns=5_000_000_000)
    t.mark_tick(10_000_000_000)
    snap = t.snapshot(now_ns=8_000_000_000)
    assert snap["seconds_since_last_tick"] == 0.0
    assert snap["uptime_seconds"] == 3.0
