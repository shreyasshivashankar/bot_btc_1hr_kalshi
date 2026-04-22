from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.calendar import CalendarGuard, ScheduledEvent
from bot_btc_1hr_kalshi.obs.clock import ManualClock

NS_PER_SEC = 1_000_000_000


class _TriggerSpy:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self) -> None:
        self.calls += 1


def _event(name: str, ts_sec: int, *, tier_one: bool = True) -> ScheduledEvent:
    return ScheduledEvent(
        name=name,
        ts_ns=ts_sec * NS_PER_SEC,
        importance="tier_1" if tier_one else "tier_2",
    )


async def test_fires_once_inside_lead_window() -> None:
    clock = ManualClock(start_ns=0)
    trigger = _TriggerSpy()
    guard = CalendarGuard(
        clock=clock,
        events=[_event("CPI", 120)],
        trigger=trigger,
        lead_seconds=60.0,
    )
    # T = 30s, event at T=120s → still outside lead window (60s).
    clock.set_ns(30 * NS_PER_SEC)
    t1 = await guard.tick()
    assert t1.fired == ()
    assert trigger.calls == 0

    # T = 65s, event at T=120s → 120 - 65 = 55s <= lead → fire.
    clock.set_ns(65 * NS_PER_SEC)
    t2 = await guard.tick()
    assert t2.fired == ("CPI",)
    assert trigger.calls == 1

    # Repeat tick must not re-fire.
    clock.set_ns(70 * NS_PER_SEC)
    t3 = await guard.tick()
    assert t3.fired == ()
    assert trigger.calls == 1


async def test_ignores_non_tier_one_events() -> None:
    clock = ManualClock(start_ns=0)
    trigger = _TriggerSpy()
    guard = CalendarGuard(
        clock=clock,
        events=[_event("Retail_Sales", 120, tier_one=False)],
        trigger=trigger,
        lead_seconds=60.0,
    )
    clock.set_ns(65 * NS_PER_SEC)
    tick = await guard.tick()
    assert tick.fired == ()
    assert tick.considered == 0
    assert trigger.calls == 0


async def test_missed_event_is_marked_fired_not_triggered() -> None:
    clock = ManualClock(start_ns=200 * NS_PER_SEC)  # boot past the event
    trigger = _TriggerSpy()
    guard = CalendarGuard(
        clock=clock,
        events=[_event("CPI", 120)],
        trigger=trigger,
        lead_seconds=60.0,
    )
    tick = await guard.tick()
    assert tick.fired == ()
    assert trigger.calls == 0
    assert "CPI" in guard.already_fired


async def test_lead_seconds_must_be_positive() -> None:
    with pytest.raises(ValueError, match="lead_seconds must be > 0"):
        CalendarGuard(
            clock=ManualClock(),
            events=[],
            trigger=_TriggerSpy(),
            lead_seconds=0.0,
        )


async def test_cooldown_seconds_must_be_positive() -> None:
    with pytest.raises(ValueError, match="cooldown_seconds must be > 0"):
        CalendarGuard(
            clock=ManualClock(),
            events=[],
            trigger=_TriggerSpy(),
            lead_seconds=60.0,
            cooldown_seconds=0.0,
        )


async def test_is_blocked_covers_pre_event_cooldown_and_outside_windows() -> None:
    clock = ManualClock(start_ns=0)
    guard = CalendarGuard(
        clock=clock,
        events=[_event("CPI", 3600)],
        trigger=_TriggerSpy(),
        lead_seconds=60.0,
        cooldown_seconds=1800.0,
    )
    # Before lead window (>60s before event).
    assert guard.is_blocked(3000 * NS_PER_SEC) is False
    # Inside lead window (T-30s).
    assert guard.is_blocked(3570 * NS_PER_SEC) is True
    # Exactly at event.
    assert guard.is_blocked(3600 * NS_PER_SEC) is True
    # Inside cooldown (T+10min).
    assert guard.is_blocked((3600 + 600) * NS_PER_SEC) is True
    # Exactly at cooldown boundary.
    assert guard.is_blocked((3600 + 1800) * NS_PER_SEC) is True
    # After cooldown.
    assert guard.is_blocked((3600 + 1801) * NS_PER_SEC) is False


async def test_is_blocked_ignores_non_tier_one_events() -> None:
    guard = CalendarGuard(
        clock=ManualClock(),
        events=[_event("Retail_Sales", 3600, tier_one=False)],
        trigger=_TriggerSpy(),
        lead_seconds=60.0,
        cooldown_seconds=1800.0,
    )
    assert guard.is_blocked(3600 * NS_PER_SEC) is False


async def test_is_blocked_still_true_after_event_fired() -> None:
    # The `_fired` ledger prevents double-flatten, but the blackout window
    # must still block new entries through the cooldown half.
    clock = ManualClock(start_ns=0)
    guard = CalendarGuard(
        clock=clock,
        events=[_event("CPI", 120)],
        trigger=_TriggerSpy(),
        lead_seconds=60.0,
        cooldown_seconds=1800.0,
    )
    clock.set_ns(70 * NS_PER_SEC)
    await guard.tick()
    assert "CPI" in guard.already_fired
    # T+10min after the event is well inside the cooldown.
    assert guard.is_blocked((120 + 600) * NS_PER_SEC) is True


async def test_is_blocked_unions_overlapping_event_windows() -> None:
    guard = CalendarGuard(
        clock=ManualClock(),
        events=[_event("FOMC", 3600), _event("NFP", 4000)],
        trigger=_TriggerSpy(),
        lead_seconds=60.0,
        cooldown_seconds=1800.0,
    )
    # Between FOMC cooldown end (5400) and NFP cooldown start (3940) — the
    # two windows overlap, so any point between 3540 and 5800 is blocked.
    assert guard.is_blocked(3800 * NS_PER_SEC) is True
    assert guard.is_blocked(5000 * NS_PER_SEC) is True
    assert guard.is_blocked(6000 * NS_PER_SEC) is False


async def test_sorted_iteration_short_circuits_future_events() -> None:
    # If the first future tier-1 is still outside the lead window, the guard
    # must not consider later events at all — guarantees O(1) amortized ticks.
    clock = ManualClock(start_ns=0)
    trigger = _TriggerSpy()
    events = [_event(f"E{i}", 3600 + i * 60) for i in range(10)]
    guard = CalendarGuard(
        clock=clock,
        events=events,
        trigger=trigger,
        lead_seconds=60.0,
    )
    tick = await guard.tick()
    assert tick.fired == ()
    # Only the first (earliest) tier-1 event was considered before break.
    assert tick.considered == 1
