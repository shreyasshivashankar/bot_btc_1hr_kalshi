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
