"""CalendarGuard: pre-emptive flatten at T-lead before every tier-1 event.

Hard rule #8: tier-1 news overrides flatten the book (winners AND losers) —
no PnL-conditional liquidation, no NLP triggers. The guard is the only
automatic trigger for tier1_override; the human kill-switch remains the
backstop.

Call `tick()` on a fixed cadence (driven by the main event loop). The guard
keeps a set of already-fired event names so we never flatten twice for the
same event even if the tick cadence is slightly faster than we expect.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass

import structlog

from bot_btc_1hr_kalshi.calendar.events import ScheduledEvent
from bot_btc_1hr_kalshi.obs.clock import Clock

log = structlog.get_logger(__name__)

_DEFAULT_LEAD_SECONDS = 60.0


@dataclass(frozen=True, slots=True)
class GuardTick:
    fired: tuple[str, ...]
    considered: int


class CalendarGuard:
    """Drives pre-emptive tier-1 overrides against an injected clock."""

    __slots__ = ("_clock", "_events", "_fired", "_lead_ns", "_trigger")

    def __init__(
        self,
        *,
        clock: Clock,
        events: Iterable[ScheduledEvent],
        trigger: Callable[[], Awaitable[object]],
        lead_seconds: float = _DEFAULT_LEAD_SECONDS,
    ) -> None:
        if lead_seconds <= 0:
            raise ValueError("lead_seconds must be > 0")
        self._clock = clock
        self._events = tuple(sorted(events, key=lambda e: e.ts_ns))
        self._trigger = trigger
        self._lead_ns = int(lead_seconds * 1_000_000_000)
        self._fired: set[str] = set()

    @property
    def events(self) -> tuple[ScheduledEvent, ...]:
        return self._events

    @property
    def already_fired(self) -> frozenset[str]:
        return frozenset(self._fired)

    def replace_events(self, new: Iterable[ScheduledEvent]) -> None:
        """Swap the scheduled-event list in place. Preserves `_fired` so
        a refresh that re-returns an already-fired event does not cause
        a double-flatten. Used by the Forex Factory refresher (Slice 11
        P1) — the FF endpoint publishes a rolling current-week window,
        so most refreshes repeat events we've already seen and most
        newly added events are further out on the calendar.
        """
        self._events = tuple(sorted(new, key=lambda e: e.ts_ns))

    async def tick(self) -> GuardTick:
        now_ns = self._clock.now_ns()
        fired_now: list[str] = []
        considered = 0
        for ev in self._events:
            if not ev.is_tier_one:
                continue
            considered += 1
            if ev.name in self._fired:
                continue
            if now_ns + self._lead_ns < ev.ts_ns:
                # Event is still beyond our lead window; because events are
                # sorted by ts_ns, nothing after this one can fire either.
                break
            if now_ns >= ev.ts_ns:
                # Window missed entirely (e.g. container started after event).
                # Mark fired so we never retroactively flatten post-event, but
                # do not trigger: post-event flatten is the human's call.
                self._fired.add(ev.name)
                log.warning("calendar_event_missed", name=ev.name, ts_ns=ev.ts_ns, now_ns=now_ns)
                continue
            self._fired.add(ev.name)
            log.info("calendar_tier1_trigger", name=ev.name, ts_ns=ev.ts_ns, now_ns=now_ns)
            await self._trigger()
            fired_now.append(ev.name)
        return GuardTick(fired=tuple(fired_now), considered=considered)
