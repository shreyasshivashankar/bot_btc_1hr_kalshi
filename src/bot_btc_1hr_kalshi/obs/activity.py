"""Activity tracker — watchdog-facing liveness state.

External process watchdog (hard rule #3 spirit: no single point of failure
for halt). An external Cloud Scheduler job polls `/admin/status` every
minute; if `seconds_since_last_tick` crosses a threshold the event loop
is assumed wedged and the watchdog can escalate (page oncall, POST
`/admin/halt`, or kill the instance so Cloud Run reboots it).

Two signals matter:

  * `last_tick_ns`     — last time ANY market-data feed delivered data.
                         Silent means the websocket is dead or the event
                         loop is stuck.
  * `last_decision_ns` — last time the OMS evaluated `consider_entry`.
                         Silent in an hour when markets are open means
                         the decision path is wedged even if feeds tick.

Snapshots return plain ints/floats so the status endpoint can emit them
as JSON directly; the `seconds_since_*` derivations happen at snapshot
time against the injected clock to keep everything consistent with
hard rule #5 (no wall-clock in trading state).
"""

from __future__ import annotations

from typing import Any


class ActivityTracker:
    """Thread-unsafe, single-event-loop liveness recorder."""

    __slots__ = ("_boot_ns", "_last_decision_ns", "_last_tick_ns")

    def __init__(self, *, boot_ns: int) -> None:
        self._boot_ns = boot_ns
        self._last_tick_ns: int | None = None
        self._last_decision_ns: int | None = None

    def mark_tick(self, ts_ns: int) -> None:
        self._last_tick_ns = ts_ns

    def mark_decision(self, ts_ns: int) -> None:
        self._last_decision_ns = ts_ns

    @property
    def last_tick_ns(self) -> int | None:
        return self._last_tick_ns

    @property
    def last_decision_ns(self) -> int | None:
        return self._last_decision_ns

    def snapshot(self, *, now_ns: int) -> dict[str, Any]:
        return {
            "boot_ns": self._boot_ns,
            "uptime_seconds": max(0.0, (now_ns - self._boot_ns) / 1e9),
            "last_tick_ns": self._last_tick_ns,
            "last_decision_ns": self._last_decision_ns,
            "seconds_since_last_tick": (
                None if self._last_tick_ns is None
                else max(0.0, (now_ns - self._last_tick_ns) / 1e9)
            ),
            "seconds_since_last_decision": (
                None if self._last_decision_ns is None
                else max(0.0, (now_ns - self._last_decision_ns) / 1e9)
            ),
        }
