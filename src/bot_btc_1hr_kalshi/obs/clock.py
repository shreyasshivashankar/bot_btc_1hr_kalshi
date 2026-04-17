"""Injected clock abstraction (hard rule #5: no datetime.now() in trading logic).

`SystemClock` is used in paper/live; `ManualClock` drives deterministic
tick-replay backtests. Every module that needs a timestamp takes `Clock` as
a dependency and calls `clock.now_ns()`.
"""

from __future__ import annotations

import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    def now_ns(self) -> int:
        """Nanoseconds since Unix epoch (or from whatever origin the clock chooses).

        Must be monotonically non-decreasing within a single run.
        """


class SystemClock:
    """Wall-clock backed by `time.time_ns()`. Use in paper/live."""

    __slots__ = ()

    def now_ns(self) -> int:
        return time.time_ns()


class ManualClock:
    """Clock driven by the test/backtest harness.

    Start at an explicit ns (defaults to 0 so tests are deterministic).
    Advance with `advance_ns` or jump with `set_ns`. Going backwards is an
    error — it would violate the Clock contract.
    """

    __slots__ = ("_ns",)

    def __init__(self, start_ns: int = 0) -> None:
        if start_ns < 0:
            raise ValueError("start_ns must be >= 0")
        self._ns = start_ns

    def now_ns(self) -> int:
        return self._ns

    def advance_ns(self, delta_ns: int) -> None:
        if delta_ns < 0:
            raise ValueError("advance_ns must be >= 0 (clocks are monotonic)")
        self._ns += delta_ns

    def set_ns(self, ns: int) -> None:
        if ns < self._ns:
            raise ValueError(f"set_ns {ns} < current {self._ns} (clocks are monotonic)")
        self._ns = ns
