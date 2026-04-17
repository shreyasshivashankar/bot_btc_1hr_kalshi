"""Circuit-breaker state machine.

Breakers:
  - drawdown_60min_freeze: single-trade ≥15% loss triggers a 60-minute API
    lockout (hard rule #3 — no override).
  - feed_staleness: primary feed staleness > 2s → halt new entries.
  - clock_drift: wall-clock drift > 250ms vs NTP → halt.

All breaker state is checked by `risk.check()` before order submission.
"""

from __future__ import annotations

DRAWDOWN_FREEZE_SEC = 3600


class BreakerState:
    __slots__ = (
        "_clock_drift_halted",
        "_drawdown_frozen_until_ns",
        "_feed_staleness_halted",
    )

    def __init__(self) -> None:
        self._drawdown_frozen_until_ns: int | None = None
        self._feed_staleness_halted: bool = False
        self._clock_drift_halted: bool = False

    def trip_drawdown(self, now_ns: int, *, duration_sec: int = DRAWDOWN_FREEZE_SEC) -> None:
        if duration_sec <= 0:
            raise ValueError("duration_sec must be > 0")
        self._drawdown_frozen_until_ns = now_ns + duration_sec * 1_000_000_000

    def set_feed_halt(self, *, halted: bool) -> None:
        self._feed_staleness_halted = halted

    def set_clock_halt(self, *, halted: bool) -> None:
        self._clock_drift_halted = halted

    def is_drawdown_frozen(self, now_ns: int) -> bool:
        return self._drawdown_frozen_until_ns is not None and now_ns < self._drawdown_frozen_until_ns

    def any_tripped(self, now_ns: int) -> bool:
        return (
            self.is_drawdown_frozen(now_ns)
            or self._feed_staleness_halted
            or self._clock_drift_halted
        )

    def reason(self, now_ns: int) -> str:
        if self.is_drawdown_frozen(now_ns):
            return "drawdown_60min_freeze"
        if self._feed_staleness_halted:
            return "feed_staleness"
        if self._clock_drift_halted:
            return "clock_drift"
        return "none"
