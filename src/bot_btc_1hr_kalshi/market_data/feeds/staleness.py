"""Per-feed staleness tracker.

Updated by the feed whenever a message is received. Checked by the App's
periodic watchdog; if any primary feed is stale for > threshold, the
feed_staleness breaker trips and `risk.check()` rejects all new entries
(see DESIGN.md §5.3).
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.clock import Clock


class StalenessTracker:
    __slots__ = ("_clock", "_last_msg_ns", "_name", "_threshold_ns")

    def __init__(self, *, name: str, clock: Clock, threshold_ms: int) -> None:
        if threshold_ms <= 0:
            raise ValueError("threshold_ms must be > 0")
        self._name = name
        self._clock = clock
        self._threshold_ns = threshold_ms * 1_000_000
        self._last_msg_ns: int | None = None

    @property
    def name(self) -> str:
        return self._name

    def mark(self) -> None:
        self._last_msg_ns = self._clock.now_ns()

    def mark_at(self, ts_ns: int) -> None:
        self._last_msg_ns = ts_ns

    @property
    def last_msg_ns(self) -> int | None:
        return self._last_msg_ns

    def is_stale(self) -> bool:
        if self._last_msg_ns is None:
            return False  # haven't started yet; boot-time is handled elsewhere
        return (self._clock.now_ns() - self._last_msg_ns) > self._threshold_ns

    def age_ms(self) -> float | None:
        if self._last_msg_ns is None:
            return None
        return (self._clock.now_ns() - self._last_msg_ns) / 1_000_000
