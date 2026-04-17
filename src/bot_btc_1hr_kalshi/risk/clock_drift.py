"""Clock-drift monitor (hard rule: wall-clock drift > 250ms vs NTP → halt).

Our SystemClock wraps `time.time_ns()`. If the host's clock drifts, signed
Kalshi requests get rejected and event timestamps misorder. This monitor
takes an injectable async probe that returns an authoritative reference
time (NTP server, broker `Date` header, etc.) and flips the clock-drift
breaker if |clock.now_ns() - reference| exceeds the threshold.

Production is expected to wire a real NTP probe (e.g. ntplib over UDP,
cached). A no-op probe is shipped for tests; leaving it wired in production
effectively disables the check, so boot logs a warning if the shipped
no-op probe is used.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.logging import get_logger
from bot_btc_1hr_kalshi.risk.breakers import BreakerState

NtpProbe = Callable[[], Awaitable[int]]


class ClockDriftMonitor:
    """Polls `probe` every `interval_sec` and trips the clock-drift breaker
    when |local - reference| > threshold_ms."""

    def __init__(
        self,
        *,
        clock: Clock,
        breakers: BreakerState,
        probe: NtpProbe,
        interval_sec: float = 30.0,
        threshold_ms: int = 250,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        if threshold_ms <= 0:
            raise ValueError("threshold_ms must be > 0")
        self._clock = clock
        self._breakers = breakers
        self._probe = probe
        self._interval = interval_sec
        self._threshold_ns = threshold_ms * 1_000_000
        self._log = get_logger("bot_btc_1hr_kalshi.clock_drift")
        self._last_halted: bool = False

    async def check_once(self) -> tuple[int, bool]:
        """Returns (drift_ns, is_halted_now). Drift sign: local - reference."""
        try:
            ref = await self._probe()
        except Exception as exc:
            # Probe failure is noisy but not halt-worthy on its own. Log and
            # carry forward the previous halt state.
            self._log.warning("clock_drift.probe_failed", error=str(exc))
            return 0, self._last_halted

        drift = self._clock.now_ns() - ref
        over = abs(drift) > self._threshold_ns

        if over and not self._last_halted:
            self._log.error(
                "clock_drift.halt", drift_ns=drift, threshold_ns=self._threshold_ns,
            )
        elif not over and self._last_halted:
            self._log.info("clock_drift.recovered", drift_ns=drift)

        self._breakers.set_clock_halt(halted=over)
        self._last_halted = over
        return drift, over

    async def run(self) -> None:
        while True:
            await self.check_once()
            await asyncio.sleep(self._interval)


def self_clock_probe(clock: Clock) -> NtpProbe:
    """A probe that reports the injected clock time — i.e. drift is always 0.
    The breaker never trips. Use as an explicit opt-out when a real NTP probe
    is not yet wired; boot code should log a visible warning when this is in
    use. Production MUST replace this before live trading."""

    async def _probe() -> int:
        return clock.now_ns()

    return _probe
