from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.risk.clock_drift import ClockDriftMonitor, self_clock_probe


async def test_drift_within_threshold_does_not_halt() -> None:
    clock = ManualClock(1_000_000_000_000)
    breakers = BreakerState()

    async def probe() -> int:
        # 100ms behind — below 250ms threshold.
        return clock.now_ns() - 100_000_000

    mon = ClockDriftMonitor(clock=clock, breakers=breakers, probe=probe, threshold_ms=250)
    drift, halted = await mon.check_once()
    assert not halted
    assert drift == 100_000_000
    assert not breakers.any_tripped(clock.now_ns())


async def test_drift_over_threshold_halts() -> None:
    clock = ManualClock(1_000_000_000_000)
    breakers = BreakerState()

    async def probe() -> int:
        # 500ms ahead of us — we are *behind* by 500ms.
        return clock.now_ns() + 500_000_000

    mon = ClockDriftMonitor(clock=clock, breakers=breakers, probe=probe, threshold_ms=250)
    _, halted = await mon.check_once()
    assert halted
    assert breakers.reason(clock.now_ns()) == "clock_drift"


async def test_drift_recovery_clears_halt() -> None:
    clock = ManualClock(1_000_000_000_000)
    breakers = BreakerState()
    state = {"drift": 500_000_000}

    async def probe() -> int:
        return clock.now_ns() - state["drift"]

    mon = ClockDriftMonitor(clock=clock, breakers=breakers, probe=probe, threshold_ms=250)
    _, halted = await mon.check_once()
    assert halted

    state["drift"] = 50_000_000  # now within threshold
    _, halted = await mon.check_once()
    assert not halted
    assert not breakers.any_tripped(clock.now_ns())


async def test_probe_exception_does_not_flap_breaker() -> None:
    """A flaky NTP server should not toggle the halt on every probe failure."""
    clock = ManualClock(1_000_000_000_000)
    breakers = BreakerState()

    async def bad_probe() -> int:
        raise RuntimeError("ntp down")

    mon = ClockDriftMonitor(clock=clock, breakers=breakers, probe=bad_probe)
    _, halted = await mon.check_once()
    assert not halted
    assert not breakers.any_tripped(clock.now_ns())


async def test_self_clock_probe_reports_zero_drift() -> None:
    """The opt-out probe must never trip the breaker."""
    clock = ManualClock(1_000_000_000_000)
    breakers = BreakerState()
    mon = ClockDriftMonitor(
        clock=clock, breakers=breakers, probe=self_clock_probe(clock), threshold_ms=1,
    )
    drift, halted = await mon.check_once()
    assert drift == 0
    assert not halted


def test_monitor_validates_constructor_args() -> None:
    clock = ManualClock(0)
    breakers = BreakerState()
    with pytest.raises(ValueError):
        ClockDriftMonitor(
            clock=clock, breakers=breakers, probe=self_clock_probe(clock), interval_sec=0,
        )
    with pytest.raises(ValueError):
        ClockDriftMonitor(
            clock=clock, breakers=breakers, probe=self_clock_probe(clock), threshold_ms=0,
        )
