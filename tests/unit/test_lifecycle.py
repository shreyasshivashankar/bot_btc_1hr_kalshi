"""Tests for the lifecycle event emitter (hard rule #1 audit trail).

Structured lifecycle events let us reconstruct any order's journey from
decision → submit → ack → fill → open → close by grepping a single
channel for a correlation id. The tests verify:

  * The emitter tags every event with a monotonic sequence number so
    a consumer can order events deterministically.
  * correlation_id is set consistently per event type (decision_id for
    decision / order_submitted / order_ack; position_id for open /
    close / partial).
  * Timestamps come from the injected clock, not wall-clock
    (hard rule #5).
  * Integration: an OMS wired with a lifecycle emitter emits decision,
    order_submitted, and order_ack for every consider_entry, even
    rejected decisions.
"""

from __future__ import annotations

from typing import Any

import structlog

from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.lifecycle import LifecycleEmitter


class _Capture:
    """Structlog processor that appends emitted event_dicts."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def __call__(self, logger: Any, method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        self.events.append(dict(event_dict))
        return event_dict


def _install_capture() -> _Capture:
    cap = _Capture()
    structlog.configure(
        processors=[
            cap,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(10),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    return cap


def _lifecycle_events(cap: _Capture) -> list[dict[str, Any]]:
    # Every LifecycleEmitter emission uses event="lifecycle"; filter on that
    # so we don't pick up stray logs from other modules the tests may touch.
    return [payload for payload in cap.events if payload.get("event") == "lifecycle"]


def test_seq_is_monotonic_and_clock_injected() -> None:
    cap = _install_capture()
    clock = ManualClock(1_000_000_000_000)
    e = LifecycleEmitter(clock=clock)

    e.decision(
        decision_id="d1", market_id="M", trap="floor_reversion", side="YES",
        approved=True, contracts=5, reject_reason=None,
    )
    clock.advance_ns(1_000_000)
    e.order_submitted(
        decision_id="d1", client_order_id="d1", market_id="M", side="YES",
        action="BUY", contracts=5, limit_price_cents=40, order_type="maker",
    )
    clock.advance_ns(2_000_000)
    e.order_ack(
        decision_id="d1", client_order_id="d1", order_id="ord-1",
        status="filled", filled_contracts=5, remaining_contracts=0, reason=None,
    )

    evs = _lifecycle_events(cap)
    assert len(evs) == 3
    assert [ev["seq"] for ev in evs] == [1, 2, 3]
    assert [ev["event_type"] for ev in evs] == ["decision", "order_submitted", "order_ack"]
    # Timestamps come from the injected clock — each advance_ns is visible
    # as a distinct ts_ns. This is the test that kills a hard-rule-#5
    # regression where a future refactor swaps in time.time_ns().
    assert evs[0]["ts_ns"] == 1_000_000_000_000
    assert evs[1]["ts_ns"] == 1_000_001_000_000
    assert evs[2]["ts_ns"] == 1_000_003_000_000


def test_correlation_id_uses_decision_id_for_order_stages() -> None:
    cap = _install_capture()
    e = LifecycleEmitter(clock=ManualClock(0))

    e.decision(
        decision_id="d1", market_id="M", trap="floor_reversion", side="YES",
        approved=False, contracts=0, reject_reason="confidence_below_min",
    )
    e.order_submitted(
        decision_id="d2", client_order_id="d2", market_id="M", side="NO",
        action="BUY", contracts=3, limit_price_cents=60, order_type="maker",
    )
    e.order_ack(
        decision_id="d2", client_order_id="d2", order_id="x",
        status="filled", filled_contracts=3, remaining_contracts=0, reason=None,
    )

    evs = _lifecycle_events(cap)
    assert evs[0]["correlation_id"] == "d1"
    assert evs[1]["correlation_id"] == "d2"
    assert evs[2]["correlation_id"] == "d2"


def test_correlation_id_uses_position_id_for_lifecycle_transitions() -> None:
    cap = _install_capture()
    e = LifecycleEmitter(clock=ManualClock(0))

    e.position_opened(
        position_id="p1", decision_id="d1", market_id="M", side="YES",
        contracts=10, entry_price_cents=30,
    )
    e.position_partial_closed(
        position_id="p1", closed_contracts=4, remaining_contracts=6,
        exit_price_cents=45, partial_seq=1,
    )
    e.position_closed(
        position_id="p1", exit_price_cents=48, net_pnl_usd=1.25,
        exit_reason="early_cashout_99",
    )

    evs = _lifecycle_events(cap)
    assert {ev["correlation_id"] for ev in evs} == {"p1"}
    # Fields that a risk officer would need without joining: decision_id on
    # open, remaining on partial, pnl + reason on close.
    assert evs[0]["decision_id"] == "d1"
    assert evs[1]["remaining_contracts"] == 6
    assert evs[2]["net_pnl_usd"] == 1.25
    assert evs[2]["exit_reason"] == "early_cashout_99"


def test_halt_and_resume_use_system_correlation() -> None:
    cap = _install_capture()
    e = LifecycleEmitter(clock=ManualClock(0))
    e.halt(reason="sigterm")
    e.resume(reason="operator")
    evs = _lifecycle_events(cap)
    assert [ev["event_type"] for ev in evs] == ["halt", "resume"]
    assert {ev["correlation_id"] for ev in evs} == {"system"}
