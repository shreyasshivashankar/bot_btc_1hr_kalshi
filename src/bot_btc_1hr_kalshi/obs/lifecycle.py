"""Lifecycle event log — structured trace of every order lifecycle stage.

Audit goal: given a `decision_id`, be able to reconstruct the full
journey from signal → decision → submit → ack → fill → open → close →
settle purely from log records, without correlating across DecisionRecord
+ BetOutcome + broker logs. This is the audit trail a risk officer runs
when investigating an outlier bet.

We do NOT ship the full event-sourcing store (no replay-from-events, no
hash chain). See feasibility note in the commit message: pure append-only
JSONL with a monotonic sequence number and decision-id correlation is
the 80/20 — crypto chaining adds complexity without an external audit
process to verify it.

Events are emitted to the dedicated logger
`bot_btc_1hr_kalshi.lifecycle`, which a Cloud Logging sink routes to
BigQuery `bot_btc_1hr_kalshi.lifecycle_events`. Existing `bet_outcomes`
emissions are unchanged — lifecycle is an additive observability layer,
not a replacement.
"""

from __future__ import annotations

import uuid
from typing import Any, Literal

from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.logging import get_logger

LIFECYCLE_LOGGER = "bot_btc_1hr_kalshi.lifecycle"

LifecycleEventType = Literal[
    "decision",
    "order_submitted",
    "order_ack",
    "position_opened",
    "position_partial_closed",
    "position_closed",
    "position_settled",
    "halt",
    "resume",
]


class LifecycleEmitter:
    """Append-only structured logger for order-lifecycle transitions.

    Every emission includes:
      * `event_id`    — UUID for this emission.
      * `event_type`  — one of `LifecycleEventType`.
      * `seq`         — per-process monotonic sequence; lets consumers
                        order events when timestamps collide.
      * `ts_ns`       — injected-clock timestamp (hard rule #5).
      * `correlation_id` — decision_id or position_id; the anchor a
                        consumer uses to join events into a lifecycle.

    Methods are intentionally explicit per event type rather than a
    generic `emit(type, payload)` — the schema-per-event is the whole
    point; a typed method surface makes drift visible at call sites
    under mypy rather than silent key-name typos in a free-form dict.
    """

    __slots__ = ("_clock", "_log", "_seq")

    def __init__(self, *, clock: Clock) -> None:
        self._clock = clock
        self._log = get_logger(LIFECYCLE_LOGGER)
        self._seq = 0

    def _emit(
        self,
        event_type: LifecycleEventType,
        correlation_id: str,
        **payload: Any,
    ) -> None:
        self._seq += 1
        self._log.info(
            "lifecycle",
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            seq=self._seq,
            ts_ns=self._clock.now_ns(),
            correlation_id=correlation_id,
            **payload,
        )

    def decision(
        self,
        *,
        decision_id: str,
        market_id: str,
        trap: str,
        side: str,
        approved: bool,
        contracts: int,
        reject_reason: str | None,
    ) -> None:
        self._emit(
            "decision",
            correlation_id=decision_id,
            market_id=market_id,
            trap=trap,
            side=side,
            approved=approved,
            contracts=contracts,
            reject_reason=reject_reason,
        )

    def order_submitted(
        self,
        *,
        decision_id: str,
        client_order_id: str,
        market_id: str,
        side: str,
        action: str,
        contracts: int,
        limit_price_cents: int,
        order_type: str,
    ) -> None:
        self._emit(
            "order_submitted",
            correlation_id=decision_id,
            client_order_id=client_order_id,
            market_id=market_id,
            side=side,
            action=action,
            contracts=contracts,
            limit_price_cents=limit_price_cents,
            order_type=order_type,
        )

    def order_ack(
        self,
        *,
        decision_id: str,
        client_order_id: str,
        order_id: str,
        status: str,
        filled_contracts: int,
        remaining_contracts: int,
        reason: str | None,
    ) -> None:
        self._emit(
            "order_ack",
            correlation_id=decision_id,
            client_order_id=client_order_id,
            order_id=order_id,
            status=status,
            filled_contracts=filled_contracts,
            remaining_contracts=remaining_contracts,
            reason=reason,
        )

    def position_opened(
        self,
        *,
        position_id: str,
        decision_id: str,
        market_id: str,
        side: str,
        contracts: int,
        entry_price_cents: int,
    ) -> None:
        self._emit(
            "position_opened",
            correlation_id=position_id,
            decision_id=decision_id,
            market_id=market_id,
            side=side,
            contracts=contracts,
            entry_price_cents=entry_price_cents,
        )

    def position_partial_closed(
        self,
        *,
        position_id: str,
        closed_contracts: int,
        remaining_contracts: int,
        exit_price_cents: int,
        partial_seq: int,
    ) -> None:
        self._emit(
            "position_partial_closed",
            correlation_id=position_id,
            closed_contracts=closed_contracts,
            remaining_contracts=remaining_contracts,
            exit_price_cents=exit_price_cents,
            partial_seq=partial_seq,
        )

    def position_closed(
        self,
        *,
        position_id: str,
        exit_price_cents: int,
        net_pnl_usd: float,
        exit_reason: str,
    ) -> None:
        self._emit(
            "position_closed",
            correlation_id=position_id,
            exit_price_cents=exit_price_cents,
            net_pnl_usd=net_pnl_usd,
            exit_reason=exit_reason,
        )

    def halt(self, *, reason: str) -> None:
        self._emit("halt", correlation_id="system", reason=reason)

    def resume(self, *, reason: str) -> None:
        self._emit("resume", correlation_id="system", reason=reason)
