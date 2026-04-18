"""Tests for ShadowBroker (hard rule #2 gate).

Shadow mode is the ≥24h validation step that sits between paper and live.
These tests assert the broker:
  * Implements the Broker protocol (so the OMS accepts it).
  * Never returns a filled ack — shadow must not mutate portfolio state.
  * Increments its intent counter so operator replay can re-order events.
  * Has idempotent list_* and cancel behaviors.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.execution.broker.base import Broker, OrderRequest
from bot_btc_1hr_kalshi.execution.broker.shadow import ShadowBroker
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _req(cid: str = "c1", contracts: int = 5) -> OrderRequest:
    return OrderRequest(
        client_order_id=cid,
        market_id="KXBTCD-26APR17-60000",
        side="YES",
        action="BUY",
        limit_price_cents=40,
        contracts=contracts,
        order_type="maker",
    )


def test_shadow_broker_implements_protocol() -> None:
    clock = ManualClock(0)
    broker = ShadowBroker(clock=clock)
    assert isinstance(broker, Broker)


async def test_submit_returns_cancelled_ack_and_no_fills() -> None:
    clock = ManualClock(1_700_000_000_000_000_000)
    broker = ShadowBroker(clock=clock)
    ack = await broker.submit(_req())
    assert ack.status == "cancelled"
    assert ack.filled_contracts == 0
    assert ack.remaining_contracts == 5
    assert ack.fills == ()
    assert ack.reason == "shadow_mode:no_wire"
    assert ack.order_id.startswith("shadow-")
    assert ack.client_order_id == "c1"


async def test_intent_counter_is_monotonic() -> None:
    clock = ManualClock(0)
    broker = ShadowBroker(clock=clock)
    a = await broker.submit(_req("c1"))
    b = await broker.submit(_req("c2"))
    c = await broker.submit(_req("c3"))
    # Order ids encode the sequence number so a replay tool can sort.
    assert a.order_id == "shadow-1"
    assert b.order_id == "shadow-2"
    assert c.order_id == "shadow-3"


async def test_list_positions_and_orders_are_empty() -> None:
    broker = ShadowBroker(clock=ManualClock(0))
    assert await broker.list_positions() == ()
    assert await broker.list_open_orders() == ()


async def test_cancel_is_idempotent_for_shadow_ids() -> None:
    broker = ShadowBroker(clock=ManualClock(0))
    ack = await broker.submit(_req())
    # Cancelling a shadow id reports success; cancelling anything else
    # reports False so paper/live tools calling into shadow mode get a
    # sensible signal.
    assert await broker.cancel(ack.order_id) is True
    assert await broker.cancel("not-a-shadow-order") is False
