"""Typed events emitted by the Kalshi private WS execution stream.

These are the parser's output type — pure data carriers, no behavior. The
stream dispatches each event to subscriber callbacks; consumers (OMS
listener, reconciler diff, telemetry) translate them into Portfolio
mutations or BetOutcome records.

We deliberately do NOT reuse `execution.broker.base.Fill` here. That type
is the broker-protocol return shape (paired with an `OrderRequest` and
includes the request-side `action`/`side`); the WS frames don't carry a
client-side request and may report fills for orders we placed in a
previous process incarnation. Keeping the WS event distinct preserves
the broker DTO as a "this submit returned this fill" contract while
letting the stream report whatever Kalshi observes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from bot_btc_1hr_kalshi.execution.broker.base import OrderAction, OrderStatus
from bot_btc_1hr_kalshi.obs.schemas import Side


@dataclass(frozen=True, slots=True)
class ExecFillEvent:
    """A single fill observed on the private `fill` channel.

    `fees_usd` aggregates whatever fee fields Kalshi reports on the
    frame (maker_fee + taker_fee, or a flat `fee` field), normalized
    into USD. `ts_ns` is the exchange-emitted timestamp; if Kalshi did
    not stamp the frame the parser falls back to the local recv time
    so consumers always see a monotonic-ish clock.
    """

    seq: int | None
    ts_ns: int
    trade_id: str
    order_id: str
    client_order_id: str
    market_id: str
    side: Side
    action: OrderAction
    price_cents: int
    contracts: int
    fees_usd: float
    is_taker: bool


@dataclass(frozen=True, slots=True)
class ExecOrderUpdate:
    """Lifecycle event for an order we placed (resting/filled/cancelled).

    Mirrors the REST `OrderAck.status` enum so the consumer can apply WS
    updates with the same status machine the broker submit path uses.
    `remaining_contracts` is canonical on the frame; `filled_contracts`
    is derived if the wire only carries `count`/`remaining_count`.
    """

    seq: int | None
    ts_ns: int
    order_id: str
    client_order_id: str
    market_id: str
    status: OrderStatus
    side: Side
    filled_contracts: int
    remaining_contracts: int
    limit_price_cents: int


@dataclass(frozen=True, slots=True)
class ExecPositionSnapshot:
    """Current broker-side position for a single market.

    Kalshi's `market_positions` channel emits a full snapshot per market
    on each change (signed YES contracts), not a delta — we normalize to
    `(side, abs_contracts)` matching `BrokerPosition`. `position == 0`
    is meaningful (flat), and the reconciler treats it that way; the
    parser preserves the zero rather than dropping the frame.
    """

    seq: int | None
    ts_ns: int
    market_id: str
    side: Side
    contracts: int
    avg_entry_price_cents: int
    realized_pnl_usd: float
    fees_paid_usd: float


ExecutionEvent = ExecFillEvent | ExecOrderUpdate | ExecPositionSnapshot

ChannelName = Literal["fill", "user_orders", "market_positions"]
