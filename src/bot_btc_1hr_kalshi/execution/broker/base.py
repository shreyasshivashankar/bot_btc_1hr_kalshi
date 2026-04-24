"""Broker protocol + DTOs. Real and paper brokers both implement `Broker`.

Hard rule #1: maker-only on entry. `OrderType.MAKER` must never cross.
Hard rule #7: broker state is authoritative — OMS reconciles against `list_positions()`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

from bot_btc_1hr_kalshi.market_data.types import TradeEvent
from bot_btc_1hr_kalshi.obs.schemas import Side

OrderType = Literal["maker", "ioc"]
OrderAction = Literal["BUY", "SELL"]
OrderStatus = Literal["resting", "filled", "partially_filled", "rejected", "cancelled"]


@dataclass(frozen=True, slots=True)
class OrderRequest:
    client_order_id: str
    market_id: str
    side: Side
    action: OrderAction
    limit_price_cents: int
    contracts: int
    order_type: OrderType


@dataclass(frozen=True, slots=True)
class Fill:
    order_id: str
    client_order_id: str
    market_id: str
    side: Side
    action: OrderAction
    price_cents: int
    contracts: int
    ts_ns: int
    fees_usd: float


@dataclass(frozen=True, slots=True)
class OrderAck:
    order_id: str
    client_order_id: str
    status: OrderStatus
    filled_contracts: int
    remaining_contracts: int
    fills: tuple[Fill, ...]
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class BrokerPosition:
    """Broker-side view of a position. Used by the reconciliation loop."""

    market_id: str
    side: Side
    contracts: int
    avg_entry_price_cents: int


@runtime_checkable
class Broker(Protocol):
    async def submit(self, req: OrderRequest) -> OrderAck: ...
    async def cancel(self, order_id: str) -> bool: ...
    async def list_open_orders(self) -> tuple[OrderAck, ...]: ...
    async def list_positions(self) -> tuple[BrokerPosition, ...]: ...
    async def match_trade(self, trade: TradeEvent) -> tuple[Fill, ...]:
        """Match a public TradeEvent against any locally-resting orders.

        Used by PaperBroker to simulate maker-order fills. Live brokers
        (KalshiBroker, ShadowBroker) return `()` — fills come back via
        the trading-API order acks / WS order channel / reconciler, not
        synthesized from public-tape trades. The smart-router exit path
        relies on this to drive resting maker exits to fills under paper
        and replay; live mode degrades to reconciler-driven fills until
        the FIX execution-report channel lands."""
        ...
