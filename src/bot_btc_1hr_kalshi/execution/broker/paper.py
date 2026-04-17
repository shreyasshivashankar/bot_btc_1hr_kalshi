"""PaperBroker — deterministic simulated fills against a local L2 book.

Used in `paper` and `shadow` modes and by the replay backtester. Behavior:

- `ioc`: walks the opposite side of the book, filling up to `contracts` at each level
  whose price satisfies the limit (BUY: ask <= limit; SELL: bid >= limit). Any residual
  is cancelled (IOC semantics).
- `maker`: if the limit crosses the book it is rejected (would be an aggressive order —
  violates hard rule #1 on entry, and our ladder uses IOC for aggressive exits). Else
  the order rests. Resting orders are matched when `match_trade(trade)` is called with
  a TradeEvent whose price touches our resting level on the aggressive side.

Fees are charged at `fee_per_contract_usd` per fill contract.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from bot_btc_1hr_kalshi.execution.broker.base import (
    BrokerPosition,
    Fill,
    OrderAck,
    OrderRequest,
    OrderStatus,
)
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import TradeEvent
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.schemas import Side


@dataclass(slots=True)
class _Resting:
    order_id: str
    req: OrderRequest
    remaining: int
    fills: list[Fill]


class PaperBroker:
    """In-memory broker. Thread-safe under asyncio via a single lock."""

    def __init__(
        self,
        *,
        clock: Clock,
        fee_per_contract_usd: float = 0.0,
    ) -> None:
        if fee_per_contract_usd < 0:
            raise ValueError("fee_per_contract_usd must be non-negative")
        self._clock = clock
        self._fee = fee_per_contract_usd
        self._books: dict[str, L2Book] = {}
        self._resting: dict[str, _Resting] = {}
        self._order_counter = 0
        self._lock = asyncio.Lock()
        # Position ledger: (market_id, side) -> (signed_contracts, cum_cost_cents).
        # A BUY adds +contracts and +contracts*price; a SELL decrements both.
        self._position_ledger: dict[tuple[str, Side], tuple[int, int]] = {}

    def register_book(self, book: L2Book) -> None:
        """OMS calls this once per market so the broker can simulate fills."""
        self._books[book.market_id] = book

    def _next_order_id(self) -> str:
        self._order_counter += 1
        return f"paper-{self._order_counter}"

    async def submit(self, req: OrderRequest) -> OrderAck:
        async with self._lock:
            return self._submit_locked(req)

    def _submit_locked(self, req: OrderRequest) -> OrderAck:
        if req.contracts <= 0:
            return _reject(req, "zero_contracts")
        if not (0 <= req.limit_price_cents <= 100):
            return _reject(req, "invalid_limit_price")

        book = self._books.get(req.market_id)
        if book is None:
            return _reject(req, "unknown_market")
        if not book.valid:
            return _reject(req, f"book_invalid:{book.invalidation_reason}")

        order_id = self._next_order_id()

        if req.order_type == "ioc":
            fills, remaining = self._fill_ioc(order_id, req, book)
            for f in fills:
                self._apply_to_ledger(f)
            status: OrderStatus = (
                "filled" if remaining == 0 else "partially_filled" if fills else "cancelled"
            )
            return OrderAck(
                order_id=order_id,
                client_order_id=req.client_order_id,
                status=status,
                filled_contracts=req.contracts - remaining,
                remaining_contracts=remaining,
                fills=tuple(fills),
            )

        # Maker: refuse if it would cross
        best_bid = book.best_bid
        best_ask = book.best_ask
        if req.action == "BUY" and best_ask is not None and req.limit_price_cents >= best_ask.price_cents:
            return _reject(req, "maker_would_cross")
        if req.action == "SELL" and best_bid is not None and req.limit_price_cents <= best_bid.price_cents:
            return _reject(req, "maker_would_cross")

        self._resting[order_id] = _Resting(order_id=order_id, req=req, remaining=req.contracts, fills=[])
        return OrderAck(
            order_id=order_id,
            client_order_id=req.client_order_id,
            status="resting",
            filled_contracts=0,
            remaining_contracts=req.contracts,
            fills=(),
        )

    def _fill_ioc(
        self, order_id: str, req: OrderRequest, book: L2Book
    ) -> tuple[list[Fill], int]:
        bids, asks = book.snapshot_levels()
        levels = asks if req.action == "BUY" else bids
        remaining = req.contracts
        fills: list[Fill] = []
        ts = self._clock.now_ns()
        for lvl in levels:
            if remaining == 0:
                break
            price_ok = (
                lvl.price_cents <= req.limit_price_cents
                if req.action == "BUY"
                else lvl.price_cents >= req.limit_price_cents
            )
            if not price_ok:
                break
            take = min(remaining, lvl.size)
            if take <= 0:
                continue
            fills.append(
                Fill(
                    order_id=order_id,
                    client_order_id=req.client_order_id,
                    market_id=req.market_id,
                    side=req.side,
                    action=req.action,
                    price_cents=lvl.price_cents,
                    contracts=take,
                    ts_ns=ts,
                    fees_usd=self._fee * take,
                )
            )
            remaining -= take
        return fills, remaining

    async def cancel(self, order_id: str) -> bool:
        async with self._lock:
            return self._resting.pop(order_id, None) is not None

    async def list_open_orders(self) -> tuple[OrderAck, ...]:
        async with self._lock:
            return tuple(
                OrderAck(
                    order_id=r.order_id,
                    client_order_id=r.req.client_order_id,
                    status="resting",
                    filled_contracts=r.req.contracts - r.remaining,
                    remaining_contracts=r.remaining,
                    fills=tuple(r.fills),
                )
                for r in self._resting.values()
            )

    async def list_positions(self) -> tuple[BrokerPosition, ...]:
        async with self._lock:
            out: list[BrokerPosition] = []
            for (market, side), (contracts, cum_cents) in self._position_ledger.items():
                if contracts == 0:
                    continue
                avg = cum_cents // contracts if contracts > 0 else 0
                out.append(BrokerPosition(
                    market_id=market, side=side, contracts=contracts, avg_entry_price_cents=avg,
                ))
            return tuple(out)

    def _apply_to_ledger(self, fill: Fill) -> None:
        key = (fill.market_id, fill.side)
        contracts, cum = self._position_ledger.get(key, (0, 0))
        if fill.action == "BUY":
            contracts += fill.contracts
            cum += fill.contracts * fill.price_cents
        else:  # SELL closes some or all of the position
            contracts -= fill.contracts
            cum -= fill.contracts * fill.price_cents
        if contracts <= 0:
            contracts, cum = 0, 0
        self._position_ledger[key] = (contracts, cum)

    async def match_trade(self, trade: TradeEvent) -> tuple[Fill, ...]:
        """Match an incoming TradeEvent against resting orders in its market.

        A resting BUY at price P fills when a sell aggressor trades the same side at <= P.
        A resting SELL at price P fills when a buy aggressor trades the same side at >= P.
        """
        async with self._lock:
            filled_now: list[Fill] = []
            completed: list[str] = []
            for oid, r in self._resting.items():
                if r.req.market_id != trade.market_id:
                    continue
                if trade.taker_side != r.req.side:
                    continue
                hits = (
                    r.req.action == "BUY"
                    and trade.aggressor == "sell"
                    and trade.price_cents <= r.req.limit_price_cents
                ) or (
                    r.req.action == "SELL"
                    and trade.aggressor == "buy"
                    and trade.price_cents >= r.req.limit_price_cents
                )
                if not hits:
                    continue
                take = min(r.remaining, trade.size)
                if take <= 0:
                    continue
                fill = Fill(
                    order_id=oid,
                    client_order_id=r.req.client_order_id,
                    market_id=r.req.market_id,
                    side=r.req.side,
                    action=r.req.action,
                    price_cents=r.req.limit_price_cents,
                    contracts=take,
                    ts_ns=trade.ts_ns,
                    fees_usd=self._fee * take,
                )
                r.fills.append(fill)
                r.remaining -= take
                filled_now.append(fill)
                self._apply_to_ledger(fill)
                if r.remaining == 0:
                    completed.append(oid)
            for oid in completed:
                del self._resting[oid]
            return tuple(filled_now)


def _reject(req: OrderRequest, reason: str) -> OrderAck:
    return OrderAck(
        order_id="",
        client_order_id=req.client_order_id,
        status="rejected",
        filled_contracts=0,
        remaining_contracts=req.contracts,
        fills=(),
        reason=reason,
    )
