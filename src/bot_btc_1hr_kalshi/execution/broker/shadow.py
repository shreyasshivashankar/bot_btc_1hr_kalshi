"""Shadow broker: live decision path, no wire.

Hard rule #2 gates every deploy to `live` behind ≥24h of shadow running:
the full decision pipeline (feeds → signal → risk → OMS → broker) runs
against real market data, but every submit() lands here. No HTTP call
is made. No order reaches Kalshi. The broker returns an ack with
status="cancelled" so the OMS's non-fill branch runs and the monitor
never sees a phantom open position.

What is retained: every OrderRequest is logged as a structured
`shadow.submit_intent` event so operators can replay shadow runs to
compute hit-rate / would-be PnL offline. Combined with the BetOutcome
emissions from the paper broker in a parallel run, this lets us compare
shadow decisions to paper fills and measure slippage attribution.

What is NOT retained: no local fill simulation. Shadow is strictly a
"what would I have submitted" recorder. Paper broker (paper.py) is the
"what would I have filled" simulator. The two are orthogonal — run
shadow to validate decision-path behavior against *live* feeds; run
paper to validate OMS + portfolio + monitor under simulated fills.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.execution.broker.base import (
    BrokerPosition,
    OrderAck,
    OrderRequest,
)
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.logging import get_logger

_SHADOW_REJECT_REASON = "shadow_mode:no_wire"


class ShadowBroker:
    """No-op broker that records intent and returns cancelled acks."""

    __slots__ = ("_clock", "_counter", "_log")

    def __init__(self, *, clock: Clock) -> None:
        self._clock = clock
        self._counter = 0
        self._log = get_logger("bot_btc_1hr_kalshi.broker.shadow")

    async def submit(self, req: OrderRequest) -> OrderAck:
        self._counter += 1
        # Intent log is the whole point — this is the record operators
        # replay offline to reconstruct the would-have-been order book of
        # a shadow run. Every field of OrderRequest is emitted so that a
        # downstream parser does not need to cross-reference any other
        # source. Timestamp is the injected clock (hard rule #5).
        self._log.info(
            "shadow.submit_intent",
            intent_seq=self._counter,
            ts_ns=self._clock.now_ns(),
            client_order_id=req.client_order_id,
            market_id=req.market_id,
            side=req.side,
            action=req.action,
            limit_price_cents=req.limit_price_cents,
            contracts=req.contracts,
            order_type=req.order_type,
        )
        # Return a cancelled ack so the OMS "non_fill" branch runs — no
        # portfolio mutation, no monitor bookkeeping, no BetOutcome
        # emission. This mirrors how a real IOC that finds no liquidity
        # cancels with no fills, which is the closest real analog to
        # "pretend the order never existed."
        return OrderAck(
            order_id=f"shadow-{self._counter}",
            client_order_id=req.client_order_id,
            status="cancelled",
            filled_contracts=0,
            remaining_contracts=req.contracts,
            fills=(),
            reason=_SHADOW_REJECT_REASON,
        )

    async def cancel(self, order_id: str) -> bool:
        # No resting orders can exist under shadow mode; every submit
        # returned cancelled. A cancel() call against a shadow order id
        # is therefore idempotent — report success and log, so operator
        # scripts can run unchanged against shadow mode.
        self._log.info("shadow.cancel", order_id=order_id)
        return order_id.startswith("shadow-")

    async def list_open_orders(self) -> tuple[OrderAck, ...]:
        return ()

    async def list_positions(self) -> tuple[BrokerPosition, ...]:
        # Shadow mode has zero positions by construction. Returning ()
        # means the reconciler finds zero mismatches against a paper
        # portfolio that is also zero, and zero mismatches against any
        # stateful portfolio the operator bootstraps. The persistence
        # gate (reconciler) will trip before this masks a real bug —
        # shadow mode without paper fills should never have open
        # positions locally.
        return ()
