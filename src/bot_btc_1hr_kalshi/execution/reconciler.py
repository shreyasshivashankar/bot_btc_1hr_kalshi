"""Periodic broker <-> local state reconciliation (hard rule #7).

The broker is authoritative. Every `interval_sec` we call `list_positions()`
and compare to the local Portfolio. If any market's net contract delta is
greater than `tolerance_contracts`, trading is halted and a structured
log record is emitted for operators.

Semantics:
  - Aggregate local positions by (market_id, side) into expected contract
    counts. The Portfolio does not carry NO-side positions today (we only
    hold the YES leg at entry) — but the reconciler is symmetric in
    anticipation of Slice 3.
  - Aggregate broker positions the same way.
  - Any (market, side) whose |broker - local| > tolerance trips the halt.

On halt, `App.trading_halted = True` so no new entries are submitted; open
positions still monitor-exit. Operators must investigate and manually
resume.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass

import structlog

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.execution.broker.base import Broker, BrokerPosition
from bot_btc_1hr_kalshi.obs.schemas import Side
from bot_btc_1hr_kalshi.portfolio.positions import OpenPosition

_log = structlog.get_logger("bot_btc_1hr_kalshi.reconciler")


@dataclass(frozen=True, slots=True)
class ReconcileMismatch:
    market_id: str
    side: Side
    local_contracts: int
    broker_contracts: int

    @property
    def delta(self) -> int:
        return self.broker_contracts - self.local_contracts


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    mismatches: tuple[ReconcileMismatch, ...]
    halted: bool


class Reconciler:
    """Compares local Portfolio to broker positions. Halts App on divergence."""

    def __init__(
        self,
        *,
        app: App,
        broker: Broker,
        interval_sec: int,
        tolerance_contracts: int = 1,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        if tolerance_contracts < 0:
            raise ValueError("tolerance_contracts must be >= 0")
        self._app = app
        self._broker = broker
        self._interval = interval_sec
        self._tolerance = tolerance_contracts

    async def check_once(self) -> ReconcileResult:
        try:
            broker_positions = await self._broker.list_positions()
        except Exception as exc:
            _log.warning("reconciler.broker_call_failed", error=str(exc))
            return ReconcileResult(mismatches=(), halted=False)

        local = _aggregate_local(self._app.portfolio.open_positions)
        broker = _aggregate_broker(broker_positions)

        keys = set(local) | set(broker)
        mismatches: list[ReconcileMismatch] = []
        for k in keys:
            l_ct = local.get(k, 0)
            b_ct = broker.get(k, 0)
            if abs(b_ct - l_ct) > self._tolerance:
                mismatches.append(ReconcileMismatch(
                    market_id=k[0], side=k[1],
                    local_contracts=l_ct, broker_contracts=b_ct,
                ))

        if mismatches:
            _log.error(
                "reconciler.mismatch_halt",
                mismatches=[
                    {"market": m.market_id, "side": m.side,
                     "local": m.local_contracts, "broker": m.broker_contracts}
                    for m in mismatches
                ],
            )
            self._app.halt()
            return ReconcileResult(mismatches=tuple(mismatches), halted=True)

        return ReconcileResult(mismatches=(), halted=False)

    async def run(self) -> None:
        """Main loop — kept simple. Cancel to stop."""
        while True:
            await self.check_once()
            await asyncio.sleep(self._interval)


def _aggregate_local(positions: tuple[OpenPosition, ...]) -> dict[tuple[str, Side], int]:
    out: dict[tuple[str, Side], int] = defaultdict(int)
    for p in positions:
        out[(p.market_id, p.side)] += p.contracts
    return dict(out)


def _aggregate_broker(
    positions: tuple[BrokerPosition, ...],
) -> dict[tuple[str, Side], int]:
    out: dict[tuple[str, Side], int] = defaultdict(int)
    for p in positions:
        out[(p.market_id, p.side)] += p.contracts
    return dict(out)
