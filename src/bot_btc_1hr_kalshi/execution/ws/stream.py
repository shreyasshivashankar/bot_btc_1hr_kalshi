"""Kalshi private WS execution stream — interim FIX substitute (#148).

Connects to the Kalshi trading-API WS using the signed handshake
(`ws_connect_kalshi_signed`), subscribes to the three private execution
channels, and dispatches parsed frames to subscriber callbacks. This is
the additive observation layer; the OMS pivot to fire-and-track lands
in a follow-up once we have soaked the stream against real frames.

Design notes:

  * Subscribers register typed callbacks (one list per event variant)
    rather than receiving the union — keeps consumers from hand-rolling
    isinstance ladders. A single bad callback never breaks dispatch:
    we log and continue (mirrors `DerivativesOracle` posture).

  * The stream is a non-critical supervisor target. A dropped WS or a
    parse error must NOT halt the trading graph — the REST POST-body
    fill path is still authoritative until the OMS pivot ships. We
    follow the `KalshiFeed` reconnect-with-backoff pattern so the
    failure mode is "no execution observations for a few seconds"
    rather than "trading halts."

  * Staleness: optional `StalenessTracker`. The trading-API WS is
    quiet by design when no orders are flowing (paper mode early in
    the session, low-volume hours), so we mark the tracker on every
    received frame including control frames — staleness here
    measures connectivity, not order activity.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from bot_btc_1hr_kalshi.execution.ws.parser import (
    KalshiExecParseError,
    build_exec_subscribe,
    parse_exec_frame,
)
from bot_btc_1hr_kalshi.execution.ws.types import (
    ExecFillEvent,
    ExecOrderUpdate,
    ExecPositionSnapshot,
)
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import (
    SessionEndedError,
    WSConnect,
    WSConnection,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.exec.ws")

EXEC_CHANNELS: tuple[str, ...] = ("fill", "user_orders", "market_positions")


_FillCb = Callable[[ExecFillEvent], None]
_OrderCb = Callable[[ExecOrderUpdate], None]
_PositionCb = Callable[[ExecPositionSnapshot], None]


class KalshiExecutionStream:
    def __init__(
        self,
        *,
        ws_url: str,
        clock: Clock,
        ws_connect: WSConnect,
        market_tickers: list[str] | None = None,
        channels: tuple[str, ...] = EXEC_CHANNELS,
        staleness: StalenessTracker | None = None,
        backoff_initial_sec: float = 1.0,
        backoff_max_sec: float = 30.0,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
    ) -> None:
        if not channels:
            raise ValueError("channels must be non-empty")
        self._url = ws_url
        self._clock = clock
        self._connect = ws_connect
        self._market_tickers = list(market_tickers) if market_tickers else None
        self._channels = tuple(channels)
        self._staleness = staleness
        self._backoff_initial = backoff_initial_sec
        self._backoff_max = backoff_max_sec
        self._sleep = sleep or _default_sleep
        self._req_id = 0
        self._fill_cbs: list[_FillCb] = []
        self._order_cbs: list[_OrderCb] = []
        self._position_cbs: list[_PositionCb] = []
        self._active_conn: WSConnection | None = None

    def subscribe_fill(self, cb: _FillCb) -> None:
        self._fill_cbs.append(cb)

    def subscribe_order(self, cb: _OrderCb) -> None:
        self._order_cbs.append(cb)

    def subscribe_position(self, cb: _PositionCb) -> None:
        self._position_cbs.append(cb)

    async def run(self) -> None:
        """Connect/subscribe/dispatch loop. Never returns under normal use.

        Reconnects on WS close with exponential backoff. Exits cleanly on
        asyncio.CancelledError (shutdown path). All other exceptions are
        logged and the loop continues — this is a non-critical supervisor
        target.

        Backoff resets on any session that successfully dispatched at
        least one parsed event. A WS that cycles due to a load balancer
        but keeps delivering frames stays at the floor; only a stretch
        of dead/broken sessions ramps the wait time.
        """
        backoff = self._backoff_initial
        while True:
            dispatched = 0
            try:
                dispatched = await self._session()
            except SessionEndedError as exc:
                _log.warning("exec.ws.reconnect", reason=str(exc), backoff_sec=backoff)
            except Exception as exc:  # pragma: no cover — defensive
                _log.error("exec.ws.session_error", error=str(exc), backoff_sec=backoff)
            if dispatched > 0:
                backoff = self._backoff_initial
            await self._sleep(backoff)
            if dispatched == 0:
                backoff = min(self._backoff_max, backoff * 2.0)

    async def _session(self) -> int:
        """Run one connect → subscribe → dispatch session.

        Returns the number of parsed events dispatched (used by `run()` to
        decide whether to reset the reconnect backoff). Raises
        `SessionEndedError` only on connect-time failures; a clean WS
        close after the iterator exhausts returns normally with the
        dispatch count so `run()` can collapse "WS cycled but worked"
        into the no-backoff path.
        """
        try:
            conn = await self._connect(self._url)
        except Exception as exc:
            raise SessionEndedError(f"connect_failed:{exc}") from exc

        dispatched = 0
        self._active_conn = conn
        try:
            self._req_id += 1
            await conn.send(
                build_exec_subscribe(
                    req_id=self._req_id,
                    channels=self._channels,
                    market_tickers=self._market_tickers,
                )
            )
            async for raw in conn:
                recv_ns = self._clock.now_ns()
                if self._staleness is not None:
                    # Mark on every frame including control frames — quiet
                    # private channels are normal in paper, so connectivity
                    # is what we actually want to track here.
                    self._staleness.mark_at(recv_ns)
                try:
                    ev = parse_exec_frame(raw, recv_ts_ns=recv_ns)
                except KalshiExecParseError as exc:
                    _log.warning("exec.ws.parse_error", error=str(exc))
                    continue
                if ev is None:
                    continue
                self._dispatch(ev)
                dispatched += 1
        finally:
            self._active_conn = None
            try:
                await conn.close()
            except Exception as exc:
                _log.warning("exec.ws.close_error", error=str(exc))
        return dispatched

    def _dispatch(self, ev: ExecFillEvent | ExecOrderUpdate | ExecPositionSnapshot) -> None:
        if isinstance(ev, ExecFillEvent):
            self._dispatch_to(self._fill_cbs, ev, kind="fill")
        elif isinstance(ev, ExecOrderUpdate):
            self._dispatch_to(self._order_cbs, ev, kind="order")
        else:
            self._dispatch_to(self._position_cbs, ev, kind="position")

    @staticmethod
    def _dispatch_to(
        cbs: list[Callable[[Any], None]], ev: Any, *, kind: str
    ) -> None:
        for cb in cbs:
            try:
                cb(ev)
            except Exception as exc:  # pragma: no cover — logged and skipped
                _log.warning("exec.ws.callback_error", event_kind=kind, error=str(exc))


async def _default_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
