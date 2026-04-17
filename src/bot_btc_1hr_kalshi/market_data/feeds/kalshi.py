"""Kalshi WS feed adapter.

Connects to Kalshi's market-data WS, subscribes to book + trade channels,
parses frames via `kalshi_parser`, and yields `FeedEvent`s downstream.

Robustness behaviors:
  * Reconnect on WS close with exponential backoff; resets the book-valid
    flag (hard rule #9 — features INVALID on gap until REST snapshot).
  * Staleness tracking per `StalenessTracker`; the App watchdog reads this.
  * On parse error, log and skip; do not crash the connection.

The transport is injected (`ws_connect`) so tests can swap in an in-process
asyncio queue without standing up a real WS server.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import (
    KalshiParseError,
    build_subscribe,
    parse_frame,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import FeedEvent
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.kalshi")


class WSConnection(Protocol):
    async def send(self, data: bytes | str) -> None: ...
    def __aiter__(self) -> AsyncIterator[bytes | str]: ...
    async def close(self) -> None: ...


WSConnect = Callable[[str], Awaitable[WSConnection]]


class KalshiFeed:
    def __init__(
        self,
        *,
        ws_url: str,
        market_tickers: list[str],
        clock: Clock,
        ws_connect: WSConnect,
        staleness: StalenessTracker,
        backoff_initial_sec: float = 1.0,
        backoff_max_sec: float = 30.0,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
        on_reconnect: Callable[[str], None] | None = None,
    ) -> None:
        if not market_tickers:
            raise ValueError("market_tickers must be non-empty")
        self._url = ws_url
        self._tickers = list(market_tickers)
        self._clock = clock
        self._connect = ws_connect
        self._staleness = staleness
        self._backoff_initial = backoff_initial_sec
        self._backoff_max = backoff_max_sec
        self._sleep = sleep or _default_sleep
        self._on_reconnect = on_reconnect
        self._req_id = 0

    async def events(self) -> AsyncIterator[FeedEvent]:
        backoff = self._backoff_initial
        while True:
            try:
                async for ev in self._session():
                    backoff = self._backoff_initial
                    yield ev
            except SessionEndedError as exc:
                _log.warning("feed.kalshi.reconnect", reason=str(exc), backoff_sec=backoff)
                # Hard rule #9: book-derived features must be INVALID after a
                # WS cycle until a fresh snapshot rebuilds the book. Notify
                # before sleeping so the gate flips in the same event loop
                # tick as the connection loss.
                if self._on_reconnect is not None:
                    try:
                        self._on_reconnect(str(exc))
                    except Exception as cb_exc:
                        _log.warning("feed.kalshi.on_reconnect_error", error=str(cb_exc))
                await self._sleep(backoff)
                backoff = min(self._backoff_max, backoff * 2.0)

    async def _session(self) -> AsyncIterator[FeedEvent]:
        try:
            conn = await self._connect(self._url)
        except Exception as exc:
            raise SessionEndedError(f"connect_failed:{exc}") from exc

        try:
            self._req_id += 1
            await conn.send(build_subscribe(req_id=self._req_id, market_tickers=self._tickers))
            async for raw in conn:
                try:
                    ev = parse_frame(raw, recv_ts_ns=self._clock.now_ns())
                except KalshiParseError as exc:
                    _log.warning("feed.kalshi.parse_error", error=str(exc))
                    continue
                if ev is None:
                    continue
                # Staleness is measured against the exchange-emitted event
                # timestamp, not our receive time. A backlogged WS queue
                # masquerading as "fresh" recv prints was the whole reason
                # hard rule #4 exists (market-data staleness > 2s → halt).
                self._staleness.mark_at(ev.ts_ns)
                yield ev
        finally:
            try:
                await conn.close()
            except Exception as exc:
                _log.warning("feed.kalshi.close_error", error=str(exc))
        raise SessionEndedError("ws_closed")


class SessionEndedError(RuntimeError):
    """Internal signal that a WS session ended (for reconnect logic)."""


async def _default_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
