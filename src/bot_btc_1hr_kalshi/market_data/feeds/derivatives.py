"""WS feed harness for derivatives venues (OI / liquidation streams).

Sibling to `feeds/spot.py` — same connect / subscribe / iterate / backoff
loop, but yields venue-specific typed events. Parameterized by the
payload type `T` so one class services both OI-snapshot streams
(`OpenInterestSample`) and liquidation-print streams (`LiquidationEvent`)
without duplicating the reconnect / staleness plumbing.

The parser is injected; this class only owns the connect / subscribe /
backoff lifecycle. Per-venue parse exceptions (subclasses of ValueError)
are logged and skipped rather than tearing down the session — a single
malformed frame is not a protocol break.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Any

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.kalshi import SessionEndedError, WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import Venue

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.derivatives")


class DerivativesParseError(ValueError):
    """Malformed derivatives frame from any venue."""


class DerivativesFeed[T]:
    """WS adapter for a derivatives venue event stream.

    Parameterized over the yielded event type `T` so one class carries
    both `OpenInterestSample` streams (Hyperliquid `metaAndAssetCtxs`,
    Bybit `tickers`) and `LiquidationEvent` streams (Bybit `liquidation`).
    Mirrors `SpotFeed` — same broad-except reconnect path, same staleness
    tracker contract, same backoff curve.
    """

    def __init__(
        self,
        *,
        name: Venue,
        ws_url: str,
        ws_connect: WSConnect,
        staleness: StalenessTracker,
        parse: Callable[[bytes | str], T | None],
        subscribe: bytes | Sequence[bytes] | None = None,
        backoff_initial_sec: float = 1.0,
        backoff_max_sec: float = 30.0,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
    ) -> None:
        self._name = name
        self._url = ws_url
        self._connect = ws_connect
        self._staleness = staleness
        self._parse = parse
        if subscribe is None:
            self._subscribe_frames: tuple[bytes, ...] = ()
        elif isinstance(subscribe, bytes):
            self._subscribe_frames = (subscribe,)
        else:
            self._subscribe_frames = tuple(subscribe)
        self._backoff_initial = backoff_initial_sec
        self._backoff_max = backoff_max_sec
        self._sleep = sleep or _default_sleep

    async def events(self) -> AsyncIterator[T]:
        backoff = self._backoff_initial
        while True:
            try:
                async for event in self._session():
                    backoff = self._backoff_initial
                    yield event
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log.warning(
                    f"feed.{self._name}.reconnect",
                    reason=str(exc),
                    exc_type=type(exc).__name__,
                    backoff_sec=backoff,
                )
                await self._sleep(backoff)
                backoff = min(self._backoff_max, backoff * 2.0)

    async def _session(self) -> AsyncIterator[T]:
        try:
            conn = await self._connect(self._url)
        except Exception as exc:
            raise SessionEndedError(f"connect_failed:{exc}") from exc

        try:
            for frame in self._subscribe_frames:
                await conn.send(frame)
            async for raw in conn:
                self._staleness.mark()
                try:
                    event = self._parse(raw)
                except DerivativesParseError as exc:
                    _log.warning(
                        f"feed.{self._name}.parse_error", error=str(exc)
                    )
                    continue
                except Exception as exc:
                    # Per-venue parse exceptions subclass ValueError. Log +
                    # continue rather than tearing down the session — a
                    # single malformed frame is not a protocol break.
                    _log.warning(
                        f"feed.{self._name}.parse_error",
                        error=str(exc),
                        exc_type=type(exc).__name__,
                    )
                    continue
                if event is None:
                    continue
                yield event
        finally:
            try:
                await conn.close()
            except Exception as exc:
                _log.warning(
                    f"feed.{self._name}.close_error", error=str(exc)
                )
        raise SessionEndedError("ws_closed")


async def _default_sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)
