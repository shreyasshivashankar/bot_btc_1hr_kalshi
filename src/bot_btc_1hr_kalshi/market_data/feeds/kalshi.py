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

import os
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol

import structlog

from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import (
    KalshiParseError,
    build_subscribe,
    parse_frame,
    peek_frame_type,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import FeedEvent
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.kalshi")
_diag_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.kalshi.diag")


class _FeedDiagnostic:
    """Periodic per-ftype stats snapshot for the Kalshi WS session.

    Enabled via BOT_BTC_1HR_KALSHI_FEED_DIAG=1. When disabled the hot path
    does a single attribute check (`self.enabled`) per frame and nothing else.

    Emits at most one `feed.kalshi.diag.window` record per `interval_sec`
    with:
      * per-ftype counts in the window (including dropped control frames)
      * per-ftype inter-arrival gap stats in local-clock ms
      * exchange-to-recv lag stats in ms (how far behind Kalshi's stamp is
        relative to our receive time — high lag explains staleness.halt
        under a tape that's active by UI but delayed on the wire)
    """

    __slots__ = (
        "_clock",
        "_counts",
        "_enabled",
        "_inter_arrivals",
        "_interval_ns",
        "_last_emit_ns",
        "_last_recv_by_type",
        "_ts_lags_ms",
    )

    def __init__(self, *, enabled: bool, clock: Clock, interval_sec: float = 30.0) -> None:
        self._enabled = enabled
        self._clock = clock
        self._interval_ns = int(interval_sec * 1_000_000_000)
        self._last_emit_ns = clock.now_ns() if enabled else 0
        self._counts: dict[str, int] = {}
        self._last_recv_by_type: dict[str, int] = {}
        self._inter_arrivals: dict[str, list[float]] = {}
        self._ts_lags_ms: list[float] = []

    @property
    def enabled(self) -> bool:
        return self._enabled

    def observe(self, ftype: str, recv_ns: int, ev_ts_ns: int | None) -> None:
        self._counts[ftype] = self._counts.get(ftype, 0) + 1
        prev = self._last_recv_by_type.get(ftype)
        if prev is not None:
            self._inter_arrivals.setdefault(ftype, []).append((recv_ns - prev) / 1_000_000)
        self._last_recv_by_type[ftype] = recv_ns
        if ev_ts_ns is not None:
            self._ts_lags_ms.append((recv_ns - ev_ts_ns) / 1_000_000)

    def maybe_emit(self, now_ns: int) -> None:
        if now_ns - self._last_emit_ns < self._interval_ns:
            return
        if not self._counts:
            self._last_emit_ns = now_ns
            return
        window_sec = round((now_ns - self._last_emit_ns) / 1_000_000_000, 1)
        _diag_log.info(
            "feed.kalshi.diag.window",
            window_sec=window_sec,
            counts=dict(self._counts),
            inter_arrival_ms=self._summarize_by_type(self._inter_arrivals),
            exchange_to_recv_lag_ms=self._summarize(self._ts_lags_ms),
        )
        self._counts = {}
        self._inter_arrivals = {}
        self._ts_lags_ms = []
        self._last_emit_ns = now_ns

    @staticmethod
    def _summarize(samples: list[float]) -> dict[str, float] | None:
        if not samples:
            return None
        samples.sort()
        n = len(samples)
        return {
            "count": n,
            "min_ms": round(samples[0], 1),
            "p50_ms": round(samples[n // 2], 1),
            "p99_ms": round(samples[min(n - 1, int(n * 0.99))], 1),
            "max_ms": round(samples[-1], 1),
        }

    @classmethod
    def _summarize_by_type(
        cls, by_type: dict[str, list[float]]
    ) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for t, v in by_type.items():
            s = cls._summarize(v)
            if s is not None:
                out[t] = s
        return out


def _diag_enabled() -> bool:
    return os.environ.get("BOT_BTC_1HR_KALSHI_FEED_DIAG", "").strip() in ("1", "true", "yes")


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

        diag = _FeedDiagnostic(enabled=_diag_enabled(), clock=self._clock)
        try:
            self._req_id += 1
            await conn.send(build_subscribe(req_id=self._req_id, market_tickers=self._tickers))
            async for raw in conn:
                recv_ns = self._clock.now_ns()
                if diag.enabled:
                    ftype = peek_frame_type(raw)
                try:
                    ev = parse_frame(raw, recv_ts_ns=recv_ns)
                except KalshiParseError as exc:
                    _log.warning("feed.kalshi.parse_error", error=str(exc))
                    if diag.enabled:
                        diag.observe("<parse-error>", recv_ns, None)
                        diag.maybe_emit(recv_ns)
                    continue
                if diag.enabled:
                    diag.observe(ftype, recv_ns, ev.ts_ns if ev is not None else None)
                    diag.maybe_emit(recv_ns)
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
