"""Spot price feeds for Coinbase (primary) and Kraken (confirmation).

Architecture is Primary / Confirmation, not primary / secondary failover:
  * Coinbase drives the FeatureEngine — Bollinger, ATR, regime classification.
  * Kraken does NOT feed features. It's consulted only by the integrity gate
    (`signal/integrity.py`) on ENTRY, and only vetoes when its directional
    velocity actively contradicts Coinbase over a ~1s window. See the
    integrity module docstring for the "silence ≠ veto, active disagreement
    vetoes" rationale.

Binance was the original confirmation venue but is geo-blocked from US IPs
(HTTP 451). Kraken V2 WS is the operational replacement.

Wire formats (public docs, as of 2026):
    Coinbase (subscribe channel=ticker, product_id=BTC-USD):
        {"type":"ticker","product_id":"BTC-USD","price":"...","last_size":"...","time":"ISO8601"}
    Kraken V2 (subscribe {"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD"]}}):
        {"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"...","price":N,"qty":N,"ord_type":"...","trade_id":N,"timestamp":"ISO8601"}]}
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any

import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.feeds.kalshi import SessionEndedError, WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import SpotTick, Venue
from bot_btc_1hr_kalshi.obs.clock import Clock
from bot_btc_1hr_kalshi.obs.money import usd_to_micros

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.spot")


class SpotParseError(ValueError):
    """Malformed or unrecognized spot frame."""


def _iso_to_ns(iso: str, *, fallback_ns: int) -> int:
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return fallback_ns
    return int(dt.timestamp() * 1_000_000_000)


def parse_coinbase(raw: bytes | str, *, recv_ts_ns: int) -> SpotTick | None:
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise SpotParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SpotParseError("frame is not an object")
    mtype = data.get("type")
    if mtype in (None, "subscriptions", "heartbeat", "error"):
        return None
    if mtype != "ticker":
        return None
    try:
        price = float(data["price"])
        size = float(data.get("last_size") or 0.0)
    except (KeyError, ValueError, TypeError) as exc:
        raise SpotParseError(f"missing price: {exc}") from exc
    ts_ns = _iso_to_ns(str(data.get("time", "")), fallback_ns=recv_ts_ns)
    return SpotTick(
        ts_ns=ts_ns,
        venue="coinbase",
        price_micros=usd_to_micros(price),
        size=size,
    )


def parse_kraken(raw: bytes | str, *, recv_ts_ns: int) -> SpotTick | None:
    """Parse a Kraken V2 WS trade frame into a SpotTick.

    Kraken emits both the subscribe ack (`{"method":"subscribe","success":true}`)
    and per-symbol snapshots (`type=="snapshot"`). Only `type=="update"` on
    `channel=="trade"` produces ticks downstream; snapshots are Kraken's
    "last trade at subscribe time" which we intentionally discard so the
    velocity tracker doesn't latch onto a stale print.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise SpotParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SpotParseError("frame is not an object")
    if data.get("channel") != "trade":
        return None
    if data.get("type") != "update":
        return None
    items = data.get("data")
    if not isinstance(items, list) or not items:
        return None
    last = items[-1]
    if not isinstance(last, dict):
        raise SpotParseError("trade entry is not an object")
    try:
        price = float(last["price"])
        size = float(last["qty"])
    except (KeyError, ValueError, TypeError) as exc:
        raise SpotParseError(f"missing fields: {exc}") from exc
    ts_ns = _iso_to_ns(str(last.get("timestamp", "")), fallback_ns=recv_ts_ns)
    return SpotTick(
        ts_ns=ts_ns,
        venue="kraken",
        price_micros=usd_to_micros(price),
        size=size,
    )


def build_coinbase_subscribe(product_ids: list[str]) -> bytes:
    return orjson.dumps(
        {"type": "subscribe", "product_ids": product_ids, "channels": ["ticker"]}
    )


def build_kraken_subscribe(symbols: list[str]) -> bytes:
    return orjson.dumps(
        {"method": "subscribe", "params": {"channel": "trade", "symbol": symbols}}
    )


SpotParser = Callable[[bytes | str, int], SpotTick | None]


class SpotFeed:
    """WS adapter for a spot venue. Parser + optional subscribe are injected."""

    def __init__(
        self,
        *,
        name: Venue,
        ws_url: str,
        clock: Clock,
        ws_connect: WSConnect,
        staleness: StalenessTracker,
        parse: Callable[[bytes | str], SpotTick | None],
        subscribe: bytes | None = None,
        backoff_initial_sec: float = 1.0,
        backoff_max_sec: float = 30.0,
        sleep: Callable[[float], Awaitable[Any]] | None = None,
    ) -> None:
        self._name = name
        self._url = ws_url
        self._clock = clock
        self._connect = ws_connect
        self._staleness = staleness
        self._parse = parse
        self._subscribe_frame = subscribe
        self._backoff_initial = backoff_initial_sec
        self._backoff_max = backoff_max_sec
        self._sleep = sleep or _default_sleep

    async def events(self) -> AsyncIterator[SpotTick]:
        backoff = self._backoff_initial
        while True:
            try:
                async for tick in self._session():
                    backoff = self._backoff_initial
                    yield tick
            except SessionEndedError as exc:
                _log.warning(f"feed.{self._name}.reconnect", reason=str(exc), backoff_sec=backoff)
                await self._sleep(backoff)
                backoff = min(self._backoff_max, backoff * 2.0)

    async def _session(self) -> AsyncIterator[SpotTick]:
        try:
            conn = await self._connect(self._url)
        except Exception as exc:
            raise SessionEndedError(f"connect_failed:{exc}") from exc

        try:
            if self._subscribe_frame is not None:
                await conn.send(self._subscribe_frame)
            async for raw in conn:
                self._staleness.mark()
                try:
                    tick = self._parse(raw)
                except SpotParseError as exc:
                    _log.warning(f"feed.{self._name}.parse_error", error=str(exc))
                    continue
                if tick is None:
                    continue
                yield tick
        finally:
            try:
                await conn.close()
            except Exception as exc:
                _log.warning(f"feed.{self._name}.close_error", error=str(exc))
        raise SessionEndedError("ws_closed")


def coinbase_parser(clock: Clock) -> Callable[[bytes | str], SpotTick | None]:
    """Bind the Coinbase parser to the given clock for recv-time fallback."""

    def _p(raw: bytes | str) -> SpotTick | None:
        return parse_coinbase(raw, recv_ts_ns=clock.now_ns())

    return _p


def kraken_parser(clock: Clock) -> Callable[[bytes | str], SpotTick | None]:
    def _p(raw: bytes | str) -> SpotTick | None:
        return parse_kraken(raw, recv_ts_ns=clock.now_ns())

    return _p


async def _default_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)
