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

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime
from typing import Any

import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.feeds.kalshi import SessionEndedError, WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import AggressorSide, SpotTick, Venue
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
    # Coinbase's `ticker` channel is maker-centric: `side` is the side of the
    # resting (maker) order that was matched. An uptick (side=="sell") means a
    # resting sell was lifted by a taker BUY; a downtick (side=="buy") means a
    # resting buy was hit by a taker SELL. Invert so the downstream CVD
    # accumulators always receive the taker/aggressor side.
    raw_side = data.get("side")
    if raw_side == "sell":
        aggressor: AggressorSide | None = "buy"
    elif raw_side == "buy":
        aggressor = "sell"
    else:
        aggressor = None
    return SpotTick(
        ts_ns=ts_ns,
        venue="coinbase",
        price_micros=usd_to_micros(price),
        size=size,
        aggressor=aggressor,
    )


def parse_kraken(raw: bytes | str, *, recv_ts_ns: int) -> SpotTick | None:
    """Parse a Kraken V2 WS frame (trade or ticker) into a SpotTick.

    We subscribe to both `trade` and `ticker`. Trades carry taker side and
    size for CVD. Ticker carries `last` + bid/ask on every top-of-book
    change — which on BTC/USD is 1-10 Hz even when actual prints are
    sparse. This matters for the integrity gate (`signal/integrity.py`):
    without ticker, a genuinely quiet BTC/USD trade channel on Kraken
    can silence `record_confirmation` for minutes and trip the gate's
    `stale_halt_sec` fail-closed branch even though the feed is
    connected and correct. Ticker gives us a steady confirmation
    heartbeat. Snapshot frames are still dropped so the velocity
    tracker doesn't latch onto a stale subscribe-time value.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise SpotParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise SpotParseError("frame is not an object")
    if data.get("type") != "update":
        return None
    channel = data.get("channel")
    if channel == "trade":
        return _parse_kraken_trade(data, recv_ts_ns=recv_ts_ns)
    if channel == "ticker":
        return _parse_kraken_ticker(data, recv_ts_ns=recv_ts_ns)
    return None


def _parse_kraken_trade(
    data: dict[str, Any], *, recv_ts_ns: int
) -> SpotTick | None:
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
    # Kraken V2 `trade` channel reports the taker side directly, so no
    # inversion is needed — this mirrors Coinbase's parser inverting for its
    # maker-centric semantics.
    raw_side = last.get("side")
    if raw_side in ("buy", "sell"):
        aggressor: AggressorSide | None = raw_side
    else:
        aggressor = None
    return SpotTick(
        ts_ns=ts_ns,
        venue="kraken",
        price_micros=usd_to_micros(price),
        size=size,
        aggressor=aggressor,
    )


def _parse_kraken_ticker(
    data: dict[str, Any], *, recv_ts_ns: int
) -> SpotTick | None:
    items = data.get("data")
    if not isinstance(items, list) or not items:
        return None
    last = items[-1]
    if not isinstance(last, dict):
        raise SpotParseError("ticker entry is not an object")
    try:
        price = float(last["last"])
    except (KeyError, ValueError, TypeError) as exc:
        raise SpotParseError(f"missing ticker last price: {exc}") from exc
    # Ticker frames are liveness-only for our purposes — no taker side,
    # no trade size. Clock stamps at receive time since Kraken ticker
    # frames don't carry a per-update timestamp.
    return SpotTick(
        ts_ns=recv_ts_ns,
        venue="kraken",
        price_micros=usd_to_micros(price),
        size=0.0,
        aggressor=None,
    )


def build_coinbase_subscribe(product_ids: list[str]) -> bytes:
    return orjson.dumps(
        {"type": "subscribe", "product_ids": product_ids, "channels": ["ticker"]}
    )


def build_kraken_subscribe(symbols: list[str]) -> tuple[bytes, ...]:
    # Two separate WS frames — Kraken V2 subscribes one channel at a
    # time. `trade` carries taker side + size for CVD; `ticker` updates
    # on every top-of-book change and provides the reliable confirmation
    # liveness the integrity gate needs (trades alone can be sparse on
    # quieter BTC pairs, tripping `stale_halt_sec` on a healthy feed).
    return (
        orjson.dumps(
            {
                "method": "subscribe",
                "params": {"channel": "trade", "symbol": symbols},
            }
        ),
        orjson.dumps(
            {
                "method": "subscribe",
                "params": {"channel": "ticker", "symbol": symbols},
            }
        ),
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
        subscribe: bytes | Sequence[bytes] | None = None,
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
        # Normalize to a tuple: one WS send() per frame on session start.
        if subscribe is None:
            self._subscribe_frames: tuple[bytes, ...] = ()
        elif isinstance(subscribe, bytes):
            self._subscribe_frames = (subscribe,)
        else:
            self._subscribe_frames = tuple(subscribe)
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
            for frame in self._subscribe_frames:
                await conn.send(frame)
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
