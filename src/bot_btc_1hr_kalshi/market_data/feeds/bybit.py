"""Bybit V5 public WebSocket feed — `tickers` for OI, `liquidation` for prints.

Two topics, one venue; we wire them as separate `DerivativesFeed[T]`
instances (distinct WS connections) so the generic feed harness can
stay single-yield-type. Bybit's public WS connection limits are
generous enough that the extra connection is free; the alternative —
one WS with a union-type yield — would fold topic-dispatch into the
parser and make the types cloudy for no real win.

Wire format (V5 public linear, documented at bybit-exchange.github.io):

    Client -> server (one subscribe frame per WS connection):
        {"op":"subscribe","args":["tickers.BTCUSDT"]}
        {"op":"subscribe","args":["liquidation.BTCUSDT"]}

    Server -> client (subscription ack — ignored):
        {"success":true,"ret_msg":"subscribe","conn_id":"...","op":"subscribe"}

    Server -> client (tickers, snapshot → delta cadence):
        {
          "topic":"tickers.BTCUSDT",
          "type":"snapshot",
          "ts":1672304484978,
          "data":{
            "symbol":"BTCUSDT",
            "openInterest":"123456.789",       // BTC-denominated
            "openInterestValue":"8642106432.5", // USD-denominated (linear)
            "lastPrice":"69900.0",
            ...
          }
        }

    Server -> client (liquidation, discrete events):
        {
          "topic":"liquidation.BTCUSDT",
          "type":"snapshot",
          "ts":1672304486868,
          "data":{
            "updatedTime":1672304486865,
            "symbol":"BTCUSDT",
            "side":"Sell",    // aggressor side of the closing fill
            "size":"0.003",   // BTC quantity
            "price":"20294.5"
          }
        }

    * Bybit reports `side` as the *aggressor* of the fill that closed the
      position. A `Sell` aggressor means the closing fill sold into the
      bid — which means the liquidated position was a *long*. We invert
      at parse time so downstream traps read "longs got wiped" / "shorts
      got wiped" rather than aggressor direction.

    * `openInterest` is BTC-denominated on linear perps. We prefer
      `openInterestValue` (USD, already computed by Bybit) when present
      and fall back to `openInterest * lastPrice` otherwise.

All numeric fields are JSON strings. The parser converts at the boundary.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal, cast

import orjson
import structlog

from bot_btc_1hr_kalshi.market_data.types import LiquidationEvent, OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import Clock

_log = structlog.get_logger("bot_btc_1hr_kalshi.feed.bybit")

BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
BYBIT_SOURCE = "bybit"


class BybitParseError(ValueError):
    """Frame did not match the documented V5 public-WS contract."""


def build_bybit_subscribe(topic: str) -> bytes:
    """Build a single-topic V5 public subscribe frame."""
    return orjson.dumps({"op": "subscribe", "args": [topic]})


def bybit_tickers_topic(symbol: str) -> str:
    return f"tickers.{symbol}"


def bybit_liquidation_topic(symbol: str) -> str:
    return f"liquidation.{symbol}"


def parse_bybit_tickers(
    raw: bytes | str,
    *,
    symbol: str,
    recv_ts_ns: int,
) -> OpenInterestSample | None:
    """Parse a `tickers.<symbol>` frame into an `OpenInterestSample`.

    Returns None for non-data frames (subscribe acks, heartbeats, other
    topics). Raises `BybitParseError` on malformed data payloads — wire
    drift the caller should log loudly.
    """
    frame = _loads(raw)
    if frame is None:
        return None
    topic = frame.get("topic")
    if topic != bybit_tickers_topic(symbol):
        return None
    data = frame.get("data")
    if not isinstance(data, dict):
        raise BybitParseError(f"tickers.data not an object: {type(data).__name__}")
    if data.get("symbol") != symbol:
        # Wrong symbol on the tickers feed — venue bug; don't raise.
        return None

    # Prefer pre-computed USD value; fall back to (contracts * lastPrice).
    oi_value = data.get("openInterestValue")
    total_oi_usd: float
    if oi_value is not None:
        try:
            total_oi_usd = float(oi_value)
        except (ValueError, TypeError) as exc:
            raise BybitParseError(f"invalid openInterestValue: {exc}") from exc
    else:
        try:
            oi_base = float(data["openInterest"])
            last_px = float(data["lastPrice"])
        except (KeyError, ValueError, TypeError) as exc:
            raise BybitParseError(
                f"missing openInterest/lastPrice: {exc}"
            ) from exc
        total_oi_usd = oi_base * last_px

    return OpenInterestSample(
        ts_ns=recv_ts_ns,
        symbol=_base_asset(symbol),
        total_oi_usd=total_oi_usd,
        exchanges_count=1,
        source=BYBIT_SOURCE,
    )


def parse_bybit_liquidation(
    raw: bytes | str,
    *,
    symbol: str,
    recv_ts_ns: int,
) -> LiquidationEvent | None:
    """Parse a `liquidation.<symbol>` frame into a `LiquidationEvent`.

    Bybit batches multiple liquidations into a single frame on busy venues.
    We only expose the *first* event per frame here; if batching becomes
    material we can evolve the parser to return `list[LiquidationEvent]`
    without a schema change on the downstream deque.

    The `side` field Bybit sends is the aggressor of the closing fill;
    we invert so the event records the *liquidated position's* direction
    (what traps actually reason about).
    """
    frame = _loads(raw)
    if frame is None:
        return None
    topic = frame.get("topic")
    if topic != bybit_liquidation_topic(symbol):
        return None
    data = frame.get("data")
    if not isinstance(data, dict):
        raise BybitParseError(
            f"liquidation.data not an object: {type(data).__name__}"
        )
    if data.get("symbol") != symbol:
        return None
    try:
        aggressor = data["side"]
        size_base = float(data["size"])
        price = float(data["price"])
    except (KeyError, ValueError, TypeError) as exc:
        raise BybitParseError(f"missing side/size/price: {exc}") from exc

    liquidated_side = _invert_aggressor(aggressor)
    if liquidated_side is None:
        raise BybitParseError(f"unexpected side value: {aggressor!r}")

    return LiquidationEvent(
        ts_ns=recv_ts_ns,
        symbol=_base_asset(symbol),
        side=liquidated_side,
        price_usd=price,
        size_usd=size_base * price,
        source=BYBIT_SOURCE,
    )


def bybit_tickers_parser(
    *, symbol: str, clock: Clock
) -> Callable[[bytes | str], OpenInterestSample | None]:
    def _p(raw: bytes | str) -> OpenInterestSample | None:
        return parse_bybit_tickers(raw, symbol=symbol, recv_ts_ns=clock.now_ns())

    return _p


def bybit_liquidation_parser(
    *, symbol: str, clock: Clock
) -> Callable[[bytes | str], LiquidationEvent | None]:
    def _p(raw: bytes | str) -> LiquidationEvent | None:
        return parse_bybit_liquidation(raw, symbol=symbol, recv_ts_ns=clock.now_ns())

    return _p


def _loads(raw: bytes | str) -> dict[str, Any] | None:
    try:
        parsed = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise BybitParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        return None
    # Subscription ack / pong / heartbeat frames carry no `topic`.
    if "topic" not in parsed:
        return None
    return cast(dict[str, Any], parsed)


def _invert_aggressor(aggressor: str) -> Literal["long", "short"] | None:
    """Bybit `side` is the aggressor of the *closing* fill. Invert to the
    liquidated position's direction: Sell aggressor closes a long; Buy
    aggressor closes a short."""
    if aggressor == "Sell":
        return "long"
    if aggressor == "Buy":
        return "short"
    return None


def _base_asset(symbol: str) -> str:
    """Strip the USDT suffix for OpenInterestSample / LiquidationEvent.symbol.

    Bybit uses `BTCUSDT` on linear perps; the rest of the system models
    `symbol` as the underlying base asset (`BTC`).
    """
    if symbol.endswith("USDT"):
        return symbol[:-4]
    if symbol.endswith("USD"):
        return symbol[:-3]
    return symbol
