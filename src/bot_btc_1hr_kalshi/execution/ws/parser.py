"""Pure parsers for Kalshi private WS execution frames.

The trading-API WS (`wss://api.elections.kalshi.com/trade-api/ws/v2`)
accepts the same signed handshake as REST (KALSHI-ACCESS-{KEY,TS,SIG})
and unlocks three additional channels beyond `orderbook_delta`/`trade`:

    fill              One frame per partial fill on a user order.
    user_orders       Lifecycle updates for user orders (resting, filled,
                      partially filled, cancelled, rejected).
    market_positions  Snapshot of the user's position in a market whenever
                      that position changes; signed YES contracts.

Kalshi's public docs don't pin the exact JSON shape for these channels,
and we have not yet captured real private-channel fixtures (the bot
runs in paper today, so nothing lands in these channels). The parser
here follows the same defensive posture as `kalshi_parser.py`:

  * Accept both string-dollar (`"0.4200"`) and int-cent (`42`) price
    fields — Kalshi's market-data WS ships the former on the real
    wire but our hand-built fixtures use the latter.
  * Accept both `market_ticker` and `ticker` so unit-test fixtures
    don't have to decide which one the real wire uses.
  * Accept multiple ts forms (ISO 8601 string, unix sec, unix ms,
    unix ns) via `_as_ns`.
  * Fail loud on unrecognized frame types so wire drift is surfaced
    immediately instead of silently dropping fills.

When real captured frames land in `tests/fixtures/` we pin the parser
against them and tighten the accepted shapes — same lifecycle the
public-channel parser went through at Slice 6.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

import orjson

from bot_btc_1hr_kalshi.execution.broker.base import OrderAction, OrderStatus
from bot_btc_1hr_kalshi.execution.ws.types import (
    ExecFillEvent,
    ExecOrderUpdate,
    ExecPositionSnapshot,
    ExecutionEvent,
)
from bot_btc_1hr_kalshi.obs.schemas import Side


class KalshiExecParseError(ValueError):
    """Raised on malformed / unrecognized private-channel frames."""


_STATUS_MAP: dict[str, OrderStatus] = {
    "resting": "resting",
    "open": "resting",
    "working": "resting",
    "filled": "filled",
    "executed": "filled",
    "partially_filled": "partially_filled",
    "partial_fill": "partially_filled",
    "partial": "partially_filled",
    "cancelled": "cancelled",
    "canceled": "cancelled",
    "rejected": "rejected",
}


def _as_ns(ts: Any, fallback_ns: int) -> int:
    """Normalize a Kalshi timestamp to nanoseconds. Mirrors `kalshi_parser._as_ns`."""
    if ts is None:
        return fallback_ns
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError as exc:
            raise KalshiExecParseError(f"unparseable ts string: {ts!r}") from exc
        return int(dt.timestamp() * 1_000_000_000)
    if isinstance(ts, (int, float)):
        # <10^12: treat as unix seconds (sec * 1e9 → ns)
        # 10^12..10^15: treat as unix milliseconds
        # >=10^15: treat as unix nanoseconds (already ns-scale)
        v = float(ts)
        if v < 1_000_000_000_000:
            return int(v * 1_000_000_000)
        if v < 1_000_000_000_000_000:
            return int(v * 1_000_000)
        return int(v)
    raise KalshiExecParseError(f"unsupported ts type: {type(ts).__name__}")


def _price_to_cents(p: Any) -> int:
    if isinstance(p, str):
        return round(float(p) * 100)
    if isinstance(p, bool):  # bool is subclass of int; reject explicitly
        raise KalshiExecParseError(f"invalid price: {p!r}")
    if isinstance(p, (int, float)):
        return round(float(p))
    raise KalshiExecParseError(f"unsupported price type: {type(p).__name__}")


def _to_int(s: Any, *, field: str) -> int:
    if isinstance(s, bool):
        raise KalshiExecParseError(f"{field}: bool where int expected")
    if isinstance(s, str):
        return int(float(s))
    if isinstance(s, (int, float)):
        return int(s)
    raise KalshiExecParseError(f"{field}: unsupported type {type(s).__name__}")


def _to_float(s: Any, *, field: str, default: float = 0.0) -> float:
    if s is None:
        return default
    if isinstance(s, bool):
        raise KalshiExecParseError(f"{field}: bool where number expected")
    if isinstance(s, (int, float)):
        return float(s)
    if isinstance(s, str):
        try:
            return float(s)
        except ValueError as exc:
            raise KalshiExecParseError(f"{field}: unparseable {s!r}") from exc
    raise KalshiExecParseError(f"{field}: unsupported type {type(s).__name__}")


def _side(raw: Any, *, field: str = "side") -> Side:
    if not isinstance(raw, str):
        raise KalshiExecParseError(f"{field}: not a string")
    u = raw.upper()
    if u not in ("YES", "NO"):
        raise KalshiExecParseError(f"{field}: unknown value {raw!r}")
    return cast(Side, u)


def _action(raw: Any) -> OrderAction:
    if not isinstance(raw, str):
        raise KalshiExecParseError("action: not a string")
    u = raw.upper()
    if u not in ("BUY", "SELL"):
        raise KalshiExecParseError(f"action: unknown value {raw!r}")
    return cast(OrderAction, u)


def _status(raw: Any, *, remaining: int) -> OrderStatus:
    if not isinstance(raw, str):
        raise KalshiExecParseError("status: not a string")
    mapped = _STATUS_MAP.get(raw.lower())
    if mapped is not None:
        if mapped == "partially_filled" and remaining == 0:
            return "filled"
        return mapped
    # Unknown lifecycle string — fall back to remaining-count heuristic so
    # future Kalshi additions don't crash the stream. We log via the caller.
    return "filled" if remaining == 0 else "resting"


def _market_id(msg: dict[str, Any]) -> str:
    raw = msg.get("market_ticker", msg.get("ticker"))
    if not isinstance(raw, str) or not raw:
        raise KalshiExecParseError("missing market_ticker / ticker")
    return raw


def _seq(frame: dict[str, Any], msg: dict[str, Any]) -> int | None:
    """Private channels don't always carry a seq (unlike orderbook). Return
    None when absent so consumers can still dedupe by order_id/trade_id."""
    raw = frame.get("seq", msg.get("seq"))
    if raw is None:
        return None
    return _to_int(raw, field="seq")


def _yes_price(msg: dict[str, Any], side: Side) -> int:
    """Kalshi fill/order frames quote both yes_price and no_price; we only
    store the price for the side we bought. Accepts legacy `price` too."""
    if side == "YES":
        raw = msg.get("yes_price_dollars", msg.get("yes_price"))
    else:
        raw = msg.get("no_price_dollars", msg.get("no_price"))
    if raw is None:
        raw = msg.get("price")
    if raw is None:
        raise KalshiExecParseError("missing yes_price / no_price / price")
    return _price_to_cents(raw)


def _fee_total(msg: dict[str, Any]) -> float:
    """Kalshi ships fees in USD. Real wire uses `maker_fee`/`taker_fee` on
    fill frames (exactly one non-zero per frame based on is_taker); legacy
    / synthesized fixtures may use a single `fee` or `fees`. Sum them all
    — anything absent reads as zero."""
    return (
        _to_float(msg.get("maker_fee"), field="maker_fee")
        + _to_float(msg.get("taker_fee"), field="taker_fee")
        + _to_float(msg.get("fee"), field="fee")
        + _to_float(msg.get("fees"), field="fees")
    )


def _parse_fill(
    frame: dict[str, Any], msg: dict[str, Any], recv_ts_ns: int
) -> ExecFillEvent:
    side = _side(msg.get("side"))
    return ExecFillEvent(
        seq=_seq(frame, msg),
        ts_ns=_as_ns(msg.get("ts") or msg.get("created_time") or msg.get("created_time_ms"), recv_ts_ns),
        trade_id=str(msg.get("trade_id", "")),
        order_id=str(msg.get("order_id", "")),
        client_order_id=str(msg.get("client_order_id", "")),
        market_id=_market_id(msg),
        side=side,
        action=_action(msg.get("action")),
        price_cents=_yes_price(msg, side),
        contracts=_to_int(msg.get("count", msg.get("filled_count", 0)), field="count"),
        fees_usd=_fee_total(msg),
        is_taker=bool(msg.get("is_taker", False)),
    )


def _parse_order_update(
    frame: dict[str, Any], msg: dict[str, Any], recv_ts_ns: int
) -> ExecOrderUpdate:
    # Kalshi frames carry either (count, remaining_count) or (filled_count,
    # remaining_count); prefer the pair that sums cleanly.
    remaining = _to_int(msg.get("remaining_count", 0), field="remaining_count")
    filled: int
    if "filled_count" in msg:
        filled = _to_int(msg["filled_count"], field="filled_count")
    else:
        total = _to_int(msg.get("count", remaining), field="count")
        filled = max(0, total - remaining)
    side = _side(msg.get("side"))
    return ExecOrderUpdate(
        seq=_seq(frame, msg),
        ts_ns=_as_ns(msg.get("ts") or msg.get("updated_time"), recv_ts_ns),
        order_id=str(msg.get("order_id", "")),
        client_order_id=str(msg.get("client_order_id", "")),
        market_id=_market_id(msg),
        status=_status(msg.get("status", ""), remaining=remaining),
        side=side,
        filled_contracts=filled,
        remaining_contracts=remaining,
        limit_price_cents=_yes_price(msg, side),
    )


def _parse_position(
    frame: dict[str, Any], msg: dict[str, Any], recv_ts_ns: int
) -> ExecPositionSnapshot:
    # Kalshi positions are signed YES contracts: positive = long YES,
    # negative = long NO. Normalize to (side, abs_contracts) matching
    # `BrokerPosition`. A zero position is meaningful (flat) — keep it.
    signed = _to_int(msg.get("position", 0), field="position")
    side: Side = "YES" if signed >= 0 else "NO"
    abs_contracts = abs(signed)
    exposure_cents = _to_int(msg.get("market_exposure", 0), field="market_exposure")
    # market_exposure is total notional in cents; avg entry is exposure / size.
    avg_entry = (exposure_cents // abs_contracts) if abs_contracts else 0
    return ExecPositionSnapshot(
        seq=_seq(frame, msg),
        ts_ns=_as_ns(msg.get("ts") or msg.get("updated_time"), recv_ts_ns),
        market_id=_market_id(msg),
        side=side,
        contracts=abs_contracts,
        avg_entry_price_cents=avg_entry,
        realized_pnl_usd=_to_float(msg.get("realized_pnl"), field="realized_pnl"),
        fees_paid_usd=_to_float(msg.get("fees_paid"), field="fees_paid"),
    )


def parse_exec_frame(
    raw: bytes | str, *, recv_ts_ns: int
) -> ExecutionEvent | None:
    """Decode a single private-channel WS frame.

    Returns `None` for control frames (subscribed ack, ping/pong, error).
    Raises `KalshiExecParseError` on malformed JSON or known-but-invalid
    payloads; unrecognized frame types also raise so wire drift is
    surfaced immediately rather than silently dropped.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise KalshiExecParseError(f"invalid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise KalshiExecParseError("frame is not an object")

    ftype = data.get("type")
    if ftype in (None, "subscribed", "ok", "error", "ping", "pong"):
        return None

    msg = data.get("msg")
    if not isinstance(msg, dict):
        raise KalshiExecParseError(f"frame missing msg: {ftype}")

    # Channel-name aliasing: Kalshi has historically shipped the same
    # semantic payload under minor naming variants. Accept all.
    if ftype in ("fill", "fills", "execution_report"):
        return _parse_fill(data, msg, recv_ts_ns)
    if ftype in ("order_update", "user_order", "user_orders", "order"):
        return _parse_order_update(data, msg, recv_ts_ns)
    if ftype in ("market_position", "market_positions", "position"):
        return _parse_position(data, msg, recv_ts_ns)

    raise KalshiExecParseError(f"unknown frame type: {ftype}")


def build_exec_subscribe(
    *,
    req_id: int,
    channels: tuple[str, ...] = ("fill", "user_orders", "market_positions"),
    market_tickers: list[str] | None = None,
) -> bytes:
    """Serialize a subscribe command for private execution channels.

    Kalshi's WS `subscribe` cmd accepts an optional `market_tickers`
    filter; when omitted the user is subscribed to events across all
    their open markets. We pass through the filter if provided so a
    caller that knows the tracked-market list can narrow the firehose.
    """
    params: dict[str, Any] = {"channels": list(channels)}
    if market_tickers:
        params["market_tickers"] = market_tickers
    return orjson.dumps({"id": req_id, "cmd": "subscribe", "params": params})
