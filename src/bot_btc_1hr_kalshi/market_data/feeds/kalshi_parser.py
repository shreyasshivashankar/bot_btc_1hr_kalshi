"""Pure parsers for Kalshi WS frames.

Kalshi's market-data WS speaks a small set of frame types (plus subscribe/ack
control frames). This module keeps the wire-format concerns isolated so the
transport layer can focus on connect/reconnect/staleness.

Frame shapes (Kalshi public docs, as of 2026):
    orderbook_snapshot:
        {"type": "orderbook_snapshot",
         "msg": {"market_ticker": "...",
                 "yes": [[price, size], ...],
                 "no":  [[price, size], ...],
                 "seq": N}}
    orderbook_delta:
        {"type": "orderbook_delta",
         "msg": {"market_ticker": "...",
                 "price": P, "delta": D, "side": "yes"|"no",
                 "seq": N}}
    trade:
        {"type": "trade",
         "msg": {"market_ticker": "...", "yes_price": P,
                 "count": N, "taker_side": "yes"|"no",
                 "ts": ISO_OR_NS}}

We model our L2Book in terms of YES bids / YES asks (YES is the contract we
trade). A NO bid at price p is equivalent to a YES ask at (100 - p); the
parser normalizes both sides into YES-space bids/asks.
"""

from __future__ import annotations

from typing import Any, cast

import orjson

from bot_btc_1hr_kalshi.market_data.types import AggressorSide, BookLevel, BookUpdate, TradeEvent
from bot_btc_1hr_kalshi.obs.schemas import Side


class KalshiParseError(ValueError):
    """Raised on malformed / unrecognized frames."""


def _as_ns(ts: Any, fallback_ns: int) -> int:
    """Kalshi timestamps can be either nanoseconds (int) or seconds (int/float).
    Treat anything < 10^12 as seconds. If absent, use the caller's fallback.
    """
    if ts is None:
        return fallback_ns
    if isinstance(ts, (int, float)):
        return int(ts * 1_000_000_000) if ts < 1_000_000_000_000 else int(ts)
    raise KalshiParseError(f"unsupported ts type: {type(ts).__name__}")


def _levels_yes(yes: list[list[int]], no: list[list[int]]) -> tuple[
    tuple[BookLevel, ...], tuple[BookLevel, ...]
]:
    """Convert Kalshi two-sided YES/NO quantities into a YES-centric book.

    YES bids stay as-is. NO bids at price `p` (someone wants to buy NO at p)
    become YES asks at `100 - p` (they'd sell YES at 100-p).
    """
    yes_bids = tuple(
        BookLevel(price_cents=int(p), size=int(s)) for p, s in yes if int(s) > 0
    )
    yes_asks = tuple(
        BookLevel(price_cents=100 - int(p), size=int(s)) for p, s in no if int(s) > 0
    )
    return yes_bids, yes_asks


def parse_frame(raw: bytes | str, *, recv_ts_ns: int) -> BookUpdate | TradeEvent | None:
    """Decode a single WS frame. Returns None for control frames (ack, heartbeat).

    `recv_ts_ns` is used when the server didn't stamp the message — we fall
    back to the local receive time to keep the staleness computation honest.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError as exc:
        raise KalshiParseError(f"invalid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise KalshiParseError("frame is not an object")

    ftype = data.get("type")
    if ftype in (None, "subscribed", "ok", "error", "ping", "pong"):
        return None

    msg = data.get("msg")
    if not isinstance(msg, dict):
        raise KalshiParseError(f"frame missing msg: {ftype}")

    if ftype == "orderbook_snapshot":
        yes_bids, yes_asks = _levels_yes(msg.get("yes", []) or [], msg.get("no", []) or [])
        return BookUpdate(
            seq=int(msg["seq"]),
            ts_ns=_as_ns(msg.get("ts"), recv_ts_ns),
            market_id=str(msg["market_ticker"]),
            bids=yes_bids,
            asks=yes_asks,
            is_snapshot=True,
        )

    if ftype == "orderbook_delta":
        side = msg["side"]
        price = int(msg["price"])
        delta = int(msg["delta"])
        # Kalshi deltas are signed quantity changes; our BookUpdate models
        # absolute post-change size. The transport layer maintains a shadow
        # book to convert deltas → absolute sizes before applying; here we
        # encode the raw per-level change as a single-level update.
        bids: tuple[BookLevel, ...]
        asks: tuple[BookLevel, ...]
        if side == "yes":
            bids = (BookLevel(price_cents=price, size=max(delta, 0)),)
            asks = ()
        elif side == "no":
            bids = ()
            asks = (BookLevel(price_cents=100 - price, size=max(delta, 0)),)
        else:
            raise KalshiParseError(f"unknown side: {side}")
        return BookUpdate(
            seq=int(msg["seq"]),
            ts_ns=_as_ns(msg.get("ts"), recv_ts_ns),
            market_id=str(msg["market_ticker"]),
            bids=bids,
            asks=asks,
            is_snapshot=False,
        )

    if ftype == "trade":
        taker_raw = str(msg["taker_side"]).upper()
        if taker_raw not in ("YES", "NO"):
            raise KalshiParseError(f"unknown taker_side: {taker_raw}")
        taker: Side = cast(Side, taker_raw)
        # A YES taker is lifting YES asks (buy-aggressor); a NO taker is
        # lifting NO asks, which in YES-space is a sell-aggressor.
        aggressor: AggressorSide = "buy" if taker == "YES" else "sell"
        return TradeEvent(
            seq=int(msg.get("seq", 0)),
            ts_ns=_as_ns(msg.get("ts"), recv_ts_ns),
            market_id=str(msg["market_ticker"]),
            price_cents=int(msg["yes_price"]),
            size=int(msg["count"]),
            aggressor=aggressor,
            taker_side=taker,
        )

    raise KalshiParseError(f"unknown frame type: {ftype}")


def build_subscribe(
    *, req_id: int, market_tickers: list[str], channels: tuple[str, ...] = ("orderbook_delta", "trade")
) -> bytes:
    """Serialize a subscribe command. Kalshi expects JSON over the text frame."""
    return orjson.dumps(
        {
            "id": req_id,
            "cmd": "subscribe",
            "params": {
                "channels": list(channels),
                "market_tickers": market_tickers,
            },
        }
    )
