"""Pure parsers for Kalshi WS frames.

Kalshi's market-data WS speaks a small set of frame types (plus subscribe/ack
control frames). This module keeps the wire-format concerns isolated so the
transport layer can focus on connect/reconnect/staleness.

Wire format (pinned against captured fixtures from api.elections.kalshi.com
/trade-api/ws/v2, April 2026 — see tests/fixtures/kalshi_ws_frames.jsonl):

    orderbook_snapshot:
        {"type": "orderbook_snapshot", "sid": 1, "seq": N,
         "msg": {"market_ticker": "...", "market_id": "...",
                 "yes_dollars_fp": [["0.0100", "5000.00"], ...],
                 "no_dollars_fp":  [["0.0100", "106.00"], ...]}}
        Either side may be empty/absent on one-sided books.

    orderbook_delta:
        {"type": "orderbook_delta", "sid": 1, "seq": N,
         "msg": {"market_ticker": "...", "market_id": "...",
                 "price_dollars": "0.8100",
                 "delta_fp": "-5000.00",      # signed; negative = cancel/fill
                 "side": "yes" | "no",
                 "ts": "2026-04-18T05:34:49.816683Z"}}

    trade (REST-observed shape; WS envelope expected to match under `msg`):
        {"type": "trade", "seq": N,
         "msg": {"market_ticker": "...",
                 "yes_price_dollars": "0.1600",
                 "no_price_dollars":  "0.8400",
                 "count_fp": "232.54",        # fractional contracts allowed
                 "taker_side": "yes" | "no",
                 "created_time": "2026-04-18T05:26:56.728579Z",
                 "trade_id": "..."}}

Notes vs the earlier spec we initially coded against:
  * `seq` is on the OUTER frame (not `msg.seq`).
  * Prices and sizes are STRINGS in dollars / contracts, not ints.
  * Timestamps are ISO 8601 strings (`ts` on deltas, `created_time` on trades).

Legacy int-cent fixtures (`yes` / `no` / `price` / `delta` / `msg.seq` /
`yes_price` / `count`) are still accepted as fallbacks so the compact
hand-built unit-test fixtures continue to exercise the same code path.

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
    """Normalize a Kalshi timestamp to nanoseconds.

    Observed wire forms:
      * ISO-8601 string with 'Z' suffix (real wire, e.g. delta.msg.ts):
        "2026-04-18T05:34:49.816683Z"
      * seconds as int/float (legacy fixtures)
      * nanoseconds as int (legacy fixtures; anything ≥ 10^12 is assumed ns)

    None → caller's fallback (usually local recv time).
    """
    if ts is None:
        return fallback_ns
    if isinstance(ts, str):
        # Python's fromisoformat handles 'Z' on 3.11+. Fall through to error
        # if the string is non-ISO so we fail loud on future wire drift.
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError as exc:
            raise KalshiParseError(f"unparseable ts string: {ts!r}") from exc
        return int(dt.timestamp() * 1_000_000_000)
    if isinstance(ts, (int, float)):
        return int(ts * 1_000_000_000) if ts < 1_000_000_000_000 else int(ts)
    raise KalshiParseError(f"unsupported ts type: {type(ts).__name__}")


def _price_to_cents(p: Any) -> int:
    """Accept either an int (legacy cents) or a stringified dollar value
    ('0.4200' → 42). Kalshi's wire format uses the latter."""
    if isinstance(p, str):
        return round(float(p) * 100)
    return int(p)


def _size_to_int(s: Any) -> int:
    if isinstance(s, str):
        return round(float(s))
    return int(s)


def _levels_yes(
    yes: list[list[Any]], no: list[list[Any]],
) -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
    """Convert Kalshi two-sided YES/NO quantities into a YES-centric book.

    YES bids stay as-is. NO bids at price `p` (someone wants to buy NO at p)
    become YES asks at `100 - p` (they'd sell YES at 100-p).
    """
    yes_bids = tuple(
        BookLevel(price_cents=_price_to_cents(p), size=_size_to_int(s))
        for p, s in yes if _size_to_int(s) > 0
    )
    yes_asks = tuple(
        BookLevel(price_cents=100 - _price_to_cents(p), size=_size_to_int(s))
        for p, s in no if _size_to_int(s) > 0
    )
    return yes_bids, yes_asks


def peek_frame_type(raw: bytes | str) -> str:
    """Return the frame's `type` field without full parse, for diagnostics.

    Returns the literal string (``"orderbook_delta"``, ``"trade"``, ``"ping"``,
    etc.) or one of the sentinels ``"<no-type>"`` / ``"<invalid-json>"`` /
    ``"<non-object>"`` so callers can bucket anomalies without re-raising.
    This is intentionally a separate pass so parse_frame's hot path is
    unchanged; the extra orjson.loads on a small WS frame is <5µs.
    """
    try:
        data = orjson.loads(raw)
    except orjson.JSONDecodeError:
        return "<invalid-json>"
    if not isinstance(data, dict):
        return "<non-object>"
    ftype = data.get("type")
    if not isinstance(ftype, str):
        return "<no-type>"
    return ftype


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

    def _seq() -> int:
        # `seq` may appear on the outer frame (real wire) or nested in msg
        # (legacy fixture format). Looked up lazily so unknown-type frames
        # raise "unknown frame type" rather than a missing-seq error.
        raw = data.get("seq", msg.get("seq"))
        if raw is None:
            raise KalshiParseError(f"{ftype} missing seq")
        return int(raw)

    if ftype == "orderbook_snapshot":
        yes_raw = msg.get("yes_dollars_fp") or msg.get("yes") or []
        no_raw = msg.get("no_dollars_fp") or msg.get("no") or []
        yes_bids, yes_asks = _levels_yes(yes_raw, no_raw)
        return BookUpdate(
            seq=_seq(),
            ts_ns=_as_ns(msg.get("ts"), recv_ts_ns),
            market_id=str(msg["market_ticker"]),
            bids=yes_bids,
            asks=yes_asks,
            is_snapshot=True,
        )

    if ftype == "orderbook_delta":
        side = msg["side"]
        # Wire format: price_dollars / delta_fp as strings. Legacy fixtures:
        # price / delta as ints. Prefer wire fields, fall back to legacy.
        price_raw = msg.get("price_dollars", msg.get("price"))
        delta_raw = msg.get("delta_fp", msg.get("delta"))
        if price_raw is None or delta_raw is None:
            raise KalshiParseError("orderbook_delta missing price/delta")
        price = _price_to_cents(price_raw)
        # Deltas are signed quantity changes. Preserve sign through
        # float→int rounding; int(round(-0.4)) is 0, not -1, so only use
        # the fp path for string inputs.
        delta = (
            round(float(delta_raw)) if isinstance(delta_raw, str)
            else int(delta_raw)
        )
        bids: tuple[BookLevel, ...]
        asks: tuple[BookLevel, ...]
        if side == "yes":
            bids = (BookLevel(price_cents=price, size=delta),)
            asks = ()
        elif side == "no":
            bids = ()
            asks = (BookLevel(price_cents=100 - price, size=delta),)
        else:
            raise KalshiParseError(f"unknown side: {side}")
        return BookUpdate(
            seq=_seq(),
            ts_ns=_as_ns(msg.get("ts"), recv_ts_ns),
            market_id=str(msg["market_ticker"]),
            bids=bids,
            asks=asks,
            is_snapshot=False,
        )

    if ftype == "trade":
        # Real Kalshi trade-record shape (observed via REST /markets/trades,
        # WS wire expected to match):
        #   {"yes_price_dollars": "0.1600", "no_price_dollars": "0.8400",
        #    "count_fp": "232.54", "taker_side": "no",
        #    "created_time": "2026-04-18T05:26:56.728579Z",
        #    "ticker": "...", "trade_id": "..."}
        # REST uses `ticker`, the WS envelope is expected to use
        # `market_ticker` like snapshot/delta — accept either.
        # Legacy fixture shape (int cents) kept as fallback for existing tests.
        taker_raw = str(msg["taker_side"]).upper()
        if taker_raw not in ("YES", "NO"):
            raise KalshiParseError(f"unknown taker_side: {taker_raw}")
        taker: Side = cast(Side, taker_raw)
        # A YES taker is lifting YES asks (buy-aggressor); a NO taker is
        # lifting NO asks, which in YES-space is a sell-aggressor.
        aggressor: AggressorSide = "buy" if taker == "YES" else "sell"

        price_raw = msg.get("yes_price_dollars", msg.get("yes_price"))
        if price_raw is None:
            raise KalshiParseError("trade missing yes_price_dollars / yes_price")
        size_raw = msg.get("count_fp", msg.get("count"))
        if size_raw is None:
            raise KalshiParseError("trade missing count_fp / count")
        ts_raw = msg.get("created_time", msg.get("ts"))
        market_raw = msg.get("market_ticker", msg.get("ticker"))
        if market_raw is None:
            raise KalshiParseError("trade missing market_ticker / ticker")

        seq_raw = data.get("seq", msg.get("seq", 0))
        return TradeEvent(
            seq=int(seq_raw) if seq_raw is not None else 0,
            ts_ns=_as_ns(ts_raw, recv_ts_ns),
            market_id=str(market_raw),
            price_cents=_price_to_cents(price_raw),
            size=_size_to_int(size_raw),
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
