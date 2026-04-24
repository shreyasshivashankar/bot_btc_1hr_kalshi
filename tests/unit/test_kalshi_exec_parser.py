"""Tests for the Kalshi private-channel WS parser.

We don't yet have captured fixtures for these channels (the bot has
never been live on Kalshi), so the tests exercise both the documented
real-wire shape and the legacy/synthesized shapes the parser intentionally
accepts as fallbacks. Pin against real fixtures once captured during
paper soak — same lifecycle as the public-channel parser at Slice 6.
"""

from __future__ import annotations

import orjson
import pytest

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

_RECV_NS = 1_700_000_000_000_000_000


def _frame(type_: str, msg: dict[str, object], **outer: object) -> bytes:
    return orjson.dumps({"type": type_, "msg": msg, **outer})


# ---------- fill channel -----------------------------------------------------


def test_fill_real_wire_shape() -> None:
    """Documented Kalshi shape: stringified-dollar prices, ISO ts, taker fee."""
    raw = _frame(
        "fill",
        {
            "trade_id": "t-1",
            "order_id": "o-1",
            "client_order_id": "c-1",
            "market_ticker": "KXBTC-26APR1817-B77875",
            "is_taker": True,
            "side": "yes",
            "yes_price_dollars": "0.4200",
            "no_price_dollars": "0.5800",
            "count": 5,
            "action": "buy",
            "taker_fee": 0.07,
            "maker_fee": 0.0,
            "ts": "2026-04-18T05:34:49.816683Z",
        },
        seq=42,
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecFillEvent)
    assert ev.seq == 42
    assert ev.trade_id == "t-1"
    assert ev.order_id == "o-1"
    assert ev.client_order_id == "c-1"
    assert ev.market_id == "KXBTC-26APR1817-B77875"
    assert ev.side == "YES"
    assert ev.action == "BUY"
    assert ev.price_cents == 42
    assert ev.contracts == 5
    assert ev.fees_usd == pytest.approx(0.07)
    assert ev.is_taker is True
    # ISO timestamp converted to ns; sanity-check it falls in 2026.
    assert 1_776_000_000_000_000_000 < ev.ts_ns < 1_777_000_000_000_000_000


def test_fill_legacy_int_cents_and_no_side() -> None:
    raw = _frame(
        "fill",
        {
            "trade_id": "t-2",
            "order_id": "o-2",
            "client_order_id": "c-2",
            "ticker": "M",
            "is_taker": False,
            "side": "no",
            "yes_price": 30,
            "no_price": 70,
            "count": 10,
            "action": "buy",
            "maker_fee": 0.05,
            "ts": 1_700_000_000,  # unix sec
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecFillEvent)
    assert ev.side == "NO"
    # NO bet — we record the NO-side price (70c), not the YES-side (30c).
    assert ev.price_cents == 70
    assert ev.fees_usd == pytest.approx(0.05)
    assert ev.is_taker is False
    assert ev.seq is None  # no seq on outer or msg
    assert ev.ts_ns == 1_700_000_000_000_000_000


def test_fill_falls_back_to_recv_ts_when_unstamped() -> None:
    raw = _frame(
        "fill",
        {
            "trade_id": "t",
            "order_id": "o",
            "client_order_id": "c",
            "market_ticker": "M",
            "side": "yes",
            "yes_price": 50,
            "no_price": 50,
            "count": 1,
            "action": "buy",
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecFillEvent)
    assert ev.ts_ns == _RECV_NS


def test_fill_aggregates_maker_and_taker_fees() -> None:
    raw = _frame(
        "fill",
        {
            "trade_id": "t",
            "order_id": "o",
            "client_order_id": "c",
            "market_ticker": "M",
            "side": "yes",
            "yes_price": 40,
            "no_price": 60,
            "count": 1,
            "action": "buy",
            "maker_fee": 0.03,
            "taker_fee": 0.04,
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecFillEvent)
    assert ev.fees_usd == pytest.approx(0.07)


def test_fill_rejects_unknown_action() -> None:
    raw = _frame(
        "fill",
        {
            "trade_id": "t", "order_id": "o", "client_order_id": "c",
            "market_ticker": "M", "side": "yes",
            "yes_price": 40, "no_price": 60, "count": 1,
            "action": "wat",
        },
    )
    with pytest.raises(KalshiExecParseError, match="action"):
        parse_exec_frame(raw, recv_ts_ns=_RECV_NS)


# ---------- order_update channel --------------------------------------------


def test_order_update_resting() -> None:
    raw = _frame(
        "order_update",
        {
            "order_id": "o-1",
            "client_order_id": "c-1",
            "market_ticker": "M",
            "side": "yes",
            "status": "resting",
            "count": 5,
            "remaining_count": 5,
            "yes_price": 40,
            "no_price": 60,
            "ts": "2026-04-18T05:35:00Z",
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecOrderUpdate)
    assert ev.status == "resting"
    assert ev.filled_contracts == 0
    assert ev.remaining_contracts == 5
    assert ev.limit_price_cents == 40
    assert ev.side == "YES"


def test_order_update_partial_fill_collapses_to_filled_when_remaining_zero() -> None:
    """A `partially_filled` lifecycle string with `remaining=0` is reported
    by the parser as `filled`, matching the REST broker's `_status_from_kalshi`
    behavior — keeps the OMS status machine consistent across REST/WS paths."""
    raw = _frame(
        "order_update",
        {
            "order_id": "o", "client_order_id": "c",
            "market_ticker": "M", "side": "yes", "status": "partially_filled",
            "count": 5, "remaining_count": 0, "yes_price": 40, "no_price": 60,
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecOrderUpdate)
    assert ev.status == "filled"
    assert ev.filled_contracts == 5
    assert ev.remaining_contracts == 0


def test_order_update_unknown_status_falls_back_to_remaining_heuristic() -> None:
    raw = _frame(
        "order_update",
        {
            "order_id": "o", "client_order_id": "c",
            "market_ticker": "M", "side": "yes", "status": "in_some_new_state",
            "count": 5, "remaining_count": 3, "yes_price": 40, "no_price": 60,
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecOrderUpdate)
    # remaining > 0 → treat as resting; unknown lifecycle should not crash.
    assert ev.status == "resting"


def test_order_update_filled_count_field_overrides_count_minus_remaining() -> None:
    raw = _frame(
        "order_update",
        {
            "order_id": "o", "client_order_id": "c",
            "market_ticker": "M", "side": "yes", "status": "partial",
            "filled_count": 7, "remaining_count": 3,
            "yes_price": 40, "no_price": 60,
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecOrderUpdate)
    assert ev.filled_contracts == 7
    assert ev.remaining_contracts == 3


# ---------- market_position channel -----------------------------------------


def test_market_position_signed_yes_long() -> None:
    raw = _frame(
        "market_position",
        {
            "market_ticker": "M",
            "position": 100,            # long YES 100 contracts
            "market_exposure": 4200,    # 42c * 100 = 4200 cents
            "realized_pnl": 1.25,
            "fees_paid": 0.10,
            "ts": "2026-04-18T05:35:00Z",
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecPositionSnapshot)
    assert ev.side == "YES"
    assert ev.contracts == 100
    assert ev.avg_entry_price_cents == 42
    assert ev.realized_pnl_usd == pytest.approx(1.25)
    assert ev.fees_paid_usd == pytest.approx(0.10)


def test_market_position_negative_signals_no_side() -> None:
    raw = _frame(
        "market_position",
        {
            "market_ticker": "M",
            "position": -50,
            "market_exposure": 1500,  # 30c * 50 = 1500
        },
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecPositionSnapshot)
    assert ev.side == "NO"
    assert ev.contracts == 50
    assert ev.avg_entry_price_cents == 30


def test_market_position_zero_is_preserved() -> None:
    """A flat position (0 contracts) is meaningful — the reconciler uses it
    to detect a position fully closing. Don't drop the frame."""
    raw = _frame(
        "market_position",
        {"market_ticker": "M", "position": 0, "market_exposure": 0},
    )
    ev = parse_exec_frame(raw, recv_ts_ns=_RECV_NS)
    assert isinstance(ev, ExecPositionSnapshot)
    assert ev.side == "YES"  # signed=0 normalizes to YES side, 0 contracts
    assert ev.contracts == 0
    assert ev.avg_entry_price_cents == 0


# ---------- control frames + errors -----------------------------------------


def test_control_frames_return_none() -> None:
    for ftype in ("subscribed", "ok", "error", "ping", "pong"):
        raw = orjson.dumps({"type": ftype, "msg": {"channel": "fill"}})
        assert parse_exec_frame(raw, recv_ts_ns=_RECV_NS) is None


def test_unknown_frame_type_raises_loud() -> None:
    raw = orjson.dumps({"type": "totally_new_frame", "msg": {}})
    with pytest.raises(KalshiExecParseError, match="unknown frame type"):
        parse_exec_frame(raw, recv_ts_ns=_RECV_NS)


def test_invalid_json_raises() -> None:
    with pytest.raises(KalshiExecParseError, match="invalid JSON"):
        parse_exec_frame(b"{not json", recv_ts_ns=_RECV_NS)


def test_non_object_raises() -> None:
    with pytest.raises(KalshiExecParseError, match="not an object"):
        parse_exec_frame(orjson.dumps([1, 2, 3]), recv_ts_ns=_RECV_NS)


def test_frame_missing_msg_raises() -> None:
    raw = orjson.dumps({"type": "fill"})
    with pytest.raises(KalshiExecParseError, match="missing msg"):
        parse_exec_frame(raw, recv_ts_ns=_RECV_NS)


def test_channel_aliases_accepted() -> None:
    """Kalshi has historically shipped these payloads under minor naming
    variants; the parser accepts the aliases so we don't break on a
    server-side rename."""
    fill_alias = _frame("execution_report", {
        "trade_id": "t", "order_id": "o", "client_order_id": "c",
        "market_ticker": "M", "side": "yes",
        "yes_price": 40, "no_price": 60, "count": 1, "action": "buy",
    })
    order_alias = _frame("user_order", {
        "order_id": "o", "client_order_id": "c", "market_ticker": "M",
        "side": "yes", "status": "resting",
        "count": 1, "remaining_count": 1, "yes_price": 40, "no_price": 60,
    })
    pos_alias = _frame("position", {
        "market_ticker": "M", "position": 1, "market_exposure": 40,
    })
    assert isinstance(parse_exec_frame(fill_alias, recv_ts_ns=_RECV_NS), ExecFillEvent)
    assert isinstance(parse_exec_frame(order_alias, recv_ts_ns=_RECV_NS), ExecOrderUpdate)
    assert isinstance(parse_exec_frame(pos_alias, recv_ts_ns=_RECV_NS), ExecPositionSnapshot)


# ---------- subscribe builder -----------------------------------------------


def test_build_subscribe_default_channels() -> None:
    out = orjson.loads(build_exec_subscribe(req_id=7))
    assert out["id"] == 7
    assert out["cmd"] == "subscribe"
    assert out["params"]["channels"] == ["fill", "user_orders", "market_positions"]
    # No ticker filter when none provided.
    assert "market_tickers" not in out["params"]


def test_build_subscribe_with_ticker_filter() -> None:
    out = orjson.loads(
        build_exec_subscribe(
            req_id=1, channels=("fill",), market_tickers=["A", "B"],
        )
    )
    assert out["params"]["channels"] == ["fill"]
    assert out["params"]["market_tickers"] == ["A", "B"]
