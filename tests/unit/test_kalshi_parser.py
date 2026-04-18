from __future__ import annotations

import json
from pathlib import Path

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import (
    KalshiParseError,
    build_subscribe,
    parse_frame,
)
from bot_btc_1hr_kalshi.market_data.types import BookUpdate, TradeEvent

RECV_NS = 1_700_000_000_000_000_000

# Path to live-captured WS frames. Refresh via:
#   .venv/bin/python scripts/capture_kalshi_frames.py
# The tests below read from this file so wire-format drift at Kalshi's end
# surfaces as a test failure, not a silent production parse error.
FIXTURE_PATH = Path(__file__).parent.parent / "fixtures" / "kalshi_ws_frames.jsonl"


def _load_fixture_frames() -> list[dict]:
    """Yield `{recv_ts_ns, raw_str, parsed_json}` for each captured frame."""
    out: list[dict] = []
    with FIXTURE_PATH.open() as f:
        for line in f:
            rec = json.loads(line)
            out.append({
                "recv_ts_ns": rec["recv_ts_ns"],
                "raw": rec["raw"],
                "parsed": json.loads(rec["raw"]),
            })
    return out


def _enc(d: dict) -> bytes:
    return orjson.dumps(d)


def test_parses_orderbook_snapshot_yes_and_no_sides_into_yes_space() -> None:
    frame = _enc({
        "type": "orderbook_snapshot",
        "msg": {
            "market_ticker": "KBTC-2604AB",
            "seq": 42,
            "ts": 1_700_000_000,  # seconds → ns via parser
            "yes": [[40, 500], [39, 100]],
            "no":  [[55, 300]],  # NO bid at 55 → YES ask at 45
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert ev.is_snapshot
    assert ev.market_id == "KBTC-2604AB"
    assert ev.seq == 42
    assert ev.ts_ns == 1_700_000_000 * 1_000_000_000
    assert [(lvl.price_cents, lvl.size) for lvl in ev.bids] == [(40, 500), (39, 100)]
    assert [(lvl.price_cents, lvl.size) for lvl in ev.asks] == [(45, 300)]


def test_parses_orderbook_delta_yes_side() -> None:
    frame = _enc({
        "type": "orderbook_delta",
        "msg": {"market_ticker": "M", "seq": 7, "price": 38, "delta": 100, "side": "yes"},
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert not ev.is_snapshot
    assert ev.bids == (ev.bids[0],) and ev.bids[0].price_cents == 38 and ev.bids[0].size == 100
    assert ev.asks == ()


def test_parses_orderbook_delta_negative_signed_through() -> None:
    """Regression: negative deltas (partial fills / cancels) must pass through
    as signed values — not be masked to 0, which would cause L2Book to wipe
    the level."""
    frame = _enc({
        "type": "orderbook_delta",
        "msg": {"market_ticker": "M", "seq": 7, "price": 38, "delta": -10, "side": "yes"},
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert ev.bids[0].size == -10
    assert ev.asks == ()


def test_parses_orderbook_delta_no_side_flips_to_yes_ask() -> None:
    frame = _enc({
        "type": "orderbook_delta",
        "msg": {"market_ticker": "M", "seq": 8, "price": 55, "delta": 50, "side": "no"},
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert ev.bids == ()
    assert len(ev.asks) == 1 and ev.asks[0].price_cents == 45 and ev.asks[0].size == 50


def test_parses_trade_yes_taker_is_buy_aggressor() -> None:
    frame = _enc({
        "type": "trade",
        "msg": {
            "market_ticker": "M", "yes_price": 42, "count": 10,
            "taker_side": "yes", "seq": 11, "ts": 1_700_000_000,
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, TradeEvent)
    assert ev.price_cents == 42
    assert ev.size == 10
    assert ev.aggressor == "buy"
    assert ev.taker_side == "YES"


def test_parses_trade_no_taker_is_sell_aggressor() -> None:
    frame = _enc({
        "type": "trade",
        "msg": {"market_ticker": "M", "yes_price": 42, "count": 5, "taker_side": "no"},
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, TradeEvent)
    assert ev.aggressor == "sell"
    assert ev.taker_side == "NO"


def test_control_frames_return_none() -> None:
    for ftype in ("subscribed", "ok", "error", "ping", "pong"):
        assert parse_frame(_enc({"type": ftype}), recv_ts_ns=RECV_NS) is None


def test_missing_ts_falls_back_to_recv() -> None:
    frame = _enc({
        "type": "orderbook_snapshot",
        "msg": {"market_ticker": "M", "seq": 1, "yes": [[40, 100]], "no": []},
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert ev.ts_ns == RECV_NS


def test_malformed_json_raises() -> None:
    with pytest.raises(KalshiParseError):
        parse_frame(b"{not json", recv_ts_ns=RECV_NS)


def test_unknown_frame_type_raises() -> None:
    with pytest.raises(KalshiParseError):
        parse_frame(_enc({"type": "mystery", "msg": {}}), recv_ts_ns=RECV_NS)


def test_unknown_side_raises() -> None:
    with pytest.raises(KalshiParseError):
        parse_frame(
            _enc({"type": "orderbook_delta",
                  "msg": {"market_ticker": "M", "seq": 1, "price": 40, "delta": 1, "side": "??"}}),
            recv_ts_ns=RECV_NS,
        )


def test_parses_real_wire_snapshot_dollars_fp_format() -> None:
    """Real api.elections.kalshi.com frames: string dollar prices, string
    float sizes, and `seq` on the outer frame (not in msg). This test pins
    that shape against regression."""
    frame = _enc({
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {
            "market_ticker": "KXBTC-26APR1801-B66250",
            "yes_dollars_fp": [["0.4200", "500.00"]],
            "no_dollars_fp":  [["0.5800", "300.00"]],  # YES ask @ 42
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert ev.is_snapshot
    assert ev.seq == 1
    assert ev.market_id == "KXBTC-26APR1801-B66250"
    assert [(lvl.price_cents, lvl.size) for lvl in ev.bids] == [(42, 500)]
    assert [(lvl.price_cents, lvl.size) for lvl in ev.asks] == [(42, 300)]


def test_parses_real_wire_delta_price_dollars_format() -> None:
    frame = _enc({
        "type": "orderbook_delta",
        "seq": 5,
        "msg": {
            "market_ticker": "M",
            "price_dollars": "0.3800",
            "delta_fp": "100.00",
            "side": "yes",
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, BookUpdate)
    assert not ev.is_snapshot
    assert ev.seq == 5
    assert ev.bids[0].price_cents == 38
    assert ev.bids[0].size == 100


def test_build_subscribe_shape() -> None:
    raw = build_subscribe(req_id=1, market_tickers=["A", "B"])
    data = orjson.loads(raw)
    assert data["id"] == 1
    assert data["cmd"] == "subscribe"
    assert data["params"]["market_tickers"] == ["A", "B"]
    assert "orderbook_delta" in data["params"]["channels"]
    assert "trade" in data["params"]["channels"]


# --- Live-fixture tests ----------------------------------------------------
# These read real frames captured from Kalshi's production WS. If Kalshi
# changes wire format, re-running the capture script and the test suite
# surfaces the break immediately rather than in production.


def test_fixture_file_exists_and_has_expected_frame_types() -> None:
    """Guard: the fixture must cover every frame type the parser handles.
    If a frame type drops out of the fixture (capture window too short,
    channel rejected, etc.) we want to know BEFORE relying on synthetic
    frames for that type."""
    assert FIXTURE_PATH.exists(), (
        f"Missing {FIXTURE_PATH}. Run scripts/capture_kalshi_frames.py."
    )
    frames = _load_fixture_frames()
    types = {f["parsed"].get("type") for f in frames}
    required = {"orderbook_snapshot", "orderbook_delta", "subscribed", "error"}
    assert required.issubset(types), (
        f"Fixture missing required frame types: {required - types}. "
        "Re-run capture on a busier market or for longer."
    )
    # `trade` is allowed to be missing (BTC can be quiet for a full window);
    # the trade branch is exercised by a REST-shaped test below.


def test_every_captured_frame_parses_without_error() -> None:
    """Replay the entire captured fixture through the parser.

    This is the wire-format canary: any field-name or type drift that
    changes how Kalshi encodes a frame will raise KalshiParseError here,
    long before it reaches live trading."""
    frames = _load_fixture_frames()
    assert frames, "empty fixture"
    parse_errors: list[tuple[str, str]] = []
    for f in frames:
        try:
            parse_frame(f["raw"].encode(), recv_ts_ns=f["recv_ts_ns"])
        except KalshiParseError as exc:
            parse_errors.append((f["parsed"].get("type", "?"), str(exc)))
    assert not parse_errors, f"parse failures: {parse_errors}"


def test_captured_orderbook_delta_shape_is_pinned() -> None:
    """Pin one real orderbook_delta frame against field-level expectations."""
    frames = _load_fixture_frames()
    deltas = [f for f in frames if f["parsed"].get("type") == "orderbook_delta"]
    assert deltas, "no orderbook_delta frames in fixture"

    sample = deltas[0]
    parsed = sample["parsed"]

    # Frame envelope: outer `seq`, outer `sid`, `msg` dict.
    assert "seq" in parsed, "orderbook_delta must have outer `seq`"
    assert "msg" in parsed
    msg = parsed["msg"]

    # Inner fields we depend on. These are the exact names that must not drift.
    assert "market_ticker" in msg
    assert "price_dollars" in msg, "wire uses `price_dollars`, not `price`"
    assert "delta_fp" in msg, "wire uses `delta_fp`, not `delta`"
    assert "side" in msg
    assert msg["side"] in ("yes", "no")
    # Timestamp is ISO 8601 with Z suffix (not int seconds).
    assert "ts" in msg
    assert msg["ts"].endswith("Z")

    # End-to-end: parser must decode it without raising.
    ev = parse_frame(sample["raw"].encode(), recv_ts_ns=sample["recv_ts_ns"])
    assert isinstance(ev, BookUpdate)
    assert not ev.is_snapshot
    assert ev.market_id == msg["market_ticker"]
    assert ev.seq == parsed["seq"]
    # ts_ns should be derived from msg.ts (ISO 8601), not recv_ts_ns.
    assert ev.ts_ns != sample["recv_ts_ns"]


def test_captured_orderbook_snapshot_shape_is_pinned() -> None:
    frames = _load_fixture_frames()
    snaps = [f for f in frames if f["parsed"].get("type") == "orderbook_snapshot"]
    assert snaps, "no orderbook_snapshot frames in fixture"

    sample = snaps[0]
    parsed = sample["parsed"]
    assert "seq" in parsed
    msg = parsed["msg"]
    assert "market_ticker" in msg
    # At least one side must be present (one-sided snapshots are allowed).
    assert "yes_dollars_fp" in msg or "no_dollars_fp" in msg

    ev = parse_frame(sample["raw"].encode(), recv_ts_ns=sample["recv_ts_ns"])
    assert isinstance(ev, BookUpdate)
    assert ev.is_snapshot
    assert ev.market_id == msg["market_ticker"]


def test_captured_error_frame_is_control_not_book() -> None:
    frames = _load_fixture_frames()
    errs = [f for f in frames if f["parsed"].get("type") == "error"]
    assert errs, "no error frames in fixture"
    sample = errs[0]
    # error frames must parse as `None` (control), not raise.
    assert parse_frame(sample["raw"].encode(), recv_ts_ns=sample["recv_ts_ns"]) is None
    # Wire shape: {type: "error", msg: {code: int, msg: str}}
    msg = sample["parsed"]["msg"]
    assert "code" in msg
    assert "msg" in msg


def test_captured_subscribed_ack_is_control() -> None:
    frames = _load_fixture_frames()
    acks = [f for f in frames if f["parsed"].get("type") == "subscribed"]
    assert acks, "no subscribed frames in fixture"
    sample = acks[0]
    assert parse_frame(sample["raw"].encode(), recv_ts_ns=sample["recv_ts_ns"]) is None
    # Wire shape: {type: "subscribed", id, msg: {channel, sid}}
    msg = sample["parsed"]["msg"]
    assert "channel" in msg
    assert "sid" in msg


def test_parses_trade_with_real_rest_shape() -> None:
    """Trade frames weren't captured during the fixture window (BTC quiet),
    so we pin the parser against the Kalshi-REST trade-record shape — the
    WS envelope is expected to wrap identical fields under `msg`. Field
    names observed via GET /trade-api/v2/markets/trades on 2026-04-18.

    If a real WS trade frame is later captured, replace this with a
    fixture-loaded test and delete this synthetic one."""
    frame = _enc({
        "type": "trade",
        "seq": 77,
        "msg": {
            "market_ticker": "KXBTC-26APR1817-B77125",
            "yes_price_dollars": "0.1600",
            "no_price_dollars":  "0.8400",
            "count_fp": "232.54",
            "taker_side": "no",
            "created_time": "2026-04-18T05:26:56.728579Z",
            "trade_id": "fa3d0cb4-8dca-6836-3dbc-9369a9c2546e",
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, TradeEvent)
    assert ev.seq == 77
    assert ev.market_id == "KXBTC-26APR1817-B77125"
    assert ev.price_cents == 16
    assert ev.size == 233  # 232.54 rounds to 233
    assert ev.taker_side == "NO"
    assert ev.aggressor == "sell"
    # ISO 8601 created_time was parsed, not discarded.
    assert ev.ts_ns != RECV_NS


def test_parses_trade_yes_taker_real_shape() -> None:
    """Symmetric case: YES taker → buy aggressor."""
    frame = _enc({
        "type": "trade",
        "seq": 78,
        "msg": {
            "market_ticker": "KXBTC-26APR1817-B77125",
            "yes_price_dollars": "0.2400",
            "no_price_dollars":  "0.7600",
            "count_fp": "199.00",
            "taker_side": "yes",
            "created_time": "2026-04-18T05:26:01.109366Z",
            "trade_id": "4abda2e6-8a8c-7de6-66d9-0247ff3bfd44",
        },
    })
    ev = parse_frame(frame, recv_ts_ns=RECV_NS)
    assert isinstance(ev, TradeEvent)
    assert ev.price_cents == 24
    assert ev.size == 199
    assert ev.taker_side == "YES"
    assert ev.aggressor == "buy"


def test_unparseable_ts_string_raises_not_silent() -> None:
    """If Kalshi changes ts format (e.g., drops the Z, changes to ns int),
    we must raise KalshiParseError — not silently substitute recv_ts_ns."""
    frame = _enc({
        "type": "orderbook_delta",
        "seq": 1,
        "msg": {
            "market_ticker": "M",
            "price_dollars": "0.4200",
            "delta_fp": "100.00",
            "side": "yes",
            "ts": "not-a-timestamp",
        },
    })
    with pytest.raises(KalshiParseError, match="unparseable ts"):
        parse_frame(frame, recv_ts_ns=RECV_NS)
