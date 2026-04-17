from __future__ import annotations

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.kalshi_parser import (
    KalshiParseError,
    build_subscribe,
    parse_frame,
)
from bot_btc_1hr_kalshi.market_data.types import BookUpdate, TradeEvent

RECV_NS = 1_700_000_000_000_000_000


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


def test_build_subscribe_shape() -> None:
    raw = build_subscribe(req_id=1, market_tickers=["A", "B"])
    data = orjson.loads(raw)
    assert data["id"] == 1
    assert data["cmd"] == "subscribe"
    assert data["params"]["market_tickers"] == ["A", "B"]
    assert "orderbook_delta" in data["params"]["channels"]
    assert "trade" in data["params"]["channels"]
