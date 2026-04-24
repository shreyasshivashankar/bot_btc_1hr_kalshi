"""Unit tests for Bybit V5 public WS parsers + subscribe builder (PR-B)."""

from __future__ import annotations

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.bybit import (
    BYBIT_SOURCE,
    BybitParseError,
    build_bybit_subscribe,
    bybit_liquidation_parser,
    bybit_liquidation_topic,
    bybit_tickers_parser,
    bybit_tickers_topic,
    parse_bybit_liquidation,
    parse_bybit_tickers,
)
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _tickers_frame(
    *,
    symbol: str = "BTCUSDT",
    open_interest: str = "12345.678",
    open_interest_value: str | None = "864000000.0",
    last_price: str = "70000.0",
) -> bytes:
    data: dict[str, str] = {
        "symbol": symbol,
        "openInterest": open_interest,
        "lastPrice": last_price,
    }
    if open_interest_value is not None:
        data["openInterestValue"] = open_interest_value
    return orjson.dumps(
        {
            "topic": bybit_tickers_topic(symbol),
            "type": "snapshot",
            "ts": 1_672_304_484_978,
            "data": data,
        }
    )


def _liq_frame(
    *,
    symbol: str = "BTCUSDT",
    side: str = "Sell",
    size: str = "0.5",
    price: str = "70000.0",
) -> bytes:
    return orjson.dumps(
        {
            "topic": bybit_liquidation_topic(symbol),
            "type": "snapshot",
            "ts": 1_672_304_486_868,
            "data": {
                "updatedTime": 1_672_304_486_865,
                "symbol": symbol,
                "side": side,
                "size": size,
                "price": price,
            },
        }
    )


# --- subscribe builder ------------------------------------------------------


def test_subscribe_payload_for_tickers_matches_v5_envelope() -> None:
    raw = build_bybit_subscribe(bybit_tickers_topic("BTCUSDT"))
    assert orjson.loads(raw) == {"op": "subscribe", "args": ["tickers.BTCUSDT"]}


def test_subscribe_payload_for_liquidation_matches_v5_envelope() -> None:
    raw = build_bybit_subscribe(bybit_liquidation_topic("BTCUSDT"))
    assert orjson.loads(raw) == {"op": "subscribe", "args": ["liquidation.BTCUSDT"]}


# --- tickers parser ---------------------------------------------------------


def test_tickers_prefers_open_interest_value_when_present() -> None:
    sample = parse_bybit_tickers(
        _tickers_frame(open_interest_value="864000000.0", open_interest="0", last_price="0"),
        symbol="BTCUSDT",
        recv_ts_ns=99,
    )
    assert sample is not None
    assert sample.total_oi_usd == pytest.approx(864_000_000.0)
    assert sample.symbol == "BTC"
    assert sample.source == BYBIT_SOURCE
    assert sample.exchanges_count == 1
    assert sample.ts_ns == 99


def test_tickers_falls_back_to_oi_times_last_price() -> None:
    sample = parse_bybit_tickers(
        _tickers_frame(open_interest_value=None, open_interest="100.0", last_price="70000.0"),
        symbol="BTCUSDT",
        recv_ts_ns=1,
    )
    assert sample is not None
    assert sample.total_oi_usd == pytest.approx(7_000_000.0)


def test_tickers_returns_none_for_subscribe_ack() -> None:
    """Subscribe acks have no `topic` field — silently skip, don't raise."""
    ack = orjson.dumps({"success": True, "ret_msg": "subscribe", "op": "subscribe"})
    assert parse_bybit_tickers(ack, symbol="BTCUSDT", recv_ts_ns=1) is None


def test_tickers_returns_none_for_other_topic() -> None:
    other = orjson.dumps({"topic": "orderbook.50.BTCUSDT", "data": {}})
    assert parse_bybit_tickers(other, symbol="BTCUSDT", recv_ts_ns=1) is None


def test_tickers_raises_on_malformed_data_object() -> None:
    raw = orjson.dumps({"topic": "tickers.BTCUSDT", "data": "not an object"})
    with pytest.raises(BybitParseError, match="not an object"):
        parse_bybit_tickers(raw, symbol="BTCUSDT", recv_ts_ns=1)


def test_tickers_raises_when_value_field_invalid_and_no_fallback() -> None:
    raw = orjson.dumps(
        {
            "topic": "tickers.BTCUSDT",
            "data": {"symbol": "BTCUSDT", "openInterestValue": "not-a-number"},
        }
    )
    with pytest.raises(BybitParseError, match="invalid openInterestValue"):
        parse_bybit_tickers(raw, symbol="BTCUSDT", recv_ts_ns=1)


def test_tickers_raises_when_fallback_fields_missing() -> None:
    raw = orjson.dumps(
        {"topic": "tickers.BTCUSDT", "data": {"symbol": "BTCUSDT"}}
    )
    with pytest.raises(BybitParseError, match="missing openInterest"):
        parse_bybit_tickers(raw, symbol="BTCUSDT", recv_ts_ns=1)


def test_tickers_raises_on_invalid_json() -> None:
    with pytest.raises(BybitParseError, match="invalid JSON"):
        parse_bybit_tickers(b"{not json", symbol="BTCUSDT", recv_ts_ns=1)


# --- liquidation parser -----------------------------------------------------


def test_liquidation_inverts_sell_aggressor_to_long() -> None:
    """Bybit `Sell` aggressor closes a long position."""
    event = parse_bybit_liquidation(
        _liq_frame(side="Sell", size="0.5", price="70000.0"),
        symbol="BTCUSDT",
        recv_ts_ns=42,
    )
    assert event is not None
    assert event.side == "long"
    assert event.size_usd == pytest.approx(0.5 * 70_000.0)
    assert event.price_usd == pytest.approx(70_000.0)
    assert event.symbol == "BTC"
    assert event.source == BYBIT_SOURCE
    assert event.ts_ns == 42


def test_liquidation_inverts_buy_aggressor_to_short() -> None:
    event = parse_bybit_liquidation(
        _liq_frame(side="Buy"), symbol="BTCUSDT", recv_ts_ns=1
    )
    assert event is not None
    assert event.side == "short"


def test_liquidation_returns_none_for_other_topic() -> None:
    other = orjson.dumps({"topic": "tickers.BTCUSDT", "data": {}})
    assert parse_bybit_liquidation(other, symbol="BTCUSDT", recv_ts_ns=1) is None


def test_liquidation_raises_on_unexpected_side_value() -> None:
    raw = _liq_frame(side="Unknown")
    with pytest.raises(BybitParseError, match="unexpected side"):
        parse_bybit_liquidation(raw, symbol="BTCUSDT", recv_ts_ns=1)


def test_liquidation_raises_on_missing_fields() -> None:
    raw = orjson.dumps(
        {"topic": "liquidation.BTCUSDT", "data": {"symbol": "BTCUSDT"}}
    )
    with pytest.raises(BybitParseError, match="missing side"):
        parse_bybit_liquidation(raw, symbol="BTCUSDT", recv_ts_ns=1)


# --- bound parser factories -------------------------------------------------


def test_tickers_parser_uses_clock() -> None:
    clock = ManualClock(123_456_789)
    parser = bybit_tickers_parser(symbol="BTCUSDT", clock=clock)
    sample = parser(_tickers_frame())
    assert sample is not None
    assert sample.ts_ns == 123_456_789
    clock.advance_ns(1_000_000)
    sample2 = parser(_tickers_frame())
    assert sample2 is not None
    assert sample2.ts_ns == 124_456_789


def test_liquidation_parser_uses_clock() -> None:
    clock = ManualClock(999)
    parser = bybit_liquidation_parser(symbol="BTCUSDT", clock=clock)
    event = parser(_liq_frame())
    assert event is not None
    assert event.ts_ns == 999
