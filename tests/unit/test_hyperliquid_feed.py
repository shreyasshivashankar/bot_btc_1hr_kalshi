"""Unit tests for Hyperliquid `metaAndAssetCtxs` parser + subscribe builder."""

from __future__ import annotations

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.hyperliquid import (
    HYPERLIQUID_SOURCE,
    HyperliquidParseError,
    build_hyperliquid_subscribe,
    hyperliquid_parser,
    parse_hyperliquid_meta_and_asset_ctxs,
)
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def _frame(*, oi: str = "23456.78", mark_px: str = "67000.0") -> bytes:
    return orjson.dumps(
        {
            "channel": "metaAndAssetCtxs",
            "data": [
                {"universe": [{"name": "BTC"}, {"name": "ETH"}, {"name": "SOL"}]},
                [
                    {"openInterest": oi, "markPx": mark_px, "funding": "0.000012"},
                    {"openInterest": "1000000", "markPx": "3000.0"},
                    {"openInterest": "5000", "markPx": "150.0"},
                ],
            ],
        }
    )


def test_subscribe_payload_matches_documented_envelope() -> None:
    raw = build_hyperliquid_subscribe()
    parsed = orjson.loads(raw)
    assert parsed == {"method": "subscribe", "subscription": {"type": "metaAndAssetCtxs"}}


def test_parses_btc_oi_and_converts_to_usd() -> None:
    sample = parse_hyperliquid_meta_and_asset_ctxs(
        _frame(oi="100.0", mark_px="70000.0"),
        asset="BTC",
        recv_ts_ns=42_000,
    )
    assert sample is not None
    # OI is BTC-denominated on Hyperliquid; we convert via markPx.
    assert sample.total_oi_usd == pytest.approx(100.0 * 70_000.0)
    assert sample.symbol == "BTC"
    assert sample.source == HYPERLIQUID_SOURCE
    assert sample.exchanges_count == 1
    assert sample.ts_ns == 42_000


def test_finds_btc_at_nonzero_index() -> None:
    """BTC's index in `universe` is not stable — the parser must look it up
    by name every frame, not cache it."""
    raw = orjson.dumps(
        {
            "channel": "metaAndAssetCtxs",
            "data": [
                {"universe": [{"name": "ETH"}, {"name": "SOL"}, {"name": "BTC"}]},
                [
                    {"openInterest": "999", "markPx": "3000"},
                    {"openInterest": "999", "markPx": "150"},
                    {"openInterest": "12.5", "markPx": "80000"},
                ],
            ],
        }
    )
    sample = parse_hyperliquid_meta_and_asset_ctxs(raw, asset="BTC", recv_ts_ns=1)
    assert sample is not None
    assert sample.total_oi_usd == pytest.approx(12.5 * 80_000.0)


def test_returns_none_for_non_data_channels() -> None:
    """Subscription acks and other channels should be silently skipped,
    not raise — the WS yields heterogeneous frames."""
    sub_ack = orjson.dumps({"channel": "subscriptionResponse", "data": {"type": "metaAndAssetCtxs"}})
    other = orjson.dumps({"channel": "trades", "data": []})
    assert (
        parse_hyperliquid_meta_and_asset_ctxs(sub_ack, asset="BTC", recv_ts_ns=1) is None
    )
    assert (
        parse_hyperliquid_meta_and_asset_ctxs(other, asset="BTC", recv_ts_ns=1) is None
    )


def test_returns_none_when_asset_absent_from_universe() -> None:
    """Wrong-asset queries must not raise — the venue may legitimately
    not list a symbol on a given account."""
    raw = orjson.dumps(
        {
            "channel": "metaAndAssetCtxs",
            "data": [
                {"universe": [{"name": "ETH"}]},
                [{"openInterest": "100", "markPx": "3000"}],
            ],
        }
    )
    assert parse_hyperliquid_meta_and_asset_ctxs(raw, asset="BTC", recv_ts_ns=1) is None


def test_raises_on_malformed_data_envelope() -> None:
    raw = orjson.dumps({"channel": "metaAndAssetCtxs", "data": "not a list"})
    with pytest.raises(HyperliquidParseError, match="data="):
        parse_hyperliquid_meta_and_asset_ctxs(raw, asset="BTC", recv_ts_ns=1)


def test_raises_on_missing_oi_field() -> None:
    raw = orjson.dumps(
        {
            "channel": "metaAndAssetCtxs",
            "data": [
                {"universe": [{"name": "BTC"}]},
                [{"markPx": "67000"}],  # no openInterest
            ],
        }
    )
    with pytest.raises(HyperliquidParseError, match=r"openInterest|markPx"):
        parse_hyperliquid_meta_and_asset_ctxs(raw, asset="BTC", recv_ts_ns=1)


def test_raises_on_invalid_json() -> None:
    with pytest.raises(HyperliquidParseError, match="invalid JSON"):
        parse_hyperliquid_meta_and_asset_ctxs(b"{not json", asset="BTC", recv_ts_ns=1)


def test_parser_callable_uses_clock_for_recv_ts() -> None:
    clock = ManualClock(123_456_789)
    p = hyperliquid_parser(asset="BTC", clock=clock)
    sample = p(_frame())
    assert sample is not None
    assert sample.ts_ns == 123_456_789
    clock.advance_ns(1_000_000)
    sample2 = p(_frame())
    assert sample2 is not None
    assert sample2.ts_ns == 124_456_789
