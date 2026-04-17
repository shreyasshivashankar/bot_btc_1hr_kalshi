from __future__ import annotations

from collections.abc import AsyncIterator

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    SpotParseError,
    binance_parser,
    build_coinbase_subscribe,
    coinbase_parser,
    parse_binance,
    parse_coinbase,
)
from bot_btc_1hr_kalshi.market_data.types import SpotTick
from bot_btc_1hr_kalshi.obs.clock import ManualClock

RECV_NS = 1_700_000_000_000_000_000


def test_coinbase_ticker_parses() -> None:
    frame = orjson.dumps({
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "63123.45",
        "last_size": "0.02",
        "time": "2026-04-17T04:00:00.123456Z",
    })
    tick = parse_coinbase(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.venue == "coinbase"
    assert tick.price_usd == pytest.approx(63123.45)
    assert tick.size == pytest.approx(0.02)
    assert tick.ts_ns > 0


def test_coinbase_subscriptions_heartbeat_return_none() -> None:
    for t in ("subscriptions", "heartbeat", "error"):
        assert parse_coinbase(orjson.dumps({"type": t}), recv_ts_ns=RECV_NS) is None


def test_coinbase_invalid_price_raises() -> None:
    with pytest.raises(SpotParseError):
        parse_coinbase(
            orjson.dumps({"type": "ticker", "product_id": "BTC-USD", "price": "oops"}),
            recv_ts_ns=RECV_NS,
        )


def test_coinbase_invalid_json_raises() -> None:
    with pytest.raises(SpotParseError):
        parse_coinbase(b"{not json", recv_ts_ns=RECV_NS)


def test_coinbase_bad_time_falls_back_to_recv() -> None:
    frame = orjson.dumps({
        "type": "ticker", "product_id": "BTC-USD", "price": "1", "time": "garbage",
    })
    tick = parse_coinbase(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.ts_ns == RECV_NS


def test_binance_trade_parses() -> None:
    frame = orjson.dumps({
        "e": "trade", "E": 1_700_000_000_000, "T": 1_700_000_000_500,
        "s": "BTCUSDT", "p": "60500.1", "q": "0.003",
    })
    tick = parse_binance(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.venue == "binance"
    assert tick.price_usd == pytest.approx(60500.1)
    assert tick.size == pytest.approx(0.003)
    assert tick.ts_ns == 1_700_000_000_500 * 1_000_000


def test_binance_non_trade_returns_none() -> None:
    assert parse_binance(orjson.dumps({"e": "kline"}), recv_ts_ns=RECV_NS) is None


def test_binance_missing_fields_raises() -> None:
    with pytest.raises(SpotParseError):
        parse_binance(orjson.dumps({"e": "trade"}), recv_ts_ns=RECV_NS)


def test_build_coinbase_subscribe_shape() -> None:
    raw = build_coinbase_subscribe(["BTC-USD"])
    data = orjson.loads(raw)
    assert data == {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}


# --- SpotFeed integration over FakeConn -------------------------------------


class FakeConn:
    def __init__(self, frames: list[bytes]) -> None:
        self._frames = list(frames)
        self.sent: list[bytes | str] = []
        self.closed = False

    async def send(self, data: bytes | str) -> None:
        self.sent.append(data)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[bytes]:
        for f in self._frames:
            yield f

    async def close(self) -> None:
        self.closed = True


async def test_spotfeed_coinbase_yields_ticks_and_sends_subscribe() -> None:
    from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker

    frame = orjson.dumps({
        "type": "ticker", "product_id": "BTC-USD", "price": "60000", "last_size": "0.1",
        "time": "2026-04-17T04:00:00Z",
    })
    conn = FakeConn([frame])
    clock = ManualClock(0)
    st = StalenessTracker(name="coinbase", clock=clock, threshold_ms=2000)

    async def connect(url: str) -> FakeConn:
        return conn

    feed = SpotFeed(
        name="coinbase",
        ws_url="ws://fake",
        clock=clock,
        ws_connect=connect,  # type: ignore[arg-type]
        staleness=st,
        parse=coinbase_parser(clock),
        subscribe=build_coinbase_subscribe(["BTC-USD"]),
    )

    async for tick in feed.events():
        assert tick.venue == "coinbase"
        assert tick.price_usd == pytest.approx(60000.0)
        break

    assert conn.sent and b'"subscribe"' in conn.sent[0]
    assert st.last_msg_ns is not None


async def test_spotfeed_binance_no_subscribe_needed() -> None:
    from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker

    frame = orjson.dumps({
        "e": "trade", "T": 1_700_000_000_000, "s": "BTCUSDT", "p": "60000", "q": "0.01",
    })
    conn = FakeConn([frame])
    clock = ManualClock(0)
    st = StalenessTracker(name="binance", clock=clock, threshold_ms=2000)

    async def connect(url: str) -> FakeConn:
        return conn

    feed = SpotFeed(
        name="binance",
        ws_url="wss://stream.binance.com:9443/ws/btcusdt@trade",
        clock=clock,
        ws_connect=connect,  # type: ignore[arg-type]
        staleness=st,
        parse=binance_parser(clock),
        subscribe=None,
    )

    async for tick in feed.events():
        assert tick.venue == "binance"
        break
    assert conn.sent == []
