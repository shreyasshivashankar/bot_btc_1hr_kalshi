from __future__ import annotations

from collections.abc import AsyncIterator

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    SpotParseError,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
    parse_coinbase,
    parse_kraken,
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


def test_kraken_trade_update_parses() -> None:
    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [
            {
                "symbol": "BTC/USD",
                "side": "buy",
                "price": 60500.1,
                "qty": 0.003,
                "ord_type": "limit",
                "trade_id": 12345,
                "timestamp": "2026-04-17T04:00:00.500Z",
            },
        ],
    })
    tick = parse_kraken(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.venue == "kraken"
    assert tick.price_usd == pytest.approx(60500.1)
    assert tick.size == pytest.approx(0.003)
    assert tick.ts_ns > 0


def test_kraken_snapshot_ignored() -> None:
    """Snapshots are Kraken's 'last trade at subscribe time' — intentionally
    discarded so the velocity tracker doesn't latch onto stale prints."""
    frame = orjson.dumps({
        "channel": "trade",
        "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "price": 1, "qty": 1}],
    })
    assert parse_kraken(frame, recv_ts_ns=RECV_NS) is None


def test_kraken_subscribe_ack_ignored() -> None:
    frame = orjson.dumps({"method": "subscribe", "success": True, "result": {}})
    assert parse_kraken(frame, recv_ts_ns=RECV_NS) is None


def test_kraken_missing_fields_raises() -> None:
    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{"symbol": "BTC/USD"}],
    })
    with pytest.raises(SpotParseError):
        parse_kraken(frame, recv_ts_ns=RECV_NS)


def test_kraken_bad_timestamp_falls_back_to_recv() -> None:
    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{"symbol": "BTC/USD", "price": 1, "qty": 1, "timestamp": "bad"}],
    })
    tick = parse_kraken(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.ts_ns == RECV_NS


def test_build_coinbase_subscribe_shape() -> None:
    raw = build_coinbase_subscribe(["BTC-USD"])
    data = orjson.loads(raw)
    assert data == {"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["ticker"]}


def test_build_kraken_subscribe_shape() -> None:
    raw = build_kraken_subscribe(["BTC/USD"])
    data = orjson.loads(raw)
    assert data == {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": ["BTC/USD"]},
    }


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


async def test_spotfeed_kraken_sends_v2_subscribe() -> None:
    from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker

    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD", "price": 60000, "qty": 0.01,
            "timestamp": "2026-04-17T04:00:00Z",
        }],
    })
    conn = FakeConn([frame])
    clock = ManualClock(0)
    st = StalenessTracker(name="kraken", clock=clock, threshold_ms=5000)

    async def connect(url: str) -> FakeConn:
        return conn

    feed = SpotFeed(
        name="kraken",
        ws_url="wss://ws.kraken.com/v2",
        clock=clock,
        ws_connect=connect,  # type: ignore[arg-type]
        staleness=st,
        parse=kraken_parser(clock),
        subscribe=build_kraken_subscribe(["BTC/USD"]),
    )

    async for tick in feed.events():
        assert tick.venue == "kraken"
        break
    assert conn.sent and b'"subscribe"' in conn.sent[0]
