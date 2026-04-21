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
    # No side field in the frame → aggressor is None (not inferable).
    assert tick.aggressor is None


def test_coinbase_ticker_inverts_side_to_taker_aggressor() -> None:
    # Coinbase ticker `side` is the MAKER side. side=="sell" means a resting
    # sell was lifted → taker (aggressor) was a BUY. The parser must invert.
    uptick = orjson.dumps({
        "type": "ticker", "product_id": "BTC-USD",
        "price": "60000", "last_size": "0.1", "side": "sell",
        "time": "2026-04-17T04:00:00Z",
    })
    downtick = orjson.dumps({
        "type": "ticker", "product_id": "BTC-USD",
        "price": "60000", "last_size": "0.1", "side": "buy",
        "time": "2026-04-17T04:00:00Z",
    })
    up = parse_coinbase(uptick, recv_ts_ns=RECV_NS)
    down = parse_coinbase(downtick, recv_ts_ns=RECV_NS)
    assert up is not None and up.aggressor == "buy"
    assert down is not None and down.aggressor == "sell"


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
    # Kraken V2 reports the taker side directly — no inversion.
    assert tick.aggressor == "buy"


def test_kraken_trade_update_side_passed_through_sell() -> None:
    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{"symbol": "BTC/USD", "side": "sell", "price": 60000, "qty": 0.05}],
    })
    tick = parse_kraken(frame, recv_ts_ns=RECV_NS)
    assert tick is not None and tick.aggressor == "sell"


def test_kraken_trade_update_missing_side_yields_none_aggressor() -> None:
    frame = orjson.dumps({
        "channel": "trade",
        "type": "update",
        "data": [{"symbol": "BTC/USD", "price": 60000, "qty": 0.05}],
    })
    tick = parse_kraken(frame, recv_ts_ns=RECV_NS)
    assert tick is not None and tick.aggressor is None


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


def test_kraken_ticker_update_parses_as_liveness_tick() -> None:
    """Ticker frames are the confirmation-liveness workhorse — they arrive
    on every top-of-book change. We use `last` as the tick price, stamp
    recv-time (Kraken ticker has no per-update timestamp), and emit with
    size=0 / aggressor=None so CVD accumulators correctly ignore them."""
    frame = orjson.dumps({
        "channel": "ticker",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "bid": 60000.1, "bid_qty": 1.5,
            "ask": 60000.5, "ask_qty": 2.0,
            "last": 60000.3,
            "volume": 1234.5,
        }],
    })
    tick = parse_kraken(frame, recv_ts_ns=RECV_NS)
    assert isinstance(tick, SpotTick)
    assert tick.venue == "kraken"
    assert tick.ts_ns == RECV_NS
    assert tick.size == 0.0
    assert tick.aggressor is None


def test_kraken_ticker_snapshot_ignored() -> None:
    """Like trade snapshots, ticker snapshots are subscribe-time state and
    shouldn't latch the velocity tracker onto a pre-subscribe price."""
    frame = orjson.dumps({
        "channel": "ticker",
        "type": "snapshot",
        "data": [{"symbol": "BTC/USD", "last": 60000, "bid": 59999, "ask": 60001}],
    })
    assert parse_kraken(frame, recv_ts_ns=RECV_NS) is None


def test_kraken_ticker_missing_last_raises() -> None:
    frame = orjson.dumps({
        "channel": "ticker",
        "type": "update",
        "data": [{"symbol": "BTC/USD", "bid": 60000, "ask": 60001}],
    })
    with pytest.raises(SpotParseError):
        parse_kraken(frame, recv_ts_ns=RECV_NS)


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
    """Kraken V2 subscribes one channel per frame, so the helper returns a
    tuple: the trade frame (CVD-bearing taker info) followed by the ticker
    frame (reliable confirmation liveness)."""
    frames = build_kraken_subscribe(["BTC/USD"])
    assert isinstance(frames, tuple)
    assert len(frames) == 2
    assert orjson.loads(frames[0]) == {
        "method": "subscribe",
        "params": {"channel": "trade", "symbol": ["BTC/USD"]},
    }
    assert orjson.loads(frames[1]) == {
        "method": "subscribe",
        "params": {"channel": "ticker", "symbol": ["BTC/USD"]},
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
    # Both trade + ticker subscriptions must hit the wire — ticker is the
    # one that keeps the integrity gate's confirmation liveness fresh.
    assert len(conn.sent) == 2
    assert b'"channel":"trade"' in conn.sent[0]
    assert b'"channel":"ticker"' in conn.sent[1]
