from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.kalshi import KalshiFeed, WSConnection
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.types import BookUpdate, TradeEvent
from bot_btc_1hr_kalshi.obs.clock import ManualClock


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


def _snap(seq: int) -> bytes:
    return orjson.dumps({
        "type": "orderbook_snapshot",
        "msg": {"market_ticker": "M", "seq": seq, "yes": [[40, 100]], "no": [[55, 100]]},
    })


def _trade(seq: int) -> bytes:
    return orjson.dumps({
        "type": "trade",
        "msg": {"market_ticker": "M", "yes_price": 40, "count": 1, "taker_side": "yes", "seq": seq},
    })


async def test_subscribes_and_yields_parsed_events() -> None:
    conn = FakeConn([_snap(1), _trade(2)])
    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)

    async def connect(url: str) -> WSConnection:
        assert url == "ws://fake"
        return conn

    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
    )

    events = []
    async for ev in feed.events():
        events.append(ev)
        if len(events) == 2:
            break

    assert isinstance(events[0], BookUpdate)
    assert isinstance(events[1], TradeEvent)
    assert conn.sent and b'"cmd":"subscribe"' in conn.sent[0]
    assert st.last_msg_ns is not None


async def test_parse_errors_are_logged_not_fatal() -> None:
    conn = FakeConn([b"{not json", _snap(1)])
    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)

    async def connect(url: str) -> WSConnection:
        return conn

    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
    )

    first = await feed.events().__anext__()
    assert isinstance(first, BookUpdate)


async def test_reconnects_with_backoff_after_close() -> None:
    attempts: list[int] = []
    sleep_calls: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleep_calls.append(sec)

    def conn_factory() -> FakeConn:
        attempts.append(1)
        # First session delivers one frame then closes; second delivers another.
        if len(attempts) == 1:
            return FakeConn([_snap(1)])
        return FakeConn([_snap(2)])

    async def connect(url: str) -> WSConnection:
        return conn_factory()

    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=fake_sleep,
        backoff_initial_sec=0.5,
    )
    out: list[BookUpdate] = []
    async for ev in feed.events():
        assert isinstance(ev, BookUpdate)
        out.append(ev)
        if len(out) == 2:
            break
    assert len(attempts) == 2
    assert sleep_calls == [0.5]


async def test_connect_failure_triggers_backoff_then_retries() -> None:
    attempts = {"n": 0}
    sleep_calls: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleep_calls.append(sec)

    async def connect(url: str) -> WSConnection:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise ConnectionRefusedError("nope")
        return FakeConn([_snap(1)])

    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=fake_sleep,
        backoff_initial_sec=0.25,
    )
    out: list[BookUpdate] = []
    async for ev in feed.events():
        assert isinstance(ev, BookUpdate)
        out.append(ev)
        break
    assert attempts["n"] == 2
    assert sleep_calls == [0.25]


def test_empty_ticker_list_rejected() -> None:
    with pytest.raises(ValueError, match="market_tickers"):
        KalshiFeed(
            ws_url="ws://fake",
            market_tickers=[],
            clock=ManualClock(0),
            ws_connect=lambda _u: asyncio.sleep(0),  # type: ignore[arg-type,return-value]
            staleness=StalenessTracker(name="k", clock=ManualClock(0), threshold_ms=100),
        )
