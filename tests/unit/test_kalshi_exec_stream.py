"""Tests for the Kalshi private-channel WS stream lifecycle.

Reuses the FakeConn pattern from test_kalshi_feed.py so transport
behavior is exercised without a real WS server. Focus areas:

  * subscribe frame is sent on connect with the right channels
  * frames dispatch to the correct callback list
  * a callback raising does not break dispatch for other callbacks
  * parse errors are logged-and-skipped (do not crash the session)
  * reconnect with backoff after a session ends
  * staleness tracker is marked on every received frame
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable

import orjson
import pytest

from bot_btc_1hr_kalshi.execution.ws.stream import KalshiExecutionStream
from bot_btc_1hr_kalshi.execution.ws.types import (
    ExecFillEvent,
    ExecOrderUpdate,
    ExecPositionSnapshot,
)
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import WSConnection
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
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


def _fill_frame(order_id: str, contracts: int) -> bytes:
    return orjson.dumps({
        "type": "fill",
        "msg": {
            "trade_id": f"t-{order_id}",
            "order_id": order_id,
            "client_order_id": f"c-{order_id}",
            "market_ticker": "M",
            "is_taker": True,
            "side": "yes",
            "yes_price": 40,
            "no_price": 60,
            "count": contracts,
            "action": "buy",
            "ts": 1_700_000_000,
        },
    })


def _order_frame(order_id: str, status: str, remaining: int) -> bytes:
    return orjson.dumps({
        "type": "order_update",
        "msg": {
            "order_id": order_id,
            "client_order_id": f"c-{order_id}",
            "market_ticker": "M",
            "side": "yes",
            "status": status,
            "count": 5,
            "remaining_count": remaining,
            "yes_price": 40,
            "no_price": 60,
        },
    })


def _position_frame(signed: int) -> bytes:
    return orjson.dumps({
        "type": "market_position",
        "msg": {
            "market_ticker": "M",
            "position": signed,
            "market_exposure": abs(signed) * 40,
        },
    })


def _one_shot_connect(
    first_frames: list[bytes],
) -> tuple[list[FakeConn], Callable[[str], Awaitable[WSConnection]]]:
    """Build a connect factory that returns a populated FakeConn once and
    empty FakeConns on every reconnect after that. Returns (sessions, fn)
    so tests can introspect what was sent on the first session."""
    sessions: list[FakeConn] = []

    async def connect(url: str) -> WSConnection:
        c = FakeConn(first_frames if not sessions else [])
        sessions.append(c)
        return c

    return sessions, connect


@pytest.mark.asyncio
async def test_sends_subscribe_and_dispatches_to_typed_callbacks() -> None:
    sessions, connect = _one_shot_connect([
        _fill_frame("o-1", 5),
        _order_frame("o-1", "filled", 0),
        _position_frame(5),
    ])
    clock = ManualClock(0)

    fills: list[ExecFillEvent] = []
    orders: list[ExecOrderUpdate] = []
    positions: list[ExecPositionSnapshot] = []
    stream = KalshiExecutionStream(
        ws_url="ws://fake/trade-api/ws/v2",
        clock=clock,
        ws_connect=connect,
        market_tickers=["M"],
        sleep=lambda _: _noop(),
    )
    stream.subscribe_fill(fills.append)
    stream.subscribe_order(orders.append)
    stream.subscribe_position(positions.append)

    task = asyncio.create_task(stream.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if len(fills) >= 1 and len(orders) >= 1 and len(positions) >= 1:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert sessions, "connect was never called"
    sent = sessions[0].sent
    assert sent
    sub = orjson.loads(sent[0])
    assert sub["cmd"] == "subscribe"
    assert sub["params"]["channels"] == ["fill", "user_orders", "market_positions"]
    assert sub["params"]["market_tickers"] == ["M"]

    assert [f.order_id for f in fills] == ["o-1"]
    assert fills[0].contracts == 5
    assert [(o.order_id, o.status) for o in orders] == [("o-1", "filled")]
    assert [(p.contracts, p.side) for p in positions] == [(5, "YES")]


@pytest.mark.asyncio
async def test_callback_exception_does_not_break_dispatch() -> None:
    _sessions, connect = _one_shot_connect([
        _fill_frame("o-1", 1), _fill_frame("o-2", 1),
    ])
    clock = ManualClock(0)

    seen_good: list[ExecFillEvent] = []

    def boom(_: ExecFillEvent) -> None:
        raise RuntimeError("downstream consumer crashed")

    stream = KalshiExecutionStream(
        ws_url="ws://fake",
        clock=clock,
        ws_connect=connect,
        sleep=lambda _: _noop(),
    )
    stream.subscribe_fill(boom)
    stream.subscribe_fill(seen_good.append)

    task = asyncio.create_task(stream.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if len(seen_good) >= 2:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Both fills delivered to the good callback even though the bad one raised.
    assert [f.order_id for f in seen_good] == ["o-1", "o-2"]


@pytest.mark.asyncio
async def test_parse_errors_are_logged_not_fatal() -> None:
    _sessions, connect = _one_shot_connect([b"{not json", _fill_frame("o-1", 1)])
    clock = ManualClock(0)

    fills: list[ExecFillEvent] = []
    stream = KalshiExecutionStream(
        ws_url="ws://fake",
        clock=clock,
        ws_connect=connect,
        sleep=lambda _: _noop(),
    )
    stream.subscribe_fill(fills.append)

    task = asyncio.create_task(stream.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if len(fills) >= 1:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Parse error skipped; the good frame still dispatched.
    assert len(fills) == 1
    assert fills[0].order_id == "o-1"


@pytest.mark.asyncio
async def test_reconnect_backoff_resets_on_dispatching_session_doubles_on_dead() -> None:
    """The backoff should stay at the floor across sessions that delivered
    frames (a healthy WS that gets cycled), and only ramp when sessions
    return zero events (a dead/broken endpoint)."""
    sleeps: list[float] = []
    sessions: list[FakeConn] = []

    async def fake_sleep(sec: float) -> None:
        sleeps.append(sec)
        await asyncio.sleep(0)

    async def connect(url: str) -> WSConnection:
        # Sessions 0 & 1 dispatch a frame → backoff should stay at 0.1.
        # Sessions 2+ are dead (no frames) → backoff doubles each iter.
        if len(sessions) == 0:
            c = FakeConn([_fill_frame("a", 1)])
        elif len(sessions) == 1:
            c = FakeConn([_fill_frame("b", 1)])
        else:
            c = FakeConn([])
        sessions.append(c)
        return c

    clock = ManualClock(0)
    fills: list[ExecFillEvent] = []
    stream = KalshiExecutionStream(
        ws_url="ws://fake",
        clock=clock,
        ws_connect=connect,
        backoff_initial_sec=0.1,
        backoff_max_sec=0.4,
        sleep=fake_sleep,
    )
    stream.subscribe_fill(fills.append)

    task = asyncio.create_task(stream.run())
    for _ in range(200):
        await asyncio.sleep(0)
        if len(sleeps) >= 5:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert [f.order_id for f in fills][:2] == ["a", "b"]
    # Sleep[0]: after session 0 dispatched → reset → 0.1
    # Sleep[1]: after session 1 dispatched → reset → 0.1
    # Sleep[2]: after session 2 dead → no reset, but sleep is still 0.1
    #          (we sleep BEFORE doubling), then backoff doubles to 0.2
    # Sleep[3]: 0.2 (after session 3 dead) → doubles to 0.4
    # Sleep[4]: 0.4 (capped)
    assert sleeps[:2] == [0.1, 0.1]
    assert sleeps[2] == 0.1
    assert sleeps[3] == 0.2
    assert sleeps[4] == 0.4


@pytest.mark.asyncio
async def test_staleness_marked_on_every_received_frame() -> None:
    _sessions, connect = _one_shot_connect([
        orjson.dumps({"type": "subscribed", "msg": {"channel": "fill"}}),
        _fill_frame("o-1", 1),
    ])
    clock = ManualClock(123_000_000_000)
    st = StalenessTracker(name="exec_ws", clock=clock, threshold_ms=30_000)

    stream = KalshiExecutionStream(
        ws_url="ws://fake",
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=lambda _: _noop(),
    )

    task = asyncio.create_task(stream.run())
    for _ in range(20):
        await asyncio.sleep(0)
        if st.last_msg_ns is not None:
            break
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert st.last_msg_ns is not None
    assert st.last_msg_ns >= 123_000_000_000


async def _noop() -> None:
    """Sleep replacement that yields once and returns. Used in tests where
    the stream's reconnect loop should not actually delay between sessions."""
    await asyncio.sleep(0)
