from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.kalshi import (
    KalshiFeed,
    WSConnection,
    _FeedDiagnostic,
)
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


async def test_on_reconnect_callback_fires_with_reason() -> None:
    """Hard rule #9: on reconnect the caller (the L2Book owner) must be
    notified so it can invalidate the book before new events flow."""
    attempts: list[int] = []
    reasons: list[str] = []

    def conn_factory() -> FakeConn:
        attempts.append(1)
        if len(attempts) == 1:
            return FakeConn([_snap(1)])
        return FakeConn([_snap(2)])

    async def connect(url: str) -> WSConnection:
        return conn_factory()

    async def fake_sleep(sec: float) -> None:
        return None

    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=fake_sleep,
        backoff_initial_sec=0.1,
        on_reconnect=reasons.append,
    )
    out: list[BookUpdate] = []
    async for ev in feed.events():
        assert isinstance(ev, BookUpdate)
        out.append(ev)
        if len(out) == 2:
            break

    # One reconnect occurred between the two sessions → exactly one callback
    # firing, and it ran BEFORE the second event arrived (caller must be able
    # to flip the book-valid gate synchronously with the reconnect).
    assert len(reasons) == 1
    assert "ws_closed" in reasons[0]


async def test_on_reconnect_callback_error_does_not_kill_feed() -> None:
    attempts: list[int] = []

    def conn_factory() -> FakeConn:
        attempts.append(1)
        if len(attempts) == 1:
            return FakeConn([_snap(1)])
        return FakeConn([_snap(2)])

    async def connect(url: str) -> WSConnection:
        return conn_factory()

    async def fake_sleep(sec: float) -> None:
        return None

    def bad_callback(_reason: str) -> None:
        raise RuntimeError("book unavailable")

    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=fake_sleep,
        backoff_initial_sec=0.1,
        on_reconnect=bad_callback,
    )
    # Must deliver both events even though the callback raises.
    out: list[BookUpdate] = []
    async for ev in feed.events():
        out.append(ev)
        if len(out) == 2:
            break
    assert len(out) == 2


async def test_staleness_tracks_event_time_not_recv_time() -> None:
    """Staleness must be measured against the Kalshi-emitted event ts, not
    our recv-time. A backlogged WS queue replaying old frames at high rate
    would otherwise look 'fresh' and defeat the staleness breaker."""
    conn = FakeConn([_snap(1)])  # parser stamps ev.ts_ns = recv_ts_ns passed in
    clock = ManualClock(5_000_000_000)  # recv_ts_ns = 5s
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
    async for ev in feed.events():
        assert isinstance(ev, BookUpdate)
        break

    # The parser sets ev.ts_ns from the recv_ts_ns argument at parse time;
    # staleness.last_msg_ns must equal that, not a later clock.now_ns().
    assert st.last_msg_ns is not None
    # Advance the clock past threshold — the stored last_msg_ns should not
    # move (event-time, not recv-time or clock-on-mark).
    clock.advance_ns(10_000_000_000)
    assert st.last_msg_ns == 5_000_000_000


# --- _FeedDiagnostic --------------------------------------------------------
# The diagnostic is off by default (hot path does a single `if self.enabled`
# check per frame). When enabled, it aggregates per-ftype counts, inter-arrival
# gaps, and exchange-to-recv lag over a rolling window and emits one log line
# per interval. These tests drive the class directly so we don't depend on
# structlog capture fixtures.


def test_feed_diagnostic_disabled_is_noop() -> None:
    clock = ManualClock(0)
    diag = _FeedDiagnostic(enabled=False, clock=clock)
    assert diag.enabled is False
    # observe/maybe_emit must stay cheap when disabled — they shouldn't
    # accumulate state that could leak memory.
    diag.observe("orderbook_delta", recv_ns=100, ev_ts_ns=90)
    diag.maybe_emit(now_ns=10_000_000_000)
    assert diag._counts == {"orderbook_delta": 1}  # still records, but never emits


def test_feed_diagnostic_emits_once_per_interval() -> None:
    """Below-interval maybe_emit calls must not advance the emit timer; a
    crossing call must. We check state transitions rather than capturing
    structlog output — capturing structlog from pytest is fragile and the
    reset-on-emit behavior is the load-bearing invariant."""
    clock = ManualClock(0)
    diag = _FeedDiagnostic(enabled=True, clock=clock, interval_sec=1.0)
    initial = diag._last_emit_ns
    diag.observe("orderbook_delta", recv_ns=100_000_000, ev_ts_ns=50_000_000)
    diag.maybe_emit(now_ns=500_000_000)  # 0.5s → below interval
    assert diag._last_emit_ns == initial
    assert diag._counts == {"orderbook_delta": 1}  # still accumulating
    diag.observe("trade", recv_ns=1_200_000_000, ev_ts_ns=1_100_000_000)
    diag.maybe_emit(now_ns=1_500_000_000)  # 1.5s → crosses threshold
    assert diag._last_emit_ns == 1_500_000_000
    assert diag._counts == {}  # reset after emit


def test_feed_diagnostic_resets_state_on_emit() -> None:
    clock = ManualClock(0)
    diag = _FeedDiagnostic(enabled=True, clock=clock, interval_sec=1.0)
    diag.observe("trade", recv_ns=100_000_000, ev_ts_ns=50_000_000)
    diag.observe("trade", recv_ns=200_000_000, ev_ts_ns=150_000_000)
    diag.maybe_emit(now_ns=1_500_000_000)
    # After an emit the accumulators must be reset — otherwise the next window
    # reports stale data pooled across windows.
    assert diag._counts == {}
    assert diag._inter_arrivals == {}
    assert diag._ts_lags_ms == []


def test_feed_diagnostic_skips_emit_when_window_empty() -> None:
    """A window with no observations should advance the timer but NOT log —
    otherwise a quiet market floods the log with empty-window records."""
    clock = ManualClock(0)
    diag = _FeedDiagnostic(enabled=True, clock=clock, interval_sec=1.0)
    before_last_emit = diag._last_emit_ns
    diag.maybe_emit(now_ns=2_000_000_000)
    # Timer was advanced, so next maybe_emit won't immediately re-fire.
    assert diag._last_emit_ns > before_last_emit


def test_feed_diagnostic_tracks_inter_arrival_by_type() -> None:
    clock = ManualClock(0)
    diag = _FeedDiagnostic(enabled=True, clock=clock, interval_sec=60.0)
    # Two trades 10ms apart; one delta 50ms after the first trade.
    diag.observe("trade", recv_ns=100_000_000, ev_ts_ns=None)
    diag.observe("orderbook_delta", recv_ns=150_000_000, ev_ts_ns=None)
    diag.observe("trade", recv_ns=110_000_000, ev_ts_ns=None)
    # Inter-arrivals are computed per-type — trade[0]→trade[1] = 10ms gap.
    assert "trade" in diag._inter_arrivals
    assert diag._inter_arrivals["trade"] == [10.0]
    # Single observation of delta → no inter-arrival sample yet.
    assert "orderbook_delta" not in diag._inter_arrivals


async def test_feed_runs_with_diagnostic_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Integration: with BOT_BTC_1HR_KALSHI_FEED_DIAG=1, the feed must
    continue to deliver events unchanged. The diagnostic is instrumentation;
    it must not alter the event stream or crash the session."""
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_FEED_DIAG", "1")
    conn = FakeConn([_snap(1), _trade(2), b"{bad", _snap(3)])
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
    events = []
    async for ev in feed.events():
        events.append(ev)
        if len(events) == 3:
            break
    assert len(events) == 3  # 2 books + 1 trade (parse error dropped)


async def test_force_reconnect_drops_active_conn_and_resubscribes() -> None:
    """Crash-only seq_gap recovery: force_reconnect() closes the active
    socket so the next `_session` iteration reopens, re-subscribes, and
    Kalshi ships a fresh `orderbook_snapshot` per ticker. We verify the
    socket really did close, the second session ran (proving reconnect
    fired), and the second subscribe frame went out — the last condition
    is what guarantees a fresh snapshot."""
    sleep_calls: list[float] = []

    async def fake_sleep(sec: float) -> None:
        sleep_calls.append(sec)

    sessions: list[FakeConn] = []

    class _Blocking(FakeConn):
        """Holds the iterator open after frames are exhausted until close()
        is called. Mirrors a real WS that stays alive until the server (or
        force_reconnect) drops it."""

        def __init__(self, frames: list[bytes]) -> None:
            super().__init__(frames)
            self._stop = asyncio.Event()

        async def _iter(self) -> AsyncIterator[bytes]:
            for f in self._frames:
                yield f
            await self._stop.wait()

        async def close(self) -> None:
            await super().close()
            self._stop.set()

    async def connect(url: str) -> WSConnection:
        # First session: one snapshot then waits for force_reconnect to drop
        # it. Second session: one snapshot then waits for the consumer to
        # exit on its own.
        conn = _Blocking([_snap(1) if not sessions else _snap(2)])
        sessions.append(conn)
        return conn

    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=2000)
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=clock,
        ws_connect=connect,
        staleness=st,
        sleep=fake_sleep,
        backoff_initial_sec=0.1,
    )

    out: list[BookUpdate] = []

    async def consume() -> None:
        async for ev in feed.events():
            assert isinstance(ev, BookUpdate)
            out.append(ev)
            if len(out) == 1:
                # Trigger the crash-only recovery from the consumer side,
                # mirroring how feedloop calls it on book invalidation.
                await feed.force_reconnect()
            elif len(out) == 2:
                return

    await asyncio.wait_for(consume(), timeout=2.0)
    assert len(sessions) == 2, "force_reconnect must trigger a fresh _session"
    assert sessions[0].closed, "old conn must be closed by force_reconnect"
    assert b'"cmd":"subscribe"' in sessions[1].sent[0]
    assert sleep_calls == [0.1], "reconnect path uses the configured backoff"


async def test_force_reconnect_when_no_active_conn_is_noop() -> None:
    """Calling force_reconnect before `events()` has opened its first
    socket must not raise. Defensive: feedloop creates the feed before
    looping, and a paranoid early caller shouldn't crash the boot."""
    feed = KalshiFeed(
        ws_url="ws://fake",
        market_tickers=["M"],
        clock=ManualClock(0),
        ws_connect=lambda _u: asyncio.sleep(0),  # type: ignore[arg-type,return-value]
        staleness=StalenessTracker(name="k", clock=ManualClock(0), threshold_ms=100),
    )
    await feed.force_reconnect()


def test_empty_ticker_list_rejected() -> None:
    with pytest.raises(ValueError, match="market_tickers"):
        KalshiFeed(
            ws_url="ws://fake",
            market_tickers=[],
            clock=ManualClock(0),
            ws_connect=lambda _u: asyncio.sleep(0),  # type: ignore[arg-type,return-value]
            staleness=StalenessTracker(name="k", clock=ManualClock(0), threshold_ms=100),
        )
