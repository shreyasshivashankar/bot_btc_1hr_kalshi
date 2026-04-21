"""Tests for the Coinglass OI poller (Slice 11 P2 shadow)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.coinglass import (
    CoinglassParseError,
    CoinglassPoller,
    fetch_coinglass_oi,
    parse_coinglass_response,
)
from bot_btc_1hr_kalshi.market_data.types import OpenInterestSample
from bot_btc_1hr_kalshi.obs.clock import ManualClock

FIXTURE = Path(__file__).parent.parent / "fixtures" / "coinglass_oi_sample.json"


def _fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


def test_parse_picks_latest_data_row() -> None:
    """The fixture has two data rows; the latest (index -1) must win.
    Polling is faster than the candle cadence, so the newest row is the
    one we want to observe."""
    clock = ManualClock(start_ns=123_000_000_000)
    sample = parse_coinglass_response(_fixture_bytes(), symbol="BTC", clock=clock)
    assert sample.symbol == "BTC"
    assert sample.source == "coinglass"
    # Latest row's USD OI is 58.3B; the earlier row was 58.23B.
    assert sample.total_oi_usd == pytest.approx(58_300_123_456.78)
    # `time` column is ms epoch → ns.
    assert sample.ts_ns == 1_745_237_100_000 * 1_000_000
    assert sample.exchanges_count == 18


def test_parse_uses_clock_when_time_field_missing() -> None:
    payload = orjson.dumps(
        {
            "code": "0",
            "data": [{"aggregated_open_interest_usd": 42.0}],
        }
    )
    clock = ManualClock(start_ns=555_000_000_000)
    sample = parse_coinglass_response(payload, symbol="BTC", clock=clock)
    assert sample.ts_ns == 555_000_000_000
    assert sample.total_oi_usd == 42.0


def test_parse_accepts_code_zero_as_int() -> None:
    """Some SDKs ship `code` as a bare integer; accept both forms so a
    server-side cleanup doesn't break the parser overnight."""
    payload = orjson.dumps(
        {
            "code": 0,
            "data": [{"open_interest_usd": 1.0}],
        }
    )
    clock = ManualClock()
    sample = parse_coinglass_response(payload, symbol="BTC", clock=clock)
    assert sample.total_oi_usd == 1.0


def test_parse_rejects_non_zero_code() -> None:
    payload = orjson.dumps({"code": "30001", "msg": "Invalid API key", "data": []})
    with pytest.raises(CoinglassParseError, match="code="):
        parse_coinglass_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_missing_data() -> None:
    payload = orjson.dumps({"code": "0"})
    with pytest.raises(CoinglassParseError, match="data"):
        parse_coinglass_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_empty_data() -> None:
    payload = orjson.dumps({"code": "0", "data": []})
    with pytest.raises(CoinglassParseError, match="missing or empty"):
        parse_coinglass_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_non_object_top_level() -> None:
    with pytest.raises(CoinglassParseError, match="JSON object"):
        parse_coinglass_response(b"[]", symbol="BTC", clock=ManualClock())


def test_parse_rejects_unknown_oi_field() -> None:
    """If the API renames its payload fields, we must fail loudly instead
    of silently reporting 0 OI — zero would look like a catastrophic OI
    flush to a downstream reader."""
    payload = orjson.dumps(
        {
            "code": "0",
            "data": [{"some_new_field_name": 1.0}],
        }
    )
    with pytest.raises(CoinglassParseError, match="no recognized"):
        parse_coinglass_response(payload, symbol="BTC", clock=ManualClock())


async def test_fetch_sends_api_key_header_when_provided() -> None:
    captured: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.headers.get("CG-API-KEY"))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await fetch_coinglass_oi(
        client=client,
        base_url="https://example.test",
        path="/oi",
        api_key="secret-abc",
        clock=ManualClock(),
    )
    assert captured == ["secret-abc"]


async def test_fetch_omits_header_when_no_api_key() -> None:
    """Free-tier path: no `CG-API-KEY` header on the outbound request.
    Sending a blank key triggers a 401 on the keyed tier — omit entirely."""
    captured: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.headers.get("CG-API-KEY"))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await fetch_coinglass_oi(
        client=client,
        base_url="https://example.test",
        path="/oi",
        api_key=None,
        clock=ManualClock(),
    )
    assert captured == [None]


async def test_fetch_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"upstream busy")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_coinglass_oi(
            client=client,
            base_url="https://example.test",
            path="/oi",
            clock=ManualClock(),
        )


async def test_poller_publishes_on_success() -> None:
    received: list[OpenInterestSample] = []

    async def on_sample(sample: OpenInterestSample) -> None:
        received.append(sample)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassPoller(
        client=client,
        clock=ManualClock(start_ns=1_000_000_000_000),
        on_sample=on_sample,
        base_url="https://example.test",
        oi_path="/oi",
        poll_interval_sec=30.0,
    )
    result = await poller.poll_once()
    assert result is not None
    assert result.total_oi_usd == pytest.approx(58_300_123_456.78)
    assert len(received) == 1


async def test_poller_swallows_fetch_errors() -> None:
    received: list[OpenInterestSample] = []

    async def on_sample(sample: OpenInterestSample) -> None:
        received.append(sample)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassPoller(
        client=client,
        clock=ManualClock(),
        on_sample=on_sample,
        base_url="https://example.test",
        oi_path="/oi",
        poll_interval_sec=30.0,
    )
    assert await poller.poll_once() is None
    assert received == []


async def test_poller_swallows_coinglass_code_errors() -> None:
    """An API-level error (non-zero code) must NOT crash the loop — the
    caller may keep polling in case the rate-limit window resets."""

    async def noop(sample: OpenInterestSample) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=orjson.dumps({"code": "40001", "msg": "rate limited", "data": []}),
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassPoller(
        client=client,
        clock=ManualClock(),
        on_sample=noop,
        base_url="https://example.test",
        oi_path="/oi",
        poll_interval_sec=30.0,
    )
    assert await poller.poll_once() is None


async def test_poller_rejects_nonpositive_interval() -> None:
    async def noop(sample: OpenInterestSample) -> None:
        return None

    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b"")))
    with pytest.raises(ValueError, match="poll_interval_sec"):
        CoinglassPoller(
            client=client,
            clock=ManualClock(),
            on_sample=noop,
            poll_interval_sec=0.0,
        )


async def test_poller_run_loop_is_cancellable() -> None:
    async def noop(sample: OpenInterestSample) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassPoller(
        client=client,
        clock=ManualClock(),
        on_sample=noop,
        base_url="https://example.test",
        oi_path="/oi",
        poll_interval_sec=3600.0,
    )
    task = asyncio.create_task(poller.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
