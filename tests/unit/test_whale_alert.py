"""Tests for the Whale Alert poller (Slice 11 P4 shadow)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.whale_alert import (
    WHALE_ALERT_API_KEY_PARAM,
    WhaleAlertParseError,
    WhaleAlertPoller,
    fetch_whale_alert,
    parse_whale_alert_response,
)
from bot_btc_1hr_kalshi.market_data.types import WhaleAlertSample
from bot_btc_1hr_kalshi.obs.clock import ManualClock

FIXTURE = Path(__file__).parent.parent / "fixtures" / "whale_alert_sample.json"


def _fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


def test_parse_summary_stats_from_fixture() -> None:
    """Fixture has 5 transactions:

    * $30M unknown -> exchange (+$30M inflow)
    * $7.2M exchange -> unknown (-$7.2M outflow)
    * $3M exchange -> exchange (skipped, internal rotation)
    * $1.5M unknown -> personal (counted, no flow movement)
    * $12M unknown -> exchange (+$12M inflow)

    Expected: net = +$34.8M, largest = $30M, count = 4.
    """
    sample = parse_whale_alert_response(
        _fixture_bytes(),
        symbol="btc",
        window_sec=60.0,
        clock=ManualClock(start_ns=555_000_000_000),
    )
    assert sample.symbol == "btc"
    assert sample.source == "whale_alert"
    assert sample.window_sec == pytest.approx(60.0)
    assert sample.net_exchange_flow_usd == pytest.approx(34_800_000.0)
    assert sample.largest_txn_usd == pytest.approx(30_000_000.0)
    assert sample.txn_count == 4
    assert sample.ts_ns == 555_000_000_000


def test_parse_rejects_failure_result() -> None:
    payload = orjson.dumps({"result": "error", "message": "invalid api key"})
    with pytest.raises(WhaleAlertParseError, match="result="):
        parse_whale_alert_response(
            payload, symbol="btc", window_sec=60.0, clock=ManualClock()
        )


def test_parse_rejects_missing_transactions_list() -> None:
    payload = orjson.dumps({"result": "success", "count": 0})
    with pytest.raises(WhaleAlertParseError, match="transactions"):
        parse_whale_alert_response(
            payload, symbol="btc", window_sec=60.0, clock=ManualClock()
        )


def test_parse_empty_transactions_produces_zero_summary() -> None:
    """An empty window is not an error — it just means no whale prints
    crossed the threshold in that poll. Zeros are a real observation."""
    payload = orjson.dumps(
        {"result": "success", "count": 0, "transactions": []}
    )
    sample = parse_whale_alert_response(
        payload, symbol="btc", window_sec=60.0, clock=ManualClock()
    )
    assert sample.net_exchange_flow_usd == 0.0
    assert sample.largest_txn_usd == 0.0
    assert sample.txn_count == 0


def test_parse_skips_malformed_transactions() -> None:
    """Defensive: a row missing `amount_usd` or with a non-numeric one
    must not crash the parser; real v1 payloads occasionally ship odd
    rows and we want the rest of the window intact."""
    payload = orjson.dumps(
        {
            "result": "success",
            "count": 3,
            "transactions": [
                {
                    "amount_usd": "not-a-number",
                    "from": {"owner_type": "unknown"},
                    "to": {"owner_type": "exchange"},
                },
                {
                    # Missing amount entirely.
                    "from": {"owner_type": "unknown"},
                    "to": {"owner_type": "exchange"},
                },
                {
                    "amount_usd": 5_000_000.0,
                    "from": {"owner_type": "unknown"},
                    "to": {"owner_type": "exchange"},
                },
            ],
        }
    )
    sample = parse_whale_alert_response(
        payload, symbol="btc", window_sec=60.0, clock=ManualClock()
    )
    assert sample.net_exchange_flow_usd == pytest.approx(5_000_000.0)
    assert sample.txn_count == 1


def test_parse_rejects_non_object_envelope() -> None:
    with pytest.raises(WhaleAlertParseError, match="expected JSON object"):
        parse_whale_alert_response(
            b"[]", symbol="btc", window_sec=60.0, clock=ManualClock()
        )


async def test_fetch_passes_api_key_as_query_param() -> None:
    """Whale Alert v1 takes the key as `api_key=...` query arg (not a
    header). The test is the canary for any refactor that tries to
    'standardize' it to the Coinglass header pattern."""
    captured: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.url.params.get(WHALE_ALERT_API_KEY_PARAM))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await fetch_whale_alert(
        client=client,
        api_key="secret-xyz",
        start_ts_sec=1_700_000_000,
        base_url="https://example.test",
        path="/v1/transactions",
        window_sec=60.0,
        clock=ManualClock(),
    )
    assert captured == ["secret-xyz"]


async def test_fetch_propagates_http_errors() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b'{"result":"error"}')

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPError):
        await fetch_whale_alert(
            client=client,
            api_key="bad",
            start_ts_sec=1_700_000_000,
            base_url="https://example.test",
            path="/v1/transactions",
            window_sec=60.0,
            clock=ManualClock(),
        )


async def test_poller_publishes_on_success_and_advances_window() -> None:
    received: list[WhaleAlertSample] = []

    async def on_sample(sample: WhaleAlertSample) -> None:
        received.append(sample)

    request_starts: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        request_starts.append(request.url.params.get("start"))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    clock = ManualClock(start_ns=1_700_000_000_000_000_000)
    poller = WhaleAlertPoller(
        client=client,
        clock=clock,
        on_sample=on_sample,
        api_key="k",
        base_url="https://example.test",
        path="/v1/transactions",
        poll_interval_sec=60.0,
    )
    first = await poller.poll_once()
    assert first is not None
    assert len(received) == 1
    # Cold-start start = now - poll_interval.
    assert request_starts[0] == str(1_700_000_000 - 60)

    # Advance the clock; next successful poll should use the updated
    # second-precision floor rather than re-requesting the original
    # window (which would double-count prints still in Whale Alert's
    # server-side buffer).
    clock.advance_ns(30 * 1_000_000_000)
    await poller.poll_once()
    assert request_starts[1] == str(1_700_000_000)


async def test_poller_swallows_fetch_errors() -> None:
    received: list[WhaleAlertSample] = []

    async def on_sample(sample: WhaleAlertSample) -> None:
        received.append(sample)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = WhaleAlertPoller(
        client=client,
        clock=ManualClock(),
        on_sample=on_sample,
        api_key="k",
        base_url="https://example.test",
        path="/v1/transactions",
        poll_interval_sec=60.0,
    )
    assert await poller.poll_once() is None
    assert received == []


async def test_poller_does_not_advance_window_on_failure() -> None:
    """Transient errors must leave the start bound where it was so the
    next retry still covers the missed window — otherwise a single
    hiccup would permanently hide whatever happened during it."""
    request_starts: list[str | None] = []

    async def noop(sample: WhaleAlertSample) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        request_starts.append(request.url.params.get("start"))
        return httpx.Response(503, content=b"")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    clock = ManualClock(start_ns=1_700_000_000_000_000_000)
    poller = WhaleAlertPoller(
        client=client,
        clock=clock,
        on_sample=noop,
        api_key="k",
        base_url="https://example.test",
        path="/v1/transactions",
        poll_interval_sec=60.0,
    )
    await poller.poll_once()
    clock.advance_ns(30 * 1_000_000_000)
    await poller.poll_once()
    # Both polls use the same start — the second didn't advance.
    assert request_starts[0] == request_starts[1]


async def test_poller_rejects_missing_key() -> None:
    async def noop(sample: WhaleAlertSample) -> None:
        return None

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b""))
    )
    with pytest.raises(ValueError, match="api key"):
        WhaleAlertPoller(
            client=client,
            clock=ManualClock(),
            on_sample=noop,
            api_key="",
        )


async def test_poller_rejects_nonpositive_interval() -> None:
    async def noop(sample: WhaleAlertSample) -> None:
        return None

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b""))
    )
    with pytest.raises(ValueError, match="poll_interval_sec"):
        WhaleAlertPoller(
            client=client,
            clock=ManualClock(),
            on_sample=noop,
            api_key="k",
            poll_interval_sec=0.0,
        )


async def test_poller_run_loop_is_cancellable() -> None:
    async def noop(sample: WhaleAlertSample) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = WhaleAlertPoller(
        client=client,
        clock=ManualClock(),
        on_sample=noop,
        api_key="k",
        base_url="https://example.test",
        path="/v1/transactions",
        poll_interval_sec=3600.0,
    )
    task = asyncio.create_task(poller.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
