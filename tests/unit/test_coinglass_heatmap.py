"""Tests for the Coinglass liquidation-heatmap poller (Slice 11 P3 shadow)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.feeds.coinglass_heatmap import (
    CoinglassHeatmapParseError,
    CoinglassHeatmapPoller,
    fetch_coinglass_heatmap,
    parse_coinglass_heatmap_response,
)
from bot_btc_1hr_kalshi.market_data.types import LiquidationHeatmapSample
from bot_btc_1hr_kalshi.obs.clock import ManualClock

FIXTURE = Path(__file__).parent.parent / "fixtures" / "coinglass_heatmap_sample.json"


def _fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


def test_parse_summary_stats_from_fixture() -> None:
    """Fixture contains 5 cells. Total = 1.5+9.8+3.2+0.7+2.1 = 17.3M. The
    peak cluster is cell[1]=9.8M at price index 2 → price $60000."""
    sample = parse_coinglass_heatmap_response(
        _fixture_bytes(), symbol="BTC", clock=ManualClock()
    )
    assert sample.symbol == "BTC"
    assert sample.source == "coinglass"
    assert sample.total_liquidation_usd == pytest.approx(17_300_000.0)
    assert sample.peak_cluster_liquidation_usd == pytest.approx(9_800_000.0)
    assert sample.peak_cluster_price_usd == pytest.approx(60_000.0)
    # `end_time` column is ms epoch → ns.
    assert sample.ts_ns == 1_745_237_400_000 * 1_000_000


def test_parse_uses_clock_when_timestamp_absent() -> None:
    payload = orjson.dumps(
        {
            "code": "0",
            "data": {
                "y": [100.0, 200.0],
                "liq": [[0, 0, 42.0]],
            },
        }
    )
    clock = ManualClock(start_ns=999_000_000_000)
    sample = parse_coinglass_heatmap_response(payload, symbol="BTC", clock=clock)
    assert sample.ts_ns == 999_000_000_000
    assert sample.peak_cluster_price_usd == 100.0


def test_parse_accepts_alternative_field_names() -> None:
    """v4 endpoints have historically renamed `liq` → `liquidation_data`
    and `y` → `prices` between minor versions; the parser must accept
    either so a server-side field rename doesn't silently zero out the
    observed metric."""
    payload = orjson.dumps(
        {
            "code": "0",
            "data": {
                "prices": [10.0, 20.0, 30.0],
                "liquidation_data": [[0, 1, 5.0], [0, 2, 8.0]],
            },
        }
    )
    sample = parse_coinglass_heatmap_response(
        payload, symbol="BTC", clock=ManualClock()
    )
    assert sample.total_liquidation_usd == pytest.approx(13.0)
    assert sample.peak_cluster_price_usd == pytest.approx(30.0)


def test_parse_rejects_non_zero_code() -> None:
    payload = orjson.dumps(
        {"code": "40001", "msg": "rate limited", "data": {"y": [], "liq": []}}
    )
    with pytest.raises(CoinglassHeatmapParseError, match="code="):
        parse_coinglass_heatmap_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_missing_data_object() -> None:
    payload = orjson.dumps({"code": "0"})
    with pytest.raises(CoinglassHeatmapParseError, match="data"):
        parse_coinglass_heatmap_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_no_usable_cells() -> None:
    """Empty cell list (or all-zero densities) is a degenerate payload
    that would produce meaningless summary stats; fail loudly instead."""
    payload = orjson.dumps(
        {"code": "0", "data": {"y": [1.0, 2.0], "liq": []}}
    )
    with pytest.raises(CoinglassHeatmapParseError, match="no usable"):
        parse_coinglass_heatmap_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_rejects_missing_price_axis() -> None:
    payload = orjson.dumps({"code": "0", "data": {"liq": [[0, 0, 1.0]]}})
    with pytest.raises(CoinglassHeatmapParseError, match="price-axis"):
        parse_coinglass_heatmap_response(payload, symbol="BTC", clock=ManualClock())


def test_parse_skips_malformed_cells() -> None:
    """Real payloads occasionally include sentinel rows (e.g. wrong-length
    tuples). Skipping them and summing the rest is the right call — the
    parser must not crash on a single bad row when 99% of the grid is fine."""
    payload = orjson.dumps(
        {
            "code": "0",
            "data": {
                "y": [50.0, 100.0],
                "liq": [
                    [0, 0, 10.0],
                    [0, 1],  # too short
                    [0, 1, "not-a-number"],
                    [0, 1, -5.0],  # negative density skipped
                    [0, 1, 7.0],
                ],
            },
        }
    )
    sample = parse_coinglass_heatmap_response(
        payload, symbol="BTC", clock=ManualClock()
    )
    assert sample.total_liquidation_usd == pytest.approx(17.0)
    assert sample.peak_cluster_price_usd == pytest.approx(50.0)


async def test_fetch_sends_api_key_header() -> None:
    captured: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.headers.get("CG-API-KEY"))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await fetch_coinglass_heatmap(
        client=client,
        base_url="https://example.test",
        path="/heatmap",
        api_key="secret-xyz",
        clock=ManualClock(),
    )
    assert captured == ["secret-xyz"]


async def test_fetch_omits_header_when_no_api_key() -> None:
    captured: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request.headers.get("CG-API-KEY"))
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await fetch_coinglass_heatmap(
        client=client,
        base_url="https://example.test",
        path="/heatmap",
        api_key=None,
        clock=ManualClock(),
    )
    assert captured == [None]


async def test_poller_publishes_on_success() -> None:
    received: list[LiquidationHeatmapSample] = []

    async def on_sample(sample: LiquidationHeatmapSample) -> None:
        received.append(sample)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassHeatmapPoller(
        client=client,
        clock=ManualClock(start_ns=1_000_000_000_000),
        on_sample=on_sample,
        base_url="https://example.test",
        heatmap_path="/heatmap",
        poll_interval_sec=60.0,
    )
    result = await poller.poll_once()
    assert result is not None
    assert result.peak_cluster_price_usd == pytest.approx(60_000.0)
    assert len(received) == 1


async def test_poller_swallows_fetch_errors() -> None:
    received: list[LiquidationHeatmapSample] = []

    async def on_sample(sample: LiquidationHeatmapSample) -> None:
        received.append(sample)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassHeatmapPoller(
        client=client,
        clock=ManualClock(),
        on_sample=on_sample,
        base_url="https://example.test",
        heatmap_path="/heatmap",
        poll_interval_sec=60.0,
    )
    assert await poller.poll_once() is None
    assert received == []


async def test_poller_rejects_nonpositive_interval() -> None:
    async def noop(sample: LiquidationHeatmapSample) -> None:
        return None

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=b""))
    )
    with pytest.raises(ValueError, match="poll_interval_sec"):
        CoinglassHeatmapPoller(
            client=client,
            clock=ManualClock(),
            on_sample=noop,
            poll_interval_sec=0.0,
        )


async def test_poller_run_loop_is_cancellable() -> None:
    async def noop(sample: LiquidationHeatmapSample) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    poller = CoinglassHeatmapPoller(
        client=client,
        clock=ManualClock(),
        on_sample=noop,
        base_url="https://example.test",
        heatmap_path="/heatmap",
        poll_interval_sec=3600.0,
    )
    task = asyncio.create_task(poller.run())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
