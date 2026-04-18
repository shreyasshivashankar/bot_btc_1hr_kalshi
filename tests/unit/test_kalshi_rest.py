from __future__ import annotations

import datetime as dt

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.kalshi_rest import (
    KalshiRestClient,
    MarketDiscoveryError,
)


def _client(handler: httpx.MockTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=handler, base_url="https://api.test")


def _iso(ts: dt.datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


async def test_current_btc_hourly_market_picks_soonest_settlement() -> None:
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    # Three candidate markets: past, 48min away (this hour), 2h away.
    past = now - dt.timedelta(minutes=30)
    this_hour = now.replace(minute=0, second=0) + dt.timedelta(hours=1)  # 15:00
    next_hour = this_hour + dt.timedelta(hours=1)

    markets = [
        {
            "ticker": "KXBTC-26APR1714-B60000",
            "expected_expiration_time": _iso(past),
            "floor_strike": 60000,
            "status": "active",
        },
        {
            "ticker": "KXBTC-26APR1715-B61000",
            "expected_expiration_time": _iso(this_hour),
            "floor_strike": 61000,
            "status": "active",
        },
        {
            "ticker": "KXBTC-26APR1716-B62000",
            "expected_expiration_time": _iso(next_hour),
            "floor_strike": 62000,
            "status": "active",
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trade-api/v2/markets"
        assert request.url.params.get("series_ticker") == "KXBTC"
        assert request.url.params.get("status") == "open"
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    picked = await client.current_btc_hourly_market(now_ns=now_ns)
    assert picked.ticker == "KXBTC-26APR1715-B61000"
    assert picked.strike_usd == 61000.0
    assert picked.settlement_ts_ns == int(this_hour.timestamp() * 1_000_000_000)


async def test_current_btc_hourly_market_no_match_raises() -> None:
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    # Only a past-expiration market listed.
    past = now - dt.timedelta(minutes=30)
    markets = [
        {
            "ticker": "KXBTC-old",
            "expected_expiration_time": _iso(past),
            "floor_strike": 60000,
        }
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    with pytest.raises(MarketDiscoveryError):
        await client.current_btc_hourly_market(now_ns=now_ns)


async def test_current_btc_hourly_market_falls_back_to_ticker_strike() -> None:
    """If floor/cap_strike absent, parse from ticker suffix."""
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    settlement = now.replace(minute=0, second=0) + dt.timedelta(hours=1)

    markets = [
        {
            "ticker": "KXBTC-26APR1715-B63500",
            "expected_expiration_time": _iso(settlement),
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    picked = await client.current_btc_hourly_market(now_ns=now_ns)
    assert picked.strike_usd == 63500.0


async def test_list_open_markets_paginates() -> None:
    page1 = {
        "markets": [{"ticker": "A"}],
        "cursor": "page2",
    }
    page2 = {
        "markets": [{"ticker": "B"}],
        "cursor": "",
    }

    calls: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        calls.append(cursor)
        body = page1 if not cursor else page2
        return httpx.Response(200, content=orjson.dumps(body))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    out = await client.list_open_markets()
    assert [m["ticker"] for m in out] == ["A", "B"]
    assert calls == [None, "page2"]


async def test_list_open_markets_http_error_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b'{"error":"boom"}')

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    with pytest.raises(MarketDiscoveryError):
        await client.list_open_markets()
