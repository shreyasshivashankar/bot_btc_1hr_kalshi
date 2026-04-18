from __future__ import annotations

import datetime as dt

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.market_data.kalshi_rest import (
    KalshiRestClient,
    MarketDiscoveryError,
    kalshi_date_header_probe,
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


async def test_current_btc_hourly_market_prefers_closest_to_spot() -> None:
    """With `btc_spot_usd` provided, the tiebreak among same-settlement
    markets is `|strike - spot|`, NOT alphabetical ticker. Without this
    Slice 6 fix, the bot systematically picked deep-ITM markets where
    YES was pinned at ~99¢ and no tradeable edge exists.
    """
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    settlement = now.replace(minute=0, second=0) + dt.timedelta(hours=1)

    # All four settle at the same hour; strikes bracket a $78k spot.
    markets = [
        {"ticker": "KXBTC-26APR1715-B66000", "expected_expiration_time": _iso(settlement), "floor_strike": 66000},
        {"ticker": "KXBTC-26APR1715-B77500", "expected_expiration_time": _iso(settlement), "floor_strike": 77500},
        {"ticker": "KXBTC-26APR1715-B78500", "expected_expiration_time": _iso(settlement), "floor_strike": 78500},
        {"ticker": "KXBTC-26APR1715-B86000", "expected_expiration_time": _iso(settlement), "floor_strike": 86000},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    picked = await client.current_btc_hourly_market(now_ns=now_ns, btc_spot_usd=78_000.0)
    # $77500 is $500 from spot, $78500 is $500 too; tiebreak alphabetically.
    assert picked.ticker == "KXBTC-26APR1715-B77500"


async def test_current_btc_hourly_market_spot_none_uses_alphabetical() -> None:
    """Back-compat: without spot, tiebreak on ticker (legacy behavior)."""
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    settlement = now.replace(minute=0, second=0) + dt.timedelta(hours=1)
    markets = [
        {"ticker": "KXBTC-26APR1715-B77500", "expected_expiration_time": _iso(settlement), "floor_strike": 77500},
        {"ticker": "KXBTC-26APR1715-B66000", "expected_expiration_time": _iso(settlement), "floor_strike": 66000},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    picked = await client.current_btc_hourly_market(now_ns=now_ns)  # no spot
    # Alphabetical: B66000 sorts before B77500.
    assert picked.ticker == "KXBTC-26APR1715-B66000"


async def test_current_btc_hourly_market_spot_beats_settlement_tie_only() -> None:
    """Settlement-time remains the primary key — spot proximity only
    matters within the same settlement. A closer-to-spot market that
    settles LATER than another must still lose."""
    now = dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC)
    now_ns = int(now.timestamp() * 1_000_000_000)
    # Both within horizon, different settlements.
    close_settle = now + dt.timedelta(minutes=30)
    far_settle = now + dt.timedelta(minutes=55)
    markets = [
        {"ticker": "KXBTC-SOON-B66000", "expected_expiration_time": _iso(close_settle), "floor_strike": 66000},
        {"ticker": "KXBTC-LATE-B78000", "expected_expiration_time": _iso(far_settle), "floor_strike": 78000},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=orjson.dumps({"markets": markets}))

    client = KalshiRestClient(client=_client(httpx.MockTransport(handler)))
    picked = await client.current_btc_hourly_market(now_ns=now_ns, btc_spot_usd=78_000.0)
    # Soonest-settlement wins even though LATE is closer to spot.
    assert picked.ticker == "KXBTC-SOON-B66000"


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


async def test_kalshi_date_header_probe_parses_rfc7231() -> None:
    # RFC 7231 IMF-fixdate: "Sun, 06 Nov 1994 08:49:37 GMT"
    # The probe adds +500ms to the parsed second-truncated Date to center
    # the zero-drift measurement on zero (the header floor-truncates to 1s).
    date_hdr = "Fri, 17 Apr 2026 14:12:00 GMT"
    parsed_ns = int(
        dt.datetime(2026, 4, 17, 14, 12, 0, tzinfo=dt.UTC).timestamp()
        * 1_000_000_000
    )
    expected_ns = parsed_ns + 500_000_000

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/trade-api/v2/exchange/status"
        return httpx.Response(
            200,
            headers={"Date": date_hdr},
            content=b'{"exchange_active":true}',
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.test/trade-api/v2",
    )
    probe = kalshi_date_header_probe(client)
    got_ns = await probe()
    assert got_ns == expected_ns


async def test_kalshi_date_header_probe_missing_header_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}")  # no Date header

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.test/trade-api/v2",
    )
    probe = kalshi_date_header_probe(client)
    with pytest.raises(RuntimeError, match="Date header"):
        await probe()


async def test_kalshi_date_header_probe_5xx_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"")

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.test/trade-api/v2",
    )
    probe = kalshi_date_header_probe(client)
    with pytest.raises(httpx.HTTPStatusError):
        await probe()
