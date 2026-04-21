"""Tests for the Forex Factory fetcher + parser (Slice 11 P1)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orjson
import pytest

from bot_btc_1hr_kalshi.calendar import (
    CalendarGuard,
    ForexFactoryParseError,
    ForexFactoryRefresher,
    ScheduledEvent,
    fetch_ff_calendar,
    parse_ff_json,
)
from bot_btc_1hr_kalshi.obs.clock import ManualClock

FIXTURE = Path(__file__).parent.parent / "fixtures" / "ff_calendar_sample.json"


def _fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


def test_parse_filters_to_tier1_us_high() -> None:
    """Default allow-list is USD + High. The medium US row, the EUR row,
    must be dropped; the three USD/High rows survive as tier_1."""
    events = parse_ff_json(_fixture_bytes())
    names = [e.name for e in events]
    # Three USD/High events in the fixture: CPI, FOMC, GDP.
    assert len(events) == 3
    assert all(e.importance == "tier_1" for e in events)
    assert all(e.source == "forex_factory" for e in events)
    assert all(":USD:" in n for n in names)
    # Events come back sorted by ts_ns — CPI on 04-10, FOMC 04-29, GDP 04-30.
    titles = [e.name.split(":")[2] for e in events]
    assert titles == ["CPI m/m", "FOMC Press Conference", "GDP q/q"]


def test_parse_respects_country_allowlist_override() -> None:
    events = parse_ff_json(
        _fixture_bytes(),
        tier_1_countries={"EUR"},
        tier_1_impacts={"High"},
    )
    # Only the EUR/High Ifo row.
    assert len(events) == 1
    assert ":EUR:" in events[0].name


def test_parse_respects_impact_allowlist_override() -> None:
    events = parse_ff_json(
        _fixture_bytes(),
        tier_1_countries={"USD"},
        tier_1_impacts={"Medium"},
    )
    # Only the USD/Medium Core Retail Sales row.
    assert len(events) == 1
    assert "Core Retail Sales" in events[0].name


def test_parse_names_are_deterministic_for_dedup() -> None:
    """Two passes over the same payload must produce the exact same
    event names — otherwise the guard's `_fired` ledger fails to dedup
    a re-fetched event and we risk double-flattening on refresh."""
    a = parse_ff_json(_fixture_bytes())
    b = parse_ff_json(_fixture_bytes())
    assert [e.name for e in a] == [e.name for e in b]
    assert [e.ts_ns for e in a] == [e.ts_ns for e in b]


def test_parse_rejects_non_array_top_level() -> None:
    with pytest.raises(ForexFactoryParseError):
        parse_ff_json(b'{"events": []}')


def test_parse_skips_malformed_entries() -> None:
    """One good row, one missing `date`, one non-dict, one unknown
    country. Expect just the good row to come through — don't crash."""
    payload = orjson.dumps(
        [
            {
                "title": "Good",
                "country": "USD",
                "date": "2026-05-01T08:30:00-04:00",
                "impact": "High",
            },
            {"title": "Bad", "country": "USD", "impact": "High"},  # no date
            "not-a-dict",
            {
                "title": "OtherLand",
                "country": "XYZ",
                "date": "2026-05-02T00:00:00Z",
                "impact": "High",
            },
        ]
    )
    events = parse_ff_json(payload)
    assert len(events) == 1
    assert "Good" in events[0].name


def test_parse_rejects_offset_naive_timestamps() -> None:
    """Hard rule #5: trading logic uses tz-aware times only. An offset-
    naive FF row is dropped, not silently promoted to UTC."""
    payload = orjson.dumps(
        [
            {
                "title": "Naive",
                "country": "USD",
                "date": "2026-05-01T08:30:00",  # no offset
                "impact": "High",
            },
        ]
    )
    events = parse_ff_json(payload)
    assert events == ()


def test_parse_tolerates_z_suffix() -> None:
    payload = orjson.dumps(
        [
            {
                "title": "Zulu",
                "country": "USD",
                "date": "2026-05-01T08:30:00Z",
                "impact": "High",
            },
        ]
    )
    events = parse_ff_json(payload)
    assert len(events) == 1


async def test_fetch_ff_calendar_roundtrips_fixture() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "nfs.faireconomy.media"
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    events = await fetch_ff_calendar(client=client)
    assert len(events) == 3


async def test_fetch_raises_on_5xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await fetch_ff_calendar(client=client)


async def test_fetch_raises_on_malformed_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(orjson.JSONDecodeError):
        await fetch_ff_calendar(client=client)


async def test_refresher_publishes_fetched_events() -> None:
    received: list[tuple[ScheduledEvent, ...]] = []

    async def on_refresh(events: tuple[ScheduledEvent, ...]) -> None:
        received.append(events)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_fixture_bytes())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ref = ForexFactoryRefresher(
        client=client,
        on_refresh=on_refresh,
        url="https://example.test/ff",
        interval_sec=3600.0,
    )
    result = await ref.refresh_once()
    assert result is not None and len(result) == 3
    assert len(received) == 1 and len(received[0]) == 3


async def test_refresher_swallows_fetch_errors() -> None:
    """refresh_once must return None on HTTP failure, NOT invoke the
    callback (so the prior event list stays live), and NOT raise."""
    received: list[tuple[ScheduledEvent, ...]] = []

    async def on_refresh(events: tuple[ScheduledEvent, ...]) -> None:
        received.append(events)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"server down")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ref = ForexFactoryRefresher(
        client=client,
        on_refresh=on_refresh,
        url="https://example.test/ff",
        interval_sec=3600.0,
    )
    assert await ref.refresh_once() is None
    assert received == []


async def test_refresher_rejects_nonpositive_interval() -> None:
    async def noop(events: tuple[ScheduledEvent, ...]) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"[]")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(ValueError, match="interval_sec"):
        ForexFactoryRefresher(
            client=client,
            on_refresh=noop,
            interval_sec=0.0,
        )


async def test_refresher_wires_into_guard_replace_events() -> None:
    """End-to-end: a fresh fetch must hot-swap the guard's event set,
    and already-fired events must remain deduped across the swap. This
    is the P1 soak-time regression guard — without it, every refresh
    could re-fire the same FOMC flatten."""
    NS = 1_000_000_000
    clock = ManualClock(start_ns=0)
    fire_count = 0

    async def trigger() -> None:
        nonlocal fire_count
        fire_count += 1

    ev_a = ScheduledEvent(name="FF:USD:A:x", ts_ns=120 * NS, importance="tier_1")
    guard = CalendarGuard(
        clock=clock,
        events=[ev_a],
        trigger=trigger,
        lead_seconds=60.0,
    )

    # Fire the first event.
    clock.set_ns(70 * NS)
    await guard.tick()
    assert fire_count == 1
    assert "FF:USD:A:x" in guard.already_fired

    # Refresh: same event re-appears + a new one further out. The old
    # one must still dedupe (fire ledger preserved), the new one must
    # be discoverable.
    ev_b = ScheduledEvent(name="FF:USD:B:y", ts_ns=300 * NS, importance="tier_1")
    guard.replace_events([ev_a, ev_b])
    await guard.tick()
    assert fire_count == 1  # no double-flatten
    # Advance into the new event's lead window.
    clock.set_ns(260 * NS)
    await guard.tick()
    assert fire_count == 2


async def test_refresher_run_loop_is_cancellable() -> None:
    async def on_refresh(events: tuple[ScheduledEvent, ...]) -> None:
        return None

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"[]")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ref = ForexFactoryRefresher(
        client=client,
        on_refresh=on_refresh,
        url="https://example.test/ff",
        interval_sec=3600.0,
    )
    task = asyncio.create_task(ref.run())
    await asyncio.sleep(0)  # let it reach the sleep
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
