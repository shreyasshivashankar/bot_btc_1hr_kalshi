"""Forex Factory calendar fetcher (Slice 11 P1).

Pulls the current-week macro calendar from the community-mirrored Forex
Factory JSON endpoint (no auth, no rate limit headers), parses it into
`ScheduledEvent` tuples, and filters down to tier-1 events that match the
project's allow-listed countries and impact levels. The output is a drop-in
replacement for the static YAML calendar — `CalendarGuard.replace_events()`
atomically swaps the in-memory set while preserving the dedup ledger.

Design notes
------------
* **No NLP, no heuristic extraction.** Hard rule #8: only structured
  calendar rows graduate to tier-1. The allow-list is explicit in
  `CalendarSettings` (countries + impact levels).
* **Deterministic event names.** Guard dedups by `name`; a refresh that
  re-fetches the same FOMC entry must produce the *exact* same name so
  the `_fired` set still suppresses re-triggering. We use
  `FF:{country}:{title}:{iso_date}` to bake the key inputs into the id.
* **Fail-open to fail-closed.** Parse errors on individual rows log a
  warning and are skipped. A 5xx or malformed top-level response raises —
  the refresher loop catches and keeps the previous event list in place
  until the next interval; it never drops events silently.
* **No broker-scope state here.** This module only produces tuples; the
  guard holds the fired-ledger and the mutable event list. Keeping this
  module stateless makes it trivial to unit-test against MockTransport.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime
from typing import Any

import httpx
import orjson
import structlog

from bot_btc_1hr_kalshi.calendar.events import ScheduledEvent

log = structlog.get_logger(__name__)

# Community mirror of the Forex Factory weekly JSON. No API key, refreshed
# on the ForexFactory weekly publish cadence. If this endpoint starts
# rate-limiting or goes offline, the `fetch_url` setting allows swap to a
# paid replacement without a code change.
FF_DEFAULT_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

DEFAULT_TIER_1_COUNTRIES: frozenset[str] = frozenset({"USD"})
DEFAULT_TIER_1_IMPACTS: frozenset[str] = frozenset({"High"})


class ForexFactoryParseError(ValueError):
    """Top-level response is not a JSON array — treat as fetch failure."""


def parse_ff_json(
    raw: bytes | str,
    *,
    tier_1_countries: Iterable[str] = DEFAULT_TIER_1_COUNTRIES,
    tier_1_impacts: Iterable[str] = DEFAULT_TIER_1_IMPACTS,
) -> tuple[ScheduledEvent, ...]:
    """Parse a Forex Factory JSON response into ScheduledEvent tuples.

    Only rows whose `country` AND `impact` are in the allow-lists are
    emitted, and they are emitted as `tier_1`. Everything else is silently
    dropped — FF's medium/low bands are too noisy to pre-emptively flatten.

    Malformed individual entries are logged and skipped. A non-array
    top-level structure raises `ForexFactoryParseError` (the caller treats
    this as a transient fetch failure and keeps the prior event list).
    """
    data: Any = orjson.loads(raw)
    if not isinstance(data, list):
        raise ForexFactoryParseError(f"expected top-level JSON array, got {type(data).__name__}")
    tier_1_countries_fs = frozenset(tier_1_countries)
    tier_1_impacts_fs = frozenset(tier_1_impacts)
    out: list[ScheduledEvent] = []
    for idx, entry in enumerate(data):
        parsed = _parse_entry(
            idx,
            entry,
            tier_1_countries_fs,
            tier_1_impacts_fs,
        )
        if parsed is not None:
            out.append(parsed)
    return tuple(sorted(out, key=lambda e: e.ts_ns))


def _parse_entry(
    idx: int,
    entry: Any,
    tier_1_countries: frozenset[str],
    tier_1_impacts: frozenset[str],
) -> ScheduledEvent | None:
    if not isinstance(entry, dict):
        log.warning("forex_factory.entry_not_dict", idx=idx)
        return None
    country = entry.get("country")
    impact = entry.get("impact")
    # Cheap allow-list prefilter: skip ~90% of rows before date parsing.
    if (
        not isinstance(country, str)
        or country not in tier_1_countries
        or not isinstance(impact, str)
        or impact not in tier_1_impacts
    ):
        return None
    title = entry.get("title")
    date_raw = entry.get("date")
    if not isinstance(title, str) or not isinstance(date_raw, str):
        log.warning(
            "forex_factory.entry_missing_fields",
            idx=idx,
            has_title=isinstance(title, str),
            has_date=isinstance(date_raw, str),
        )
        return None
    try:
        dt = _parse_ff_date(date_raw)
    except ValueError as exc:
        log.warning(
            "forex_factory.entry_bad_date",
            idx=idx,
            date=date_raw,
            error=str(exc),
        )
        return None
    ts_ns = int(dt.timestamp() * 1_000_000_000)
    name = f"FF:{country}:{title}:{dt.isoformat()}"
    return ScheduledEvent(
        name=name,
        ts_ns=ts_ns,
        importance="tier_1",
        source="forex_factory",
    )


def _parse_ff_date(raw: str) -> datetime:
    """Parse an FF timestamp. FF serializes as ISO-8601 with a ±HH:MM
    offset (`2026-04-22T08:30:00-04:00`). If the string is offset-naive
    (should never happen on this endpoint), we reject — hard rule #5 only
    trusts timezone-aware timestamps inside trading logic.
    """
    # Python 3.11+ handles the `Z` suffix natively, but FF uses explicit
    # numeric offsets already. Kept the replace() for forward-compat with
    # any upstream that switches to Z notation.
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("FF date is offset-naive")
    return dt


async def fetch_ff_calendar(
    *,
    client: httpx.AsyncClient,
    url: str = FF_DEFAULT_URL,
    tier_1_countries: Iterable[str] = DEFAULT_TIER_1_COUNTRIES,
    tier_1_impacts: Iterable[str] = DEFAULT_TIER_1_IMPACTS,
) -> tuple[ScheduledEvent, ...]:
    """Fetch + parse the Forex Factory weekly calendar. Raises on HTTP or
    parse errors so callers can decide whether to retain the prior set
    vs. propagate.
    """
    resp = await client.get(url)
    resp.raise_for_status()
    return parse_ff_json(
        resp.content,
        tier_1_countries=tier_1_countries,
        tier_1_impacts=tier_1_impacts,
    )


class ForexFactoryRefresher:
    """Background loop that re-fetches the FF calendar and publishes the
    new event set via `on_refresh`. Fails soft — a failed fetch logs and
    retries next cycle. The caller owns the `on_refresh` callback (which
    typically delegates to `CalendarGuard.replace_events`) so the
    refresher stays agnostic to the guard's concurrency model.

    Parameters
    ----------
    client
        Shared `httpx.AsyncClient`. Owned by the caller; this loop never
        closes it.
    on_refresh
        Async callback invoked with each freshly-parsed event tuple
        (including the empty tuple if the endpoint returns no tier-1
        rows). NOT invoked on fetch failure — the prior set stays live.
    interval_sec
        Time between completed refreshes. The first refresh fires
        immediately on `run()`; subsequent refreshes are scheduled
        `interval_sec` after the previous one *completes*, so a slow
        endpoint can't stampede us.
    """

    __slots__ = (
        "_client",
        "_interval_sec",
        "_on_refresh",
        "_tier_1_countries",
        "_tier_1_impacts",
        "_url",
    )

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        on_refresh: Callable[[tuple[ScheduledEvent, ...]], Awaitable[None]],
        url: str = FF_DEFAULT_URL,
        interval_sec: float = 1800.0,
        tier_1_countries: Iterable[str] = DEFAULT_TIER_1_COUNTRIES,
        tier_1_impacts: Iterable[str] = DEFAULT_TIER_1_IMPACTS,
    ) -> None:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be > 0")
        self._client = client
        self._on_refresh = on_refresh
        self._url = url
        self._interval_sec = interval_sec
        self._tier_1_countries = frozenset(tier_1_countries)
        self._tier_1_impacts = frozenset(tier_1_impacts)

    async def refresh_once(self) -> tuple[ScheduledEvent, ...] | None:
        """Single fetch + publish cycle. Returns the fetched events on
        success (after publishing) or None on failure. Used directly by
        tests; `run()` calls this in a loop.
        """
        try:
            events = await fetch_ff_calendar(
                client=self._client,
                url=self._url,
                tier_1_countries=self._tier_1_countries,
                tier_1_impacts=self._tier_1_impacts,
            )
        except (httpx.HTTPError, ForexFactoryParseError, ValueError) as exc:
            log.warning(
                "forex_factory.fetch_failed",
                url=self._url,
                error=str(exc),
                exc_type=type(exc).__name__,
            )
            return None
        log.info(
            "forex_factory.refreshed",
            url=self._url,
            tier1_count=len(events),
        )
        await self._on_refresh(events)
        return events

    async def run(self) -> None:
        """Forever-loop: refresh_once → sleep → repeat. Cancel via
        `task.cancel()`; the loop re-raises CancelledError per asyncio
        convention.
        """
        while True:
            await self.refresh_once()
            await asyncio.sleep(self._interval_sec)
