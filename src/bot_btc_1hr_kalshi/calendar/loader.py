"""Parse an economic-calendar YAML file into ScheduledEvent tuples.

File format (timestamps are ISO-8601, UTC required):

    events:
      - name: FOMC_Press_Conference
        when: 2026-05-01T18:30:00Z
        importance: tier_1
        source: manual
      - name: US_CPI
        when: 2026-04-10T12:30:00Z
        importance: tier_1

No network I/O — the operator checks in this file alongside the config bundle
so the deployed container has a deterministic schedule.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, get_args

import yaml

from bot_btc_1hr_kalshi.calendar.events import Importance, ScheduledEvent

_VALID_IMPORTANCES: frozenset[str] = frozenset(get_args(Importance))


def load_calendar(path: Path | str) -> tuple[ScheduledEvent, ...]:
    raw = Path(path).read_text(encoding="utf-8")
    return parse_calendar(raw)


def parse_calendar(raw: str) -> tuple[ScheduledEvent, ...]:
    data: Any = yaml.safe_load(raw) or {}
    if not isinstance(data, dict):
        raise ValueError("calendar YAML must be a mapping with an `events` key")
    entries = data.get("events", [])
    if not isinstance(entries, list):
        raise ValueError("`events` must be a list")
    out: list[ScheduledEvent] = []
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise ValueError(f"events[{idx}] must be a mapping")
        out.append(_build_event(idx, entry))
    return tuple(sorted(out, key=lambda e: e.ts_ns))


def _build_event(idx: int, entry: dict[str, Any]) -> ScheduledEvent:
    try:
        name = str(entry["name"])
        when = entry["when"]
        importance = str(entry["importance"])
    except KeyError as e:
        raise ValueError(f"events[{idx}] missing required key {e}") from e

    if importance not in _VALID_IMPORTANCES:
        raise ValueError(
            f"events[{idx}] importance {importance!r} not in {sorted(_VALID_IMPORTANCES)}"
        )

    if isinstance(when, datetime):
        dt = when
    elif isinstance(when, str):
        dt = datetime.fromisoformat(when.replace("Z", "+00:00"))
    else:
        raise ValueError(f"events[{idx}] `when` must be an ISO-8601 string or datetime")
    if dt.tzinfo is None:
        raise ValueError(f"events[{idx}] `when` must be timezone-aware (use ...Z for UTC)")

    ts_ns = int(dt.timestamp() * 1_000_000_000)
    source = str(entry.get("source", "manual"))
    return ScheduledEvent(
        name=name,
        ts_ns=ts_ns,
        importance=importance,  # type: ignore[arg-type]
        source=source,
    )
