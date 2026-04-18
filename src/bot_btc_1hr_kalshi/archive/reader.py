"""Streaming reader for the JSONL tick archive.

Given an archive directory (the same layout ArchiveWriter produces),
yield FeedEvents in ts_ns order across all hour-files that fall within
the requested [start_ns, end_ns) window. Events within a single file
are assumed already in ts_ns order (ArchiveWriter preserves arrival
order); across files we merge by opening all overlapping hours and
streaming in hour order.

Memory behavior: events are yielded one-at-a-time — we never hold an
entire hour's worth of lines in memory. A 2-week replay at 10k
events/hour = 1.7 GB of JSONL streams through in constant memory.

Malformed lines are logged and skipped rather than fatal; a single
corrupted event should not kill a multi-day backtest. Unknown
`kind` values or version mismatches DO propagate — those indicate
an archive from a newer code version, which is silent-data-loss
territory if we skip them.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterator
from pathlib import Path

import structlog

from bot_btc_1hr_kalshi.archive.format import ArchiveFormatError, from_dict
from bot_btc_1hr_kalshi.market_data.types import FeedEvent

_log = structlog.get_logger("bot_btc_1hr_kalshi.archive.reader")


def _parse_hour_key(filename: str) -> dt.datetime | None:
    # Expect "events-YYYY-MM-DDTHH.jsonl"
    if not filename.startswith("events-") or not filename.endswith(".jsonl"):
        return None
    stem = filename[len("events-"):-len(".jsonl")]
    try:
        return dt.datetime.strptime(stem, "%Y-%m-%dT%H").replace(tzinfo=dt.UTC)
    except ValueError:
        return None


def iter_archive(
    archive_dir: Path | str,
    *,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Iterator[FeedEvent]:
    """Yield FeedEvents across all JSONL files in archive_dir.

    `start_ns` and `end_ns` bound the per-event ts_ns window (half-open).
    Files whose hour bucket is clearly outside the window are skipped
    without opening; the per-event filter handles the boundary hours.
    """
    directory = Path(archive_dir)
    if not directory.exists():
        raise FileNotFoundError(f"archive dir does not exist: {directory}")

    hour_files: list[tuple[dt.datetime, Path]] = []
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        hour = _parse_hour_key(entry.name)
        if hour is None:
            continue
        hour_files.append((hour, entry))
    hour_files.sort(key=lambda t: t[0])

    for hour, path in hour_files:
        hour_start_ns = int(hour.timestamp() * 1_000_000_000)
        hour_end_ns = hour_start_ns + 3600 * 1_000_000_000
        if end_ns is not None and hour_start_ns >= end_ns:
            break
        if start_ns is not None and hour_end_ns <= start_ns:
            continue
        yield from _iter_file(path, start_ns=start_ns, end_ns=end_ns)


def _iter_file(
    path: Path,
    *,
    start_ns: int | None,
    end_ns: int | None,
) -> Iterator[FeedEvent]:
    with path.open("r", encoding="utf-8") as fh:
        for lineno, raw in enumerate(fh, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = from_dict(json.loads(raw))
            except (json.JSONDecodeError, KeyError) as exc:
                _log.warning("archive.malformed_line", path=str(path),
                             lineno=lineno, error=str(exc))
                continue
            except ArchiveFormatError:
                raise  # version/kind errors are fatal
            if start_ns is not None and event.ts_ns < start_ns:
                continue
            if end_ns is not None and event.ts_ns >= end_ns:
                continue
            yield event
