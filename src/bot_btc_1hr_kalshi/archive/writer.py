"""Hourly-rotated JSONL writer for captured FeedEvents.

Call `write(event)` from the live feed loop. Each hour rolls to a new
file named `events-YYYY-MM-DDTHH.jsonl` under the configured archive
directory; the hour bucket is derived from `event.ts_ns` so the on-disk
partitioning is deterministic under replay regardless of wall-clock
skew at record time.

Durability note: we `flush()` after every line so a process crash
loses at most the in-flight line; we do NOT `fsync()` because the
recorder is not the system of record for fills (hard rule #7 — the
broker is). The archive is research input, not audit input; the
lifecycle log handles audit.

GCS upload is out of scope for this module — run
`gsutil rsync <archive_dir> gs://bot-btc-1hr-kalshi-tick-archive-<env>/`
from a systemd timer or Cloud Run sidecar. Keeping it offline lets the
bot survive a GCS outage without stalling the hot path.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import IO

from bot_btc_1hr_kalshi.archive.format import to_dict
from bot_btc_1hr_kalshi.market_data.types import FeedEvent


def _hour_key(ts_ns: int) -> str:
    # ts_ns is UTC epoch ns. datetime.fromtimestamp with tz=UTC is deterministic
    # and independent of local tz, which is what we want for archive paths.
    seconds = ts_ns // 1_000_000_000
    return dt.datetime.fromtimestamp(seconds, tz=dt.UTC).strftime("%Y-%m-%dT%H")


class ArchiveWriter:
    """Append-only JSONL writer with lazy, hour-keyed file rotation.

    Thread-unsafe — call from a single event-loop task. Closing an
    ArchiveWriter flushes + closes the current file. Using it as a
    context manager is the recommended pattern.
    """

    __slots__ = ("_current_hour", "_dir", "_fh", "_lines_written")

    def __init__(self, archive_dir: Path | str) -> None:
        self._dir = Path(archive_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._current_hour: str | None = None
        self._fh: IO[str] | None = None
        self._lines_written = 0

    @property
    def lines_written(self) -> int:
        return self._lines_written

    def write(self, event: FeedEvent) -> None:
        hour = _hour_key(event.ts_ns)
        if hour != self._current_hour or self._fh is None:
            self._roll_to(hour)
        fh = self._fh
        if fh is None:  # pragma: no cover — _roll_to always sets _fh
            raise RuntimeError("archive writer file handle not open")
        fh.write(json.dumps(to_dict(event), separators=(",", ":")) + "\n")
        fh.flush()
        self._lines_written += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None
            self._current_hour = None

    def __enter__(self) -> ArchiveWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _roll_to(self, hour: str) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
        path = self._dir / f"events-{hour}.jsonl"
        # Append mode — if a prior process wrote to this hour bucket (e.g.
        # the bot restarted mid-hour) we keep those events.
        self._fh = path.open("a", encoding="utf-8")
        self._current_hour = hour
