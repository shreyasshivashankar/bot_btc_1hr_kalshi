"""Hourly-rotated JSONL writer for captured FeedEvents.

Call `write(event)` from the live feed loop. Each hour rolls to a new
file named `events-YYYY-MM-DDTHH.jsonl` under the configured archive
directory; the hour bucket is derived from `event.ts_ns` so the on-disk
partitioning is deterministic under replay regardless of wall-clock
skew at record time.

Durability contract: we flush on hour-roll and on explicit `close()` —
NOT per line. Rationale: the archive dir is backed by a GCS FUSE mount
in production (see deploy/cloudrun.yaml), where `flush()` is either a
no-op or triggers a full-object upload. At ~50 ticks/sec a per-line
flush would issue 180k object rewrites per hour, saturating the hot
path for no durability benefit. A crash (OOM / SIGKILL / Cloud Run
instance rotation) loses up to the current in-flight hour of ticks;
graceful SIGTERM runs through serve()'s finally block which calls
`close()`, finalizing the file. This is acceptable because the
recorder is not the system of record for fills (hard rule #7 — the
broker is) and the archive is research input, not audit input; the
lifecycle log handles audit.

GCS delivery is handled transparently by the Cloud Run GCS FUSE CSI
mount — close() on an hour-roll finalizes the object in the bucket.
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
