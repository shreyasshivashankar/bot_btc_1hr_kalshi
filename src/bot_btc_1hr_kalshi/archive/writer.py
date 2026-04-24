"""Hourly-rotated JSONL writer for captured FeedEvents.

Call `write(event)` from the live feed loop. Each hour rolls to a new
file named `events-YYYY-MM-DDTHH.jsonl` under the configured archive
directory; the hour bucket is derived from `event.ts_ns` so the on-disk
partitioning is deterministic under replay regardless of wall-clock
skew at record time.

Two-tier persistence: the writer writes to a local `staging_dir`
(default: a tempdir). On hour-roll and on `close()` the finalized
hour file is moved to the operator-supplied `archive_dir`. In
production the `archive_dir` is the GCS FUSE mount; staging is the
container's local disk. This is intentional — GCS objects are not
appendable, so every `flush()`/`close()` against a FUSE-backed file
triggers a full object rewrite. At ~50 ticks/sec a per-write FUSE
flush would issue ~180k object rewrites per hour, saturating the
mount and emitting `feedloop.archive_write_error` at feed rate. With
staging the FUSE driver only sees one move per hour-roll plus one on
graceful shutdown — three orders of magnitude fewer ops.

Restart semantics: if the destination already has a file for the same
hour bucket (a previous process died mid-hour and finalized only its
prefix), the staging file is appended onto the existing destination
file rather than overwriting it. This preserves the
"append-on-restart" contract that the reader relies on.

Durability contract: we flush on hour-roll and on explicit `close()` —
NOT per line. A crash (OOM / SIGKILL / Cloud Run instance rotation)
loses up to the current in-flight hour of ticks (which only ever
existed in staging, never reached the FUSE mount); graceful SIGTERM
runs through serve()'s finally block which calls `close()`,
finalizing the file. Acceptable because the recorder is not the
system of record for fills (hard rule #7 — the broker is) and the
archive is research input, not audit input; the lifecycle log handles
audit.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import tempfile
from pathlib import Path
from typing import IO

from bot_btc_1hr_kalshi.archive.format import to_dict
from bot_btc_1hr_kalshi.market_data.types import FeedEvent


def _hour_key(ts_ns: int) -> str:
    # ts_ns is UTC epoch ns. datetime.fromtimestamp with tz=UTC is deterministic
    # and independent of local tz, which is what we want for archive paths.
    seconds = ts_ns // 1_000_000_000
    return dt.datetime.fromtimestamp(seconds, tz=dt.UTC).strftime("%Y-%m-%dT%H")


def _hour_filename(hour: str) -> str:
    return f"events-{hour}.jsonl"


def _default_staging_dir() -> Path:
    return Path(tempfile.gettempdir()) / "bot-btc-1hr-kalshi-archive"


class ArchiveWriter:
    """Append-only JSONL writer with lazy, hour-keyed file rotation
    and a local staging tier in front of the supplied archive_dir.

    Thread-unsafe — call from a single event-loop task. Closing an
    ArchiveWriter flushes + closes the current file and finalizes
    (moves) it to the archive_dir. Using it as a context manager is
    the recommended pattern.
    """

    __slots__ = (
        "_archive_dir",
        "_current_hour",
        "_fh",
        "_lines_written",
        "_staging_dir",
    )

    def __init__(
        self,
        archive_dir: Path | str,
        *,
        staging_dir: Path | str | None = None,
    ) -> None:
        self._archive_dir = Path(archive_dir)
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._staging_dir = (
            Path(staging_dir) if staging_dir is not None else _default_staging_dir()
        )
        self._staging_dir.mkdir(parents=True, exist_ok=True)
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
        try:
            fh.write(json.dumps(to_dict(event), separators=(",", ":")) + "\n")
        except Exception:
            # Defensive even on local disk: if the staging fh somehow dies
            # (disk full, host eviction surfacing as I/O error), clear state
            # so the next call reopens and we don't loop on a dead handle.
            self._fh = None
            self._current_hour = None
            raise
        self._lines_written += 1

    def close(self) -> None:
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None
        if self._current_hour is not None:
            self._finalize_hour(self._current_hour)
            self._current_hour = None

    def __enter__(self) -> ArchiveWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _roll_to(self, hour: str) -> None:
        prior_hour = self._current_hour
        if self._fh is not None:
            self._fh.flush()
            self._fh.close()
            self._fh = None
        if prior_hour is not None and prior_hour != hour:
            self._finalize_hour(prior_hour)
        path = self._staging_dir / _hour_filename(hour)
        # Append mode — if a prior process wrote to this hour bucket (e.g.
        # the bot restarted mid-hour and the staging tier survived) we keep
        # those events.
        self._fh = path.open("a", encoding="utf-8")
        self._current_hour = hour

    def _finalize_hour(self, hour: str) -> None:
        """Move the staging file for `hour` into the archive_dir.

        If the destination already exists (prior process finalized a
        prefix of this same hour bucket before crashing), append the
        staging file onto it so we preserve the older content. Single
        FUSE op per hour-roll — that is the whole point of staging.
        """
        src = self._staging_dir / _hour_filename(hour)
        if not src.exists():
            return
        dst = self._archive_dir / _hour_filename(hour)
        if dst.exists():
            with src.open("rb") as fsrc, dst.open("ab") as fdst:
                shutil.copyfileobj(fsrc, fdst)
            src.unlink()
        else:
            shutil.move(str(src), str(dst))
