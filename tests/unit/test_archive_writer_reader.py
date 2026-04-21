"""Tests for the writer -> reader round trip.

Covers:
  * Hour-bucket rotation based on event.ts_ns (not wall-clock).
  * Append-on-restart semantics (new writer, same hour, preserves prior lines).
  * Reader applies [start_ns, end_ns) window correctly.
  * Reader skips a single malformed line without aborting the stream.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

from bot_btc_1hr_kalshi.archive.reader import iter_archive
from bot_btc_1hr_kalshi.archive.writer import ArchiveWriter
from bot_btc_1hr_kalshi.market_data.types import (
    BookLevel,
    BookUpdate,
    SpotTick,
    TradeEvent,
)
from bot_btc_1hr_kalshi.obs.money import Micros


def _hour_ns(year: int, month: int, day: int, hour: int) -> int:
    return int(dt.datetime(year, month, day, hour, tzinfo=dt.UTC).timestamp()
               * 1_000_000_000)


def _book(ts_ns: int, seq: int = 1) -> BookUpdate:
    return BookUpdate(
        seq=seq, ts_ns=ts_ns, market_id="KBTC-M",
        bids=(BookLevel(40, 100),),
        asks=(BookLevel(45, 100),),
        is_snapshot=True,
    )


def _spot(ts_ns: int, price_micros: int = 60_000_000_000) -> SpotTick:
    return SpotTick(ts_ns=ts_ns, venue="coinbase",
                    price_micros=Micros(price_micros), size=1.0)


def test_writer_finalizes_prior_hour_on_roll(tmp_path: Path) -> None:
    """Slice 11 durability contract: per-line flush is removed (incompatible
    with GCS FUSE). The hour-roll is the only mid-run persistence
    checkpoint — it must fully flush and close the prior-hour file so the
    underlying GCS object is finalized while the writer keeps running."""
    h0 = _hour_ns(2026, 4, 17, 15)
    h1 = _hour_ns(2026, 4, 17, 16)
    w = ArchiveWriter(tmp_path)
    try:
        w.write(_book(h0 + 1))
        w.write(_spot(h0 + 2))
        # Cross the hour boundary — prior file must be closed, not just flushed.
        w.write(_book(h1 + 1, seq=2))
        prior = tmp_path / "events-2026-04-17T15.jsonl"
        assert prior.read_text().splitlines(), "prior-hour file empty after roll"
        assert len(prior.read_text().splitlines()) == 2
    finally:
        w.close()


def test_writer_rotates_on_hour_boundary(tmp_path: Path) -> None:
    h0 = _hour_ns(2026, 4, 17, 15)
    h1 = _hour_ns(2026, 4, 17, 16)
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h0 + 1_000_000))          # hour 15
        w.write(_spot(h0 + 2_000_000))          # hour 15
        w.write(_book(h1 + 3_000_000, seq=2))   # hour 16
    files = sorted(p.name for p in tmp_path.iterdir() if p.suffix == ".jsonl")
    assert files == ["events-2026-04-17T15.jsonl", "events-2026-04-17T16.jsonl"]
    hour15 = (tmp_path / "events-2026-04-17T15.jsonl").read_text().splitlines()
    hour16 = (tmp_path / "events-2026-04-17T16.jsonl").read_text().splitlines()
    assert len(hour15) == 2
    assert len(hour16) == 1


def test_roundtrip_preserves_order_and_values(tmp_path: Path) -> None:
    h0 = _hour_ns(2026, 4, 17, 15)
    events = [
        _book(h0 + 1_000),
        _spot(h0 + 2_000, price_micros=60_111_111),
        TradeEvent(seq=9, ts_ns=h0 + 3_000, market_id="KBTC-M",
                   price_cents=42, size=7, aggressor="sell", taker_side="NO"),
    ]
    with ArchiveWriter(tmp_path) as w:
        for ev in events:
            w.write(ev)

    readback = list(iter_archive(tmp_path))
    assert readback == events


def test_reader_respects_time_window(tmp_path: Path) -> None:
    h15 = _hour_ns(2026, 4, 17, 15)
    h16 = _hour_ns(2026, 4, 17, 16)
    h17 = _hour_ns(2026, 4, 17, 17)
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h15 + 1))
        w.write(_book(h16 + 1, seq=2))
        w.write(_book(h17 + 1, seq=3))

    # window covering only hour 16
    events = list(iter_archive(tmp_path, start_ns=h16, end_ns=h17))
    assert [e.seq for e in events if isinstance(e, BookUpdate)] == [2]


def test_writer_appends_on_restart_within_same_hour(tmp_path: Path) -> None:
    h0 = _hour_ns(2026, 4, 17, 15)
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h0 + 1))
    # Process restart — new writer, same hour.
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h0 + 2, seq=2))
    events = list(iter_archive(tmp_path))
    assert [e.seq for e in events if isinstance(e, BookUpdate)] == [1, 2]


def test_reader_skips_malformed_line_but_continues(tmp_path: Path) -> None:
    h0 = _hour_ns(2026, 4, 17, 15)
    path = tmp_path / "events-2026-04-17T15.jsonl"
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h0 + 1))
        w.write(_book(h0 + 2, seq=2))
    # Inject a corrupt line in the middle of the file.
    lines = path.read_text().splitlines()
    lines.insert(1, "{not valid json")
    path.write_text("\n".join(lines) + "\n")

    events = list(iter_archive(tmp_path))
    # Corrupt line skipped; both good events still delivered.
    assert len(events) == 2


def test_writer_recovers_when_fh_closed_mid_hour(tmp_path: Path) -> None:
    """Regression: GCS-FUSE can close the fh mid-hour (CSI driver remount,
    transient upload stall). Before the fix the writer kept calling
    fh.write() on the dead handle every tick, spamming
    `feedloop.archive_write_error` at feed rate. The fix clears _fh on
    write exception so the next call triggers a fresh _roll_to."""
    h0 = _hour_ns(2026, 4, 17, 15)
    w = ArchiveWriter(tmp_path)
    try:
        w.write(_book(h0 + 1))
        # Simulate GCS-FUSE closing the underlying file handle out from
        # under us, mid-hour, no exception until the next write.
        assert w._fh is not None
        w._fh.close()
        import pytest
        with pytest.raises(ValueError):
            w.write(_book(h0 + 2, seq=2))
        # Internal state must be cleared so the next write reopens cleanly.
        assert w._fh is None
        assert w._current_hour is None
        # Next write succeeds against a freshly reopened (append) file.
        w.write(_book(h0 + 3, seq=3))
    finally:
        w.close()
    events = [e for e in iter_archive(tmp_path) if isinstance(e, BookUpdate)]
    # seq=1 landed on disk before close(); seq=2 was lost with the dead
    # handle; seq=3 landed after recovery.
    assert [e.seq for e in events] == [1, 3]


def test_reader_ignores_unrelated_files(tmp_path: Path) -> None:
    h0 = _hour_ns(2026, 4, 17, 15)
    with ArchiveWriter(tmp_path) as w:
        w.write(_book(h0 + 1))
    (tmp_path / "notes.txt").write_text("unrelated")
    (tmp_path / "events-bogus.jsonl").write_text(json.dumps({"v": 1}) + "\n")

    events = list(iter_archive(tmp_path))
    # Only the properly named file is read; the malformed filename is skipped
    # without opening.
    assert len(events) == 1
