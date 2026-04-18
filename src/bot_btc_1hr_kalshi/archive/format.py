"""JSONL wire format for archived FeedEvents.

Each line is a JSON object tagged with a `kind` discriminator so a reader
can reconstruct the correct concrete FeedEvent. Schema drift is fatal —
an archive written under format vN must remain parseable by every future
reader, otherwise captured history becomes unusable. The schema version
is embedded on each line so future migrations can branch.

Stability contract:
  * Fields are never renamed once shipped.
  * New optional fields are fine; defaults live in the deserializer.
  * A version bump gates any breaking change.
"""

from __future__ import annotations

from typing import Any

from bot_btc_1hr_kalshi.market_data.types import (
    BookLevel,
    BookUpdate,
    FeedEvent,
    SpotTick,
    TradeEvent,
)
from bot_btc_1hr_kalshi.obs.money import Micros

ARCHIVE_FORMAT_VERSION = 1


class ArchiveFormatError(ValueError):
    """Raised on malformed or version-mismatched archive lines."""


def to_dict(event: FeedEvent) -> dict[str, Any]:
    """Serialize a FeedEvent to a JSON-ready dict."""
    if isinstance(event, BookUpdate):
        return {
            "v": ARCHIVE_FORMAT_VERSION,
            "kind": "book",
            "seq": event.seq,
            "ts_ns": event.ts_ns,
            "market_id": event.market_id,
            "bids": [[lvl.price_cents, lvl.size] for lvl in event.bids],
            "asks": [[lvl.price_cents, lvl.size] for lvl in event.asks],
            "is_snapshot": event.is_snapshot,
        }
    if isinstance(event, TradeEvent):
        return {
            "v": ARCHIVE_FORMAT_VERSION,
            "kind": "trade",
            "seq": event.seq,
            "ts_ns": event.ts_ns,
            "market_id": event.market_id,
            "price_cents": event.price_cents,
            "size": event.size,
            "aggressor": event.aggressor,
            "taker_side": event.taker_side,
        }
    if isinstance(event, SpotTick):
        return {
            "v": ARCHIVE_FORMAT_VERSION,
            "kind": "spot",
            "ts_ns": event.ts_ns,
            "venue": event.venue,
            "price_micros": int(event.price_micros),
            "size": event.size,
            # Aggressor is additive per the stability contract — v1 archives
            # written before Slice 9 emit no `aggressor` key; the reader uses
            # `.get()` so they deserialize cleanly with `aggressor=None`.
            "aggressor": event.aggressor,
        }
    raise ArchiveFormatError(f"unknown FeedEvent type: {type(event).__name__}")


def from_dict(d: dict[str, Any]) -> FeedEvent:
    """Deserialize a dict back into a FeedEvent."""
    v = d.get("v")
    if v != ARCHIVE_FORMAT_VERSION:
        raise ArchiveFormatError(
            f"archive format version mismatch: got {v!r}, "
            f"this build reads v{ARCHIVE_FORMAT_VERSION}"
        )
    kind = d.get("kind")
    if kind == "book":
        return BookUpdate(
            seq=int(d["seq"]),
            ts_ns=int(d["ts_ns"]),
            market_id=str(d["market_id"]),
            bids=tuple(BookLevel(int(p), int(s)) for p, s in d["bids"]),
            asks=tuple(BookLevel(int(p), int(s)) for p, s in d["asks"]),
            is_snapshot=bool(d["is_snapshot"]),
        )
    if kind == "trade":
        return TradeEvent(
            seq=int(d["seq"]),
            ts_ns=int(d["ts_ns"]),
            market_id=str(d["market_id"]),
            price_cents=int(d["price_cents"]),
            size=int(d["size"]),
            aggressor=d["aggressor"],
            taker_side=d["taker_side"],
        )
    if kind == "spot":
        return SpotTick(
            ts_ns=int(d["ts_ns"]),
            venue=d["venue"],
            price_micros=Micros(int(d["price_micros"])),
            size=float(d["size"]),
            aggressor=d.get("aggressor"),
        )
    raise ArchiveFormatError(f"unknown kind: {kind!r}")
