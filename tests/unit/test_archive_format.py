"""Tests for archive/format.py — roundtrip every FeedEvent variant.

If this drifts, captured history becomes unreadable, so the contract is
tight: every dataclass field on every FeedEvent variant must roundtrip
bit-identical.
"""

from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.archive.format import (
    ARCHIVE_FORMAT_VERSION,
    ArchiveFormatError,
    from_dict,
    to_dict,
)
from bot_btc_1hr_kalshi.market_data.types import (
    BookLevel,
    BookUpdate,
    SpotTick,
    TradeEvent,
)
from bot_btc_1hr_kalshi.obs.money import Micros


def test_bookupdate_roundtrip_snapshot_and_delta() -> None:
    for is_snapshot in (True, False):
        ev = BookUpdate(
            seq=42, ts_ns=1_700_000_000_000_000_000,
            market_id="KBTC-M",
            bids=(BookLevel(40, 100), BookLevel(39, 250)),
            asks=(BookLevel(45, 200),),
            is_snapshot=is_snapshot,
        )
        out = from_dict(to_dict(ev))
        assert out == ev


def test_tradeevent_roundtrip() -> None:
    ev = TradeEvent(
        seq=7, ts_ns=1_700_000_000_123_456_789, market_id="KBTC-M",
        price_cents=42, size=15, aggressor="buy", taker_side="YES",
    )
    assert from_dict(to_dict(ev)) == ev


def test_spottick_roundtrip_preserves_integer_micros() -> None:
    # Important: micros must NOT round-trip through float.
    ev = SpotTick(
        ts_ns=1_700_000_000_000_000_000,
        venue="coinbase",
        price_micros=Micros(60_123_456_789),
        size=0.01234,
    )
    out = from_dict(to_dict(ev))
    assert out == ev
    assert isinstance(out.price_micros, int)
    assert out.price_micros == 60_123_456_789


def test_version_mismatch_is_fatal() -> None:
    bad = {"v": ARCHIVE_FORMAT_VERSION + 1, "kind": "spot",
           "ts_ns": 0, "venue": "coinbase", "price_micros": 0, "size": 0.0}
    with pytest.raises(ArchiveFormatError):
        from_dict(bad)


def test_unknown_kind_is_fatal() -> None:
    bad = {"v": ARCHIVE_FORMAT_VERSION, "kind": "mystery"}
    with pytest.raises(ArchiveFormatError):
        from_dict(bad)
