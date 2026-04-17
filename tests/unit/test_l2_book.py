from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.market_data import BookLevel, BookUpdate, L2Book


def _snapshot(
    seq: int,
    bids: list[tuple[int, int]],
    asks: list[tuple[int, int]],
    market_id: str = "BTC-1H",
    ts_ns: int = 1,
) -> BookUpdate:
    return BookUpdate(
        seq=seq,
        ts_ns=ts_ns,
        market_id=market_id,
        bids=tuple(BookLevel(p, s) for p, s in bids),
        asks=tuple(BookLevel(p, s) for p, s in asks),
        is_snapshot=True,
    )


def _delta(
    seq: int,
    bids: list[tuple[int, int]],
    asks: list[tuple[int, int]],
    market_id: str = "BTC-1H",
    ts_ns: int = 1,
) -> BookUpdate:
    return BookUpdate(
        seq=seq,
        ts_ns=ts_ns,
        market_id=market_id,
        bids=tuple(BookLevel(p, s) for p, s in bids),
        asks=tuple(BookLevel(p, s) for p, s in asks),
        is_snapshot=False,
    )


def test_book_starts_invalid() -> None:
    b = L2Book("BTC-1H")
    assert not b.valid
    assert b.best_bid is None
    assert b.best_ask is None
    assert b.mid_cents is None
    assert b.spread_cents is None


def test_snapshot_makes_book_valid() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 100), (39, 50)], [(42, 80), (43, 40)]))
    assert b.valid
    assert b.best_bid == BookLevel(40, 100)
    assert b.best_ask == BookLevel(42, 80)
    assert b.mid_cents == 41.0
    assert b.spread_cents == 2


def test_delta_inserts_and_removes() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 100)], [(42, 80)]))
    b.apply(_delta(2, [(41, 25)], []))  # +25 at new tier
    assert b.best_bid == BookLevel(41, 25)

    b.apply(_delta(3, [(41, -25)], []))  # cancel the 25 → tier removed
    assert b.best_bid == BookLevel(40, 100)


def test_delta_partial_fill_preserves_resting_size() -> None:
    """Regression: a signed negative delta must subtract from the resting
    quantity, not pop the entire level. Previously the parser masked
    negatives to 0 and L2Book treated size=0 as a level-remove, so a single
    10-lot partial fill would wipe the other 490 contracts at that price."""
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 500)], [(42, 80)]))

    b.apply(_delta(2, [(40, -10)], []))
    assert b.best_bid == BookLevel(40, 490)

    b.apply(_delta(3, [(40, 50)], []))
    assert b.best_bid == BookLevel(40, 540)

    b.apply(_delta(4, [(40, -540)], []))
    assert b.best_bid is None  # only bid tier cleared
    assert b.valid  # cleanly subtracted, not gap-invalidated


def test_seq_gap_invalidates_book() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(10, [(40, 100)], [(42, 80)]))
    assert b.valid
    b.apply(_delta(12, [(41, 25)], []))  # gap: expected 11
    assert not b.valid
    assert b.invalidation_reason is not None
    assert "seq_gap" in b.invalidation_reason


def test_snapshot_after_gap_restores_validity() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(10, [(40, 100)], [(42, 80)]))
    b.apply(_delta(12, [(41, 25)], []))
    assert not b.valid

    b.apply(_snapshot(20, [(39, 90)], [(41, 70)]))
    assert b.valid
    assert b.best_bid == BookLevel(39, 90)


def test_market_id_mismatch_raises() -> None:
    b = L2Book("BTC-1H")
    with pytest.raises(ValueError, match="market_id mismatch"):
        b.apply(_snapshot(1, [(40, 1)], [(42, 1)], market_id="OTHER"))


def test_book_depth_sums_top_n_each_side() -> None:
    b = L2Book("BTC-1H")
    b.apply(
        _snapshot(
            1,
            bids=[(40, 10), (39, 20), (38, 5), (37, 2), (36, 1), (35, 999)],
            asks=[(42, 8), (43, 12), (44, 6), (45, 3), (46, 1), (47, 999)],
        )
    )
    # levels=5 excludes the (35,999) and (47,999) outliers
    assert b.book_depth(levels=5) == float(10 + 20 + 5 + 2 + 1 + 8 + 12 + 6 + 3 + 1)


def test_zero_size_in_snapshot_is_dropped() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 100), (39, 0)], [(42, 80)]))
    assert 39 not in b.snapshot_levels()[0]


def test_invalidate_clears_book_and_marks_invalid() -> None:
    """Hard rule #9: a reconnect / external invalidate must make all book-
    derived features treat the book as INVALID and must not leak the stale
    prices that remained from the prior session."""
    b = L2Book("BTC-1H")
    b.apply(_snapshot(10, [(40, 100)], [(42, 80)]))
    assert b.valid
    assert b.best_bid is not None

    b.invalidate("reconnect")

    assert not b.valid
    assert b.invalidation_reason == "reconnect"
    assert b.best_bid is None
    assert b.best_ask is None
    assert b.last_seq is None
    assert b.book_depth() == 0.0


def test_snapshot_after_invalidate_restores_validity() -> None:
    b = L2Book("BTC-1H")
    b.apply(_snapshot(10, [(40, 100)], [(42, 80)]))
    b.invalidate("reconnect")

    # A fresh snapshot must rebuild fully — and crucially, the seq-gap
    # detector must not re-trigger on the first post-invalidate snapshot.
    b.apply(_snapshot(500, [(41, 55)], [(43, 45)]))
    assert b.valid
    assert b.invalidation_reason is None
    assert b.best_bid == BookLevel(41, 55)
