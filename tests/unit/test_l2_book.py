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


def test_top_of_book_cache_matches_brute_force_under_mixed_deltas() -> None:
    """The cached top-of-book must agree with `max(bids)` / `min(asks)` after
    every mutation — promotions, in-place size changes, top-level depletion
    forcing a rescan, and add-then-fill churn. This is the regression net for
    the cache: if any branch in `apply()` forgets to update the pointer, the
    final brute-force comparison surfaces it.

    The book starts with a few resting levels then absorbs ~40 mixed deltas
    spanning every code path (new tier inside the spread, partial fill of
    the top, full clear of the top, refill of a cleared tier, lift through
    multiple price points).
    """
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 100), (39, 50), (38, 25)], [(42, 80), (43, 30)]))

    deltas: list[tuple[list[tuple[int, int]], list[tuple[int, int]]]] = [
        ([(41, 20)], []),                # promote bid via new better tier
        ([(41, 30)], []),                # add to current best (size grows)
        ([(41, -50)], []),               # clear best → rescan to 40
        ([(40, -100)], []),              # clear next best → rescan to 39
        ([(39, -50)], []),               # clear → rescan to 38
        ([(38, -25)], []),               # clear → no bids
        ([(35, 10), (36, 20), (37, 30)], []),  # repopulate → best=37
        ([], [(41, 5)]),                 # promote ask inside spread
        ([], [(41, -5), (44, 60)]),      # clear top ask → rescan past 42 to itself
        ([], [(42, -80)]),               # clear next ask → rescan to 43
        ([(38, -1000)], []),             # delta below 0 — should pop, no error
    ]
    seq = 2
    for bid_deltas, ask_deltas in deltas:
        b.apply(_delta(seq, bid_deltas, ask_deltas))
        seq += 1

    # Cache must agree with a from-scratch recompute at every step's end.
    expected_best_bid = (
        BookLevel(max(b._bids), b._bids[max(b._bids)]) if b._bids else None  # type: ignore[attr-defined]
    )
    expected_best_ask = (
        BookLevel(min(b._asks), b._asks[min(b._asks)]) if b._asks else None  # type: ignore[attr-defined]
    )
    assert b.best_bid == expected_best_bid
    assert b.best_ask == expected_best_ask
    if expected_best_bid is not None and expected_best_ask is not None:
        assert b.spread_cents == expected_best_ask.price_cents - expected_best_bid.price_cents
        assert b.mid_cents == (expected_best_bid.price_cents + expected_best_ask.price_cents) / 2.0


def test_top_of_book_cache_consistent_after_each_intermediate_step() -> None:
    """Stronger version: assert cache invariant after EVERY operation, not
    just at the end. Catches a class of bugs where the cache drifts mid-
    sequence but happens to be correct at the final state."""
    b = L2Book("BTC-1H")
    b.apply(_snapshot(1, [(40, 100), (39, 50)], [(42, 80), (43, 30)]))

    def _check() -> None:
        bids = b._bids  # type: ignore[attr-defined]
        asks = b._asks  # type: ignore[attr-defined]
        if bids:
            assert b.best_bid == BookLevel(max(bids), bids[max(bids)])
        else:
            assert b.best_bid is None
        if asks:
            assert b.best_ask == BookLevel(min(asks), asks[min(asks)])
        else:
            assert b.best_ask is None

    _check()
    seq = 2
    sequence: list[tuple[list[tuple[int, int]], list[tuple[int, int]]]] = [
        ([(41, 20)], []),
        ([(41, -10)], []),
        ([(41, -10)], []),
        ([(40, -100)], []),
        ([], [(41, 5)]),
        ([], [(41, -5)]),
        ([], [(42, -80)]),
        ([(45, 7)], [(46, 9)]),  # new bid above prior best, new ask
    ]
    for bd, ad in sequence:
        b.apply(_delta(seq, bd, ad))
        _check()
        seq += 1
