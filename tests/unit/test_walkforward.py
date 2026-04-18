"""Tests for research.walkforward split generator."""

from __future__ import annotations

from itertools import pairwise

import pytest

from bot_btc_1hr_kalshi.research.walkforward import (
    NS_PER_DAY,
    walk_forward_splits,
)


def test_anchored_train_window_grows() -> None:
    splits = list(walk_forward_splits(
        total_start_ns=0,
        total_end_ns=30 * NS_PER_DAY,
        train_days=10,
        validate_days=5,
    ))
    # 30 days total, 10 train + 5 validate, step 5 -> 4 validate windows
    # at days [10-15, 15-20, 20-25, 25-30].
    assert len(splits) == 4
    for s in splits:
        assert s.train_start_ns == 0           # anchored
    assert splits[0].validate_start_ns == 10 * NS_PER_DAY
    assert splits[-1].validate_end_ns == 30 * NS_PER_DAY


def test_sliding_train_window_uses_recent_days() -> None:
    splits = list(walk_forward_splits(
        total_start_ns=0,
        total_end_ns=30 * NS_PER_DAY,
        train_days=10,
        validate_days=5,
        anchored=False,
    ))
    for s in splits:
        # Train window width constant at 10 days.
        assert pytest.approx(s.train_days) == 10.0
    # First split trains on days 0-10; last split trains on days 15-25.
    assert splits[0].train_start_ns == 0
    assert splits[-1].train_start_ns == 15 * NS_PER_DAY


def test_validation_windows_never_overlap() -> None:
    splits = list(walk_forward_splits(
        total_start_ns=0, total_end_ns=40 * NS_PER_DAY,
        train_days=10, validate_days=7,
    ))
    for prev, nxt in pairwise(splits):
        assert prev.validate_end_ns <= nxt.validate_start_ns


def test_incomplete_trailing_window_is_dropped() -> None:
    # 12 total days, 10 train + 5 validate = 15 -> no splits fit
    assert list(walk_forward_splits(
        total_start_ns=0, total_end_ns=12 * NS_PER_DAY,
        train_days=10, validate_days=5,
    )) == []
    # 20 total days -> exactly one split (10..15); the 15..20 window would
    # need another 5-day validate window that DOES fit, so 2 splits.
    assert len(list(walk_forward_splits(
        total_start_ns=0, total_end_ns=20 * NS_PER_DAY,
        train_days=10, validate_days=5,
    ))) == 2


def test_rejects_nonpositive_windows() -> None:
    with pytest.raises(ValueError):
        list(walk_forward_splits(
            total_start_ns=0, total_end_ns=NS_PER_DAY,
            train_days=0, validate_days=5,
        ))
    with pytest.raises(ValueError):
        list(walk_forward_splits(
            total_start_ns=0, total_end_ns=NS_PER_DAY,
            train_days=5, validate_days=-1,
        ))
    with pytest.raises(ValueError):
        list(walk_forward_splits(
            total_start_ns=10, total_end_ns=5,
            train_days=1, validate_days=1,
        ))
