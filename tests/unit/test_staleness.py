from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.obs.clock import ManualClock


def test_not_stale_before_first_message() -> None:
    st = StalenessTracker(name="k", clock=ManualClock(0), threshold_ms=100)
    assert not st.is_stale()
    assert st.age_ms() is None


def test_stale_after_threshold() -> None:
    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=100)
    st.mark()
    clock.advance_ns(50 * 1_000_000)
    assert not st.is_stale()
    clock.advance_ns(60 * 1_000_000)  # total 110ms since mark
    assert st.is_stale()


def test_mark_resets_staleness() -> None:
    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=100)
    st.mark()
    clock.advance_ns(200 * 1_000_000)
    assert st.is_stale()
    st.mark()
    assert not st.is_stale()


def test_threshold_must_be_positive() -> None:
    with pytest.raises(ValueError, match="threshold_ms"):
        StalenessTracker(name="k", clock=ManualClock(0), threshold_ms=0)


def test_age_ms_returns_positive_after_mark() -> None:
    clock = ManualClock(0)
    st = StalenessTracker(name="k", clock=clock, threshold_ms=100)
    st.mark()
    clock.advance_ns(25 * 1_000_000)
    age = st.age_ms()
    assert age is not None
    assert age == pytest.approx(25.0)
