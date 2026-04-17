from __future__ import annotations

from pathlib import Path

import pytest

from bot_btc_1hr_kalshi.calendar import load_calendar, parse_calendar


def test_parses_iso_and_sorts_by_time() -> None:
    raw = """
    events:
      - name: CPI
        when: 2026-05-01T12:30:00Z
        importance: tier_1
      - name: FOMC
        when: 2026-04-29T18:00:00Z
        importance: tier_1
        source: manual
      - name: Retail_Sales
        when: 2026-04-15T12:30:00Z
        importance: tier_2
    """
    events = parse_calendar(raw)
    assert [e.name for e in events] == ["Retail_Sales", "FOMC", "CPI"]
    assert events[1].is_tier_one
    assert not events[0].is_tier_one


def test_rejects_naive_timestamp() -> None:
    raw = """
    events:
      - name: CPI
        when: 2026-05-01T12:30:00
        importance: tier_1
    """
    with pytest.raises(ValueError, match="timezone-aware"):
        parse_calendar(raw)


def test_rejects_unknown_importance() -> None:
    raw = """
    events:
      - name: CPI
        when: 2026-05-01T12:30:00Z
        importance: critical
    """
    with pytest.raises(ValueError, match="importance"):
        parse_calendar(raw)


def test_rejects_missing_key() -> None:
    raw = """
    events:
      - name: CPI
        importance: tier_1
    """
    with pytest.raises(ValueError, match="missing required key"):
        parse_calendar(raw)


def test_empty_yaml_returns_empty_tuple() -> None:
    assert parse_calendar("") == ()


def test_load_calendar_round_trip(tmp_path: Path) -> None:
    p = tmp_path / "cal.yaml"
    p.write_text(
        "events:\n  - name: CPI\n    when: 2026-05-01T12:30:00Z\n    importance: tier_1\n",
        encoding="utf-8",
    )
    events = load_calendar(p)
    assert len(events) == 1
    assert events[0].name == "CPI"
