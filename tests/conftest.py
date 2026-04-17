"""Shared pytest fixtures for bot_btc_1hr_kalshi.

Fixtures added here as the implementation lands. Priorities per DESIGN.md:
- `frozen_clock`: injected deterministic clock (hard rule #5 — no datetime.now()).
- `synthetic_l2_book`: seedable order book for trap / sizing unit tests.
- `replay_feed`: tick-replay harness for integration tests (DESIGN.md §9).
"""

from __future__ import annotations

import pytest


@pytest.fixture
def project_root() -> str:
    """Absolute path to repo root — used for locating fixture data files."""
    from pathlib import Path

    return str(Path(__file__).resolve().parent.parent)
