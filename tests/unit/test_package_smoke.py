"""Smoke test: every package declared in DESIGN.md §2 imports cleanly.

This is the tripwire that catches broken __init__.py during scaffold churn.
Replace with real unit tests as modules land.
"""

from __future__ import annotations

import importlib

import pytest

PACKAGES = [
    "bot_btc_1hr_kalshi",
    "bot_btc_1hr_kalshi.market_data",
    "bot_btc_1hr_kalshi.signal",
    "bot_btc_1hr_kalshi.risk",
    "bot_btc_1hr_kalshi.execution",
    "bot_btc_1hr_kalshi.portfolio",
    "bot_btc_1hr_kalshi.monitor",
    "bot_btc_1hr_kalshi.research",
    "bot_btc_1hr_kalshi.obs",
    "bot_btc_1hr_kalshi.admin",
    "bot_btc_1hr_kalshi.config",
    "bot_btc_1hr_kalshi.calendar",
]


@pytest.mark.parametrize("module_name", PACKAGES)
def test_package_imports(module_name: str) -> None:
    importlib.import_module(module_name)
