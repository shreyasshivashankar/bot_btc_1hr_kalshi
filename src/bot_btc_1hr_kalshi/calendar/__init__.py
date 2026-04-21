"""Structured economic calendar poller (hard rule #8).

Tier-1 events trigger a pre-emptive flatten at T-lead (default 60s). No NLP
triggers — only curated YAML schedule, the Forex Factory weekly feed (Slice
11 P1, allow-listed by country/impact), or the human kill-switch.
"""

from bot_btc_1hr_kalshi.calendar.events import Importance, ScheduledEvent
from bot_btc_1hr_kalshi.calendar.forex_factory import (
    FF_DEFAULT_URL,
    ForexFactoryParseError,
    ForexFactoryRefresher,
    fetch_ff_calendar,
    parse_ff_json,
)
from bot_btc_1hr_kalshi.calendar.guard import CalendarGuard, GuardTick
from bot_btc_1hr_kalshi.calendar.loader import load_calendar, parse_calendar

__all__ = [
    "FF_DEFAULT_URL",
    "CalendarGuard",
    "ForexFactoryParseError",
    "ForexFactoryRefresher",
    "GuardTick",
    "Importance",
    "ScheduledEvent",
    "fetch_ff_calendar",
    "load_calendar",
    "parse_calendar",
    "parse_ff_json",
]
