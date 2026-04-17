"""Structured economic calendar poller (hard rule #8).

Tier-1 events trigger a pre-emptive flatten at T-lead (default 60s). No NLP
triggers — only the curated YAML schedule or the human kill-switch.
"""

from bot_btc_1hr_kalshi.calendar.events import Importance, ScheduledEvent
from bot_btc_1hr_kalshi.calendar.guard import CalendarGuard, GuardTick
from bot_btc_1hr_kalshi.calendar.loader import load_calendar, parse_calendar

__all__ = [
    "CalendarGuard",
    "GuardTick",
    "Importance",
    "ScheduledEvent",
    "load_calendar",
    "parse_calendar",
]
