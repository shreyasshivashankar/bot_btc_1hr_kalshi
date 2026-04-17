"""Structured economic-calendar events.

A ScheduledEvent is the parsed representation of a single row in an upstream
calendar (manually curated YAML or scraped from ForexFactory). Only tier-1
events ever trigger a pre-emptive flatten (hard rule #8 / RISK.md §6); lower
tiers are kept for decision telemetry but do not affect execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Importance = Literal["tier_1", "tier_2", "other"]


@dataclass(frozen=True, slots=True)
class ScheduledEvent:
    """A single calendar event anchored at an absolute wall-clock instant (ns)."""

    name: str
    ts_ns: int
    importance: Importance
    source: str = "manual"

    @property
    def is_tier_one(self) -> bool:
        return self.importance == "tier_1"
