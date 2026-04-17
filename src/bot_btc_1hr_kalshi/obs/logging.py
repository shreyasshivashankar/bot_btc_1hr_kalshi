"""Structured logging setup.

JSON lines to stdout — Cloud Logging parses each line into a LogEntry. Bet
outcomes go through a dedicated logger name (`bot_btc_1hr_kalshi.bet_outcomes`)
so the log sink can route them to BigQuery.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

BET_OUTCOMES_LOGGER = "bot_btc_1hr_kalshi.bet_outcomes"


def configure(level: str = "INFO", *, development: bool = False) -> None:
    """Install structlog's JSON processor chain. Idempotent — safe to call twice."""
    level_no = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    renderer: structlog.types.Processor
    if development:
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level_no),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial: Any) -> structlog.stdlib.BoundLogger:
    """Get a bound structlog logger. Pass permanent context as kwargs."""
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    if initial:
        logger = logger.bind(**initial)
    return logger


def bind_context(**kwargs: Any) -> None:
    """Bind key/value pairs to the current contextvar so every subsequent log
    line in this task includes them. Typical keys: trace_id, bet_id."""
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    structlog.contextvars.clear_contextvars()
