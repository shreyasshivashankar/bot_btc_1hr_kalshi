"""Observability: structured logs, bet-outcome telemetry, admin audit, metrics.

Every closed bet emits a pydantic-validated BetOutcome to
`bot_btc_1hr_kalshi.bet_outcomes` (hard rule #6). Schema drift breaks tuning queries.
"""

from bot_btc_1hr_kalshi.obs.clock import Clock, ManualClock, SystemClock
from bot_btc_1hr_kalshi.obs.logging import (
    BET_OUTCOMES_LOGGER,
    bind_context,
    clear_context,
    configure,
    get_logger,
)
from bot_btc_1hr_kalshi.obs.schemas import (
    BetOutcome,
    DecisionRecord,
    ExitReason,
    Features,
    Position,
    RegimeTrend,
    RegimeVol,
    Side,
    Sizing,
    TrapName,
)

__all__ = [
    "BET_OUTCOMES_LOGGER",
    "BetOutcome",
    "Clock",
    "DecisionRecord",
    "ExitReason",
    "Features",
    "ManualClock",
    "Position",
    "RegimeTrend",
    "RegimeVol",
    "Side",
    "Sizing",
    "SystemClock",
    "TrapName",
    "bind_context",
    "clear_context",
    "configure",
    "get_logger",
]
