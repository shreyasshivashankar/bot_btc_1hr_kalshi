"""Research: deterministic tick-replay backtest, walk-forward, param sweeps, shadow mode.

Clock is injected -- no datetime.now() in trading code (hard rule #5).
"""

from bot_btc_1hr_kalshi.research.replay import (
    PendingEntry,
    ReplayOrchestrator,
    ReplayResult,
    replay,
)

__all__ = ["PendingEntry", "ReplayOrchestrator", "ReplayResult", "replay"]
