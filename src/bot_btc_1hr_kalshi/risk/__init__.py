"""Risk: fractional-Kelly * ATR sizing, circuit breakers, VaR, exposure caps.

`risk.check()` is the pure gate for every order submission (Approve | Reject).
15% single-trade loss -> 60-min API lockout, no override (hard rule #3).
"""

from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.risk.check import Approve, Reject, RiskDecision, RiskInput, check
from bot_btc_1hr_kalshi.risk.kelly import kelly_contracts

__all__ = [
    "Approve",
    "BreakerState",
    "Reject",
    "RiskDecision",
    "RiskInput",
    "check",
    "kelly_contracts",
]
