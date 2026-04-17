"""Risk: fractional-Kelly * ATR sizing, circuit breakers, VaR, exposure caps.

`risk.check()` is the pure gate for every order submission (Approve | Reject).
15% single-trade loss → 60-min API lockout, no override (hard rule #3).
"""
