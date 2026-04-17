"""PositionMonitor: always-on per-tick evaluation of every open position.

Priority (DESIGN.md §7.4a): early-cashout (≥99¢) > theta-net-target > adaptive-soft-stop.
Only one exit order in flight per position.
"""
