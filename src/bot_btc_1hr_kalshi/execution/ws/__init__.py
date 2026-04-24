"""Kalshi private WS execution-channel stream.

Subscribes the trading-API WS to the `fill`, `user_orders`, and
`market_positions` channels so the OMS can move from sync POST-body fill
parsing toward fire-and-track. This package is the additive observation
layer; the OMS pivot lands in a follow-up PR (gated on real-frame
fixtures captured during paper soak — see CLAUDE.md "Build status").
"""

from bot_btc_1hr_kalshi.execution.ws.parser import (
    KalshiExecParseError,
    build_exec_subscribe,
    parse_exec_frame,
)
from bot_btc_1hr_kalshi.execution.ws.stream import (
    EXEC_CHANNELS,
    KalshiExecutionStream,
)
from bot_btc_1hr_kalshi.execution.ws.types import (
    ExecFillEvent,
    ExecOrderUpdate,
    ExecPositionSnapshot,
    ExecutionEvent,
)

__all__ = [
    "EXEC_CHANNELS",
    "ExecFillEvent",
    "ExecOrderUpdate",
    "ExecPositionSnapshot",
    "ExecutionEvent",
    "KalshiExecParseError",
    "KalshiExecutionStream",
    "build_exec_subscribe",
    "parse_exec_frame",
]
