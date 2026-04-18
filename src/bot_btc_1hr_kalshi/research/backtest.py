"""Backtest metrics — compute Sharpe / maxDD / hit rate / per-trap attribution
from a sequence of closed BetOutcomes.

SLICE 5 SKELETON. The metrics math is real and tested. What is NOT yet wired:

  * A CLI that reads a captured tick archive from
    `gs://bot-btc-1hr-kalshi-tick-archive-*` and drives a replay to produce
    the outcome stream. The current `make backtest` target stays a
    placeholder until tick capture is live — see docs/DESIGN.md §10.
  * Benchmark attribution (per-regime, per-time-of-day). Deferred because
    we have no archived regime-labeled tick data to validate against.

The hit-rate definition: a bet is a "hit" iff `net_pnl_usd > 0`. Zero PnL
is not a hit (deliberate — fees-only exits should not flatter the rate).
Sharpe is computed on per-bet net PnL with 0% risk-free rate; this is
bet-level Sharpe, not daily, so it is NOT comparable to CTA-style
annualized figures without a time-scaling step documented at call site.
Max drawdown is measured in dollars on the cumulative PnL curve; for
portfolio-relative drawdown the caller divides by starting bankroll.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from bot_btc_1hr_kalshi.obs.schemas import BetOutcome


@dataclass(frozen=True, slots=True)
class BacktestMetrics:
    n_bets: int
    n_winners: int
    hit_rate: float
    total_net_pnl_usd: float
    avg_net_pnl_usd: float
    sharpe_per_bet: float
    max_drawdown_usd: float
    per_trap_pnl_usd: dict[str, float] = field(default_factory=dict)
    per_trap_count: dict[str, int] = field(default_factory=dict)
    per_exit_reason_count: dict[str, int] = field(default_factory=dict)


def compute_metrics(outcomes: list[BetOutcome]) -> BacktestMetrics:
    n = len(outcomes)
    if n == 0:
        return BacktestMetrics(
            n_bets=0, n_winners=0, hit_rate=0.0,
            total_net_pnl_usd=0.0, avg_net_pnl_usd=0.0,
            sharpe_per_bet=0.0, max_drawdown_usd=0.0,
        )

    pnls = [o.net_pnl_usd for o in outcomes]
    n_winners = sum(1 for p in pnls if p > 0)
    total = sum(pnls)
    avg = total / n

    # Bet-level Sharpe. With a single bet the stdev is 0 — report 0.0
    # rather than infinity; a one-bet "Sharpe" is not meaningful anyway.
    if n > 1:
        var = sum((p - avg) ** 2 for p in pnls) / (n - 1)
        std = math.sqrt(var)
        sharpe = (avg / std) if std > 0 else 0.0
    else:
        sharpe = 0.0

    # Max drawdown on the cumulative PnL curve.
    peak = 0.0
    cum = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    per_trap_pnl: dict[str, float] = defaultdict(float)
    per_trap_count: Counter[str] = Counter()
    per_exit: Counter[str] = Counter()
    for o in outcomes:
        per_trap_pnl[o.trap] += o.net_pnl_usd
        per_trap_count[o.trap] += 1
        per_exit[o.exit_reason] += 1

    return BacktestMetrics(
        n_bets=n,
        n_winners=n_winners,
        hit_rate=n_winners / n,
        total_net_pnl_usd=total,
        avg_net_pnl_usd=avg,
        sharpe_per_bet=sharpe,
        max_drawdown_usd=max_dd,
        per_trap_pnl_usd=dict(per_trap_pnl),
        per_trap_count=dict(per_trap_count),
        per_exit_reason_count=dict(per_exit),
    )
