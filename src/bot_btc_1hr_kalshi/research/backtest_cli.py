"""Backtest CLI — replay a tick archive through the live trading graph.

Usage:
  python -m bot_btc_1hr_kalshi.research.backtest_cli \
      --archive-dir ./archive \
      --market KBTC-26APR1600-B60000 \
      --strike-usd 60000 \
      --from 2026-04-01T00 --to 2026-04-15T00 \
      --bankroll 1000

Loads JSONL hour-files produced by `archive.writer.ArchiveWriter`,
streams each FeedEvent through `research.replay.ReplayOrchestrator`
(which wraps the real signal -> risk -> OMS -> monitor path against a
PaperBroker), collects BetOutcomes emitted by the OMS, and prints the
metrics table from `research.backtest.compute_metrics`.

The replay uses a ManualClock seeded from the first event's ts_ns and
advanced by event.ts_ns on each handle — deterministic under replay
regardless of wall-clock (hard rule #5).

Settlement time: BTC hourly markets settle at the top of each UTC hour
(XX:00:00). The MTS helper computes minutes-to-next-hour-boundary from
the clock; override via --strike-minute if a market uses a different
settlement minute.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import json
import sys
from collections.abc import Iterable, MutableMapping
from pathlib import Path
from typing import Any

import structlog

from bot_btc_1hr_kalshi.archive.reader import iter_archive
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.market_data.types import FeedEvent
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.logging import BET_OUTCOMES_LOGGER
from bot_btc_1hr_kalshi.obs.schemas import BetOutcome
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.research.backtest import BacktestMetrics, compute_metrics
from bot_btc_1hr_kalshi.research.replay import ReplayOrchestrator
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import FeatureEngine


class _OutcomeCapture:
    """Structlog processor that collects bet_outcome records as BetOutcomes."""

    def __init__(self) -> None:
        self.outcomes: list[BetOutcome] = []

    def __call__(
        self, logger: Any, method: str, event_dict: MutableMapping[str, Any]
    ) -> MutableMapping[str, Any]:
        if event_dict.get("event") == "bet_outcome":
            data = {k: v for k, v in event_dict.items() if k not in ("event", "level")}
            # Caller may have added `logger` from a previous processor; BetOutcome
            # rejects unknown fields.
            data.pop("logger", None)
            # A malformed emit shouldn't kill a multi-day replay — skip it.
            with contextlib.suppress(Exception):
                self.outcomes.append(BetOutcome.model_validate(data))
        return event_dict


def _install_outcome_capture() -> _OutcomeCapture:
    cap = _OutcomeCapture()
    structlog.configure(
        processors=[cap, structlog.processors.JSONRenderer()],
        wrapper_class=structlog.make_filtering_bound_logger(20),
        logger_factory=structlog.PrintLoggerFactory(file=open("/dev/null", "w")),  # noqa: SIM115
        cache_logger_on_first_use=False,
    )
    # Reference BET_OUTCOMES_LOGGER to wire this module to the logger-name
    # constant even though the capture currently filters on the event name.
    _ = BET_OUTCOMES_LOGGER
    return cap


def _minutes_to_hour_top_fn(strike_minute: int = 0) -> Any:
    def mts(now_ns: int) -> float:
        seconds = now_ns // 1_000_000_000
        ts = dt.datetime.fromtimestamp(seconds, tz=dt.UTC)
        target = ts.replace(minute=strike_minute, second=0, microsecond=0)
        if target <= ts:
            target = target + dt.timedelta(hours=1)
        return (target - ts).total_seconds() / 60.0
    return mts


async def _replay_stream(
    orch: ReplayOrchestrator, events: Iterable[FeedEvent],
) -> int:
    n = 0
    for event in events:
        await orch.handle(event)
        n += 1
    return n


def run_backtest(
    *,
    archive_dir: Path,
    market_id: str,
    strike_usd: float,
    bankroll_usd: float,
    settings_mode: str = "paper",
    config_dir: Path | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    strike_minute: int = 0,
) -> tuple[BacktestMetrics, int]:
    cap = _install_outcome_capture()
    settings = load_settings(settings_mode, config_dir=config_dir)  # type: ignore[arg-type]

    clock = ManualClock(0)
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=bankroll_usd)
    broker = PaperBroker(clock=clock)
    oms = OMS(
        broker=broker, portfolio=portfolio, breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)

    from bot_btc_1hr_kalshi.app import App
    app = App(
        settings=settings, clock=clock, breakers=breakers,
        portfolio=portfolio, oms=oms, monitor=monitor,
    )
    feature_engine = FeatureEngine(
        timeframes=["1m", "5m", "15m", "1h", "1d"],
        bollinger_period=settings.signal.bollinger_period_bars,
        bollinger_std_mult=settings.signal.bollinger_std_mult,
    )
    orch = ReplayOrchestrator(
        app=app, broker=broker, clock=clock, market_id=market_id,
        feature_engine=feature_engine, strike_usd=strike_usd,
        minutes_to_settlement_fn=_minutes_to_hour_top_fn(strike_minute),
    )

    events = iter_archive(archive_dir, start_ns=start_ns, end_ns=end_ns)
    n_events = asyncio.run(_replay_stream(orch, events))

    metrics = compute_metrics(cap.outcomes)
    return metrics, n_events


def _parse_iso_hour_to_ns(s: str) -> int:
    ts = dt.datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=dt.UTC)
    return int(ts.timestamp() * 1_000_000_000)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="backtest_cli")
    p.add_argument("--archive-dir", required=True, type=Path)
    p.add_argument("--market", required=True)
    p.add_argument("--strike-usd", required=True, type=float)
    p.add_argument("--bankroll", type=float, default=1000.0)
    p.add_argument("--settings-mode", default="paper")
    p.add_argument("--config-dir", type=Path, default=None)
    p.add_argument("--from", dest="from_", default=None,
                   help="Inclusive start, format YYYY-MM-DDTHH (UTC).")
    p.add_argument("--to", dest="to_", default=None,
                   help="Exclusive end, format YYYY-MM-DDTHH (UTC).")
    p.add_argument("--strike-minute", type=int, default=0)
    p.add_argument("--json", action="store_true", help="Emit metrics as JSON.")
    ns = p.parse_args(argv)

    start_ns = _parse_iso_hour_to_ns(ns.from_) if ns.from_ else None
    end_ns = _parse_iso_hour_to_ns(ns.to_) if ns.to_ else None

    metrics, n_events = run_backtest(
        archive_dir=ns.archive_dir,
        market_id=ns.market,
        strike_usd=ns.strike_usd,
        bankroll_usd=ns.bankroll,
        settings_mode=ns.settings_mode,
        config_dir=ns.config_dir,
        start_ns=start_ns, end_ns=end_ns,
        strike_minute=ns.strike_minute,
    )

    if ns.json:
        payload = {
            "n_events_replayed": n_events,
            "n_bets": metrics.n_bets,
            "hit_rate": metrics.hit_rate,
            "total_net_pnl_usd": metrics.total_net_pnl_usd,
            "avg_net_pnl_usd": metrics.avg_net_pnl_usd,
            "sharpe_per_bet": metrics.sharpe_per_bet,
            "max_drawdown_usd": metrics.max_drawdown_usd,
            "per_trap_pnl_usd": metrics.per_trap_pnl_usd,
            "per_trap_count": metrics.per_trap_count,
            "per_exit_reason_count": metrics.per_exit_reason_count,
        }
        sys.stdout.write(json.dumps(payload, indent=2) + "\n")
    else:
        sys.stdout.write(
            f"events replayed:     {n_events}\n"
            f"bets closed:         {metrics.n_bets}\n"
            f"hit rate:            {metrics.hit_rate:.3f}\n"
            f"total net PnL (USD): {metrics.total_net_pnl_usd:.2f}\n"
            f"avg  net PnL (USD):  {metrics.avg_net_pnl_usd:.2f}\n"
            f"bet-level Sharpe:    {metrics.sharpe_per_bet:.3f}\n"
            f"max drawdown (USD):  {metrics.max_drawdown_usd:.2f}\n"
            f"per-trap PnL:        {metrics.per_trap_pnl_usd}\n"
            f"per-trap count:      {metrics.per_trap_count}\n"
            f"per-exit-reason:     {metrics.per_exit_reason_count}\n"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
