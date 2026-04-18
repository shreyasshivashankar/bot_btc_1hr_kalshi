"""End-to-end smoke test for the backtest CLI.

Proves the full pipeline hangs together: write synthetic events via
ArchiveWriter -> run_backtest reads them through iter_archive -> feeds
them through ReplayOrchestrator -> compute_metrics returns a result.

The synthetic events don't trigger a real entry (we don't seed enough
spot history to fire a trap), so the metrics are zeros across the board.
That's fine — the point of this test is that the wiring is intact so
the CLI will run as soon as real captured data exists.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

from bot_btc_1hr_kalshi.archive.writer import ArchiveWriter
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate, SpotTick
from bot_btc_1hr_kalshi.obs.money import Micros
from bot_btc_1hr_kalshi.research.backtest_cli import main, run_backtest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
MARKET = "KBTC-26APR1600-B60000"


def _hour_ns(year: int, month: int, day: int, hour: int) -> int:
    return int(dt.datetime(year, month, day, hour, tzinfo=dt.UTC).timestamp()
               * 1_000_000_000)


def _seed_archive(tmp: Path) -> int:
    h0 = _hour_ns(2026, 4, 17, 15)
    with ArchiveWriter(tmp) as w:
        w.write(BookUpdate(
            seq=1, ts_ns=h0 + 1_000, market_id=MARKET,
            bids=(BookLevel(40, 200),),
            asks=(BookLevel(45, 200),),
            is_snapshot=True,
        ))
        for i in range(3):
            w.write(SpotTick(
                ts_ns=h0 + 2_000 + i,
                venue="coinbase",
                price_micros=Micros(60_000_000_000 + i * 1_000_000),
                size=1.0,
            ))
    return h0


def test_run_backtest_end_to_end(tmp_path: Path) -> None:
    _seed_archive(tmp_path)
    metrics, n_events = run_backtest(
        archive_dir=tmp_path,
        market_id=MARKET,
        strike_usd=60_000.0,
        bankroll_usd=1_000.0,
        config_dir=CONFIG_DIR,
    )
    # 1 book + 3 spot = 4 events driven through the orchestrator.
    assert n_events == 4
    # No trap should fire on 3 spot ticks (Bollinger needs the full
    # period of history), so no bets close.
    assert metrics.n_bets == 0


def test_cli_main_runs(tmp_path: Path, capsys: object) -> None:
    _seed_archive(tmp_path)
    rc = main([
        "--archive-dir", str(tmp_path),
        "--market", MARKET,
        "--strike-usd", "60000",
        "--config-dir", str(CONFIG_DIR),
    ])
    assert rc == 0
