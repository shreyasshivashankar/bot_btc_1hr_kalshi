from __future__ import annotations

from pathlib import Path

import pytest

from bot_btc_1hr_kalshi.config import Settings, load_settings


def _write(p: Path, body: str) -> Path:
    p.write_text(body, encoding="utf-8")
    return p


PAPER_YAML = """\
mode: paper
feeds:
  kalshi:
    ws_url_env: BOT_BTC_1HR_KALSHI_WS_URL
    rest_url_env: BOT_BTC_1HR_KALSHI_REST_URL
    staleness_halt_ms: 2000
  coinbase:
    ws_url: ${CB_WS_URL:-wss://default.example/cb}
    staleness_halt_ms: 2000
  binance:
    ws_url: ${BINANCE_WS_URL}
    staleness_halt_ms: 2000
risk:
  kelly_fraction: 0.25
  max_position_notional_usd: 100
  max_daily_loss_pct: 0.05
  single_trade_drawdown_freeze_pct: 0.15
  reconcile_interval_sec: 60
  clock_drift_halt_ms: 1000
signal:
  bollinger_period_bars: 20
  bollinger_std_mult: 2.0
  min_signal_confidence: 0.6
monitor:
  early_cashout_price_cents: 99
  soft_stop:
    base_fraction: 0.10
    regime_multiplier_high_vol: 1.25
    regime_multiplier_trending: 1.40
    time_multiplier_late_window: 0.70
  theta_net_book_depth_threshold: 0.5
execution:
  maker_only_entry: true
  ioc_ladder_cents: [3, 6]
telemetry:
  bet_outcomes_logger: bot_btc_1hr_kalshi.bet_outcomes
  bq_dataset: bot_btc_1hr_kalshi_bet_outcomes
  bq_table: outcomes
"""


def test_loads_paper_config(tmp_path: Path) -> None:
    _write(tmp_path / "paper.yaml", PAPER_YAML)
    env = {"BINANCE_WS_URL": "wss://example.com/bn"}
    s = load_settings("paper", config_dir=tmp_path, env=env)
    assert isinstance(s, Settings)
    assert s.mode == "paper"
    assert s.feeds.binance.ws_url == "wss://example.com/bn"
    # default-fallback branch hit (CB_WS_URL not set)
    assert s.feeds.coinbase.ws_url == "wss://default.example/cb"


def test_missing_required_env_raises(tmp_path: Path) -> None:
    _write(tmp_path / "paper.yaml", PAPER_YAML)
    with pytest.raises(KeyError, match="BINANCE_WS_URL"):
        load_settings("paper", config_dir=tmp_path, env={})


def test_unknown_field_in_yaml_is_rejected(tmp_path: Path) -> None:
    from pydantic import ValidationError

    bad = PAPER_YAML.replace("mode: paper\n", "mode: paper\nstray: oops\n")
    _write(tmp_path / "paper.yaml", bad)
    with pytest.raises(ValidationError):
        load_settings("paper", config_dir=tmp_path, env={"BINANCE_WS_URL": "wss://x"})


def test_missing_file(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_settings("paper", config_dir=tmp_path, env={})


def test_loads_checked_in_dev_yaml() -> None:
    """The dev.yaml checked into the repo must validate as-is."""
    repo_cfg = Path(__file__).resolve().parent.parent.parent / "config"
    s = load_settings("dev", config_dir=repo_cfg, env={
        "BOT_BTC_1HR_KALSHI_WS_URL": "wss://kalshi.example/ws",
        "BOT_BTC_1HR_KALSHI_REST_URL": "https://kalshi.example/rest",
    })
    assert s.mode == "dev"
