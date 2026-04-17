"""Smoke-tests the process entrypoint boot path (build_app / parse_args)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from bot_btc_1hr_kalshi.__main__ import build_app, main, parse_args

REPO_ROOT = Path(__file__).resolve().parents[2]


def _feed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_WS_URL", "wss://example/ws")
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_REST_URL", "https://example/rest")


def test_parse_args_requires_mode() -> None:
    with pytest.raises(SystemExit):
        parse_args([])


def test_parse_args_accepts_paper() -> None:
    ns = parse_args(["--mode", "paper"])
    assert ns.mode == "paper"
    assert ns.port == 8080
    assert ns.host == "0.0.0.0"


def test_build_app_paper_mode_boots(monkeypatch: pytest.MonkeyPatch) -> None:
    _feed_env(monkeypatch)
    app = build_app(mode="paper", bankroll=1000.0, config_dir=str(REPO_ROOT / "config"))
    assert app.settings.mode == "paper"
    assert app.status()["bankroll_usd"] == 1000.0
    assert app.settings.calendar.lead_seconds > 0


def test_main_refuses_live_without_risk_signoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _feed_env(monkeypatch)
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_ADMIN_TOKEN", "token")
    monkeypatch.delenv("RISK_COMMITTEE_SIGNED", raising=False)
    os.chdir(REPO_ROOT)
    rc = main(["--mode", "live"])
    assert rc == 2


def test_main_refuses_without_admin_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _feed_env(monkeypatch)
    monkeypatch.delenv("BOT_BTC_1HR_KALSHI_ADMIN_TOKEN", raising=False)
    os.chdir(REPO_ROOT)
    rc = main(["--mode", "paper"])
    assert rc == 2
