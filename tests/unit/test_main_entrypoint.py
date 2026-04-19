"""Smoke-tests the process entrypoint boot path (build_app / parse_args)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from bot_btc_1hr_kalshi.__main__ import build_app, main, parse_args
from bot_btc_1hr_kalshi.execution.broker.kalshi import KalshiBroker
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker

REPO_ROOT = Path(__file__).resolve().parents[2]


def _feed_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_WS_URL", "wss://example/ws")
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_REST_URL", "https://example/rest")


def _write_test_pem(path: Path) -> None:
    """Generate a throwaway RSA PEM so KalshiSigner construction succeeds.

    The key never goes anywhere — live-mode wiring just needs a readable,
    parseable private key to instantiate the signer at boot.
    """
    priv = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path.write_bytes(pem)


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


def test_build_app_paper_mode_wires_paper_broker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _feed_env(monkeypatch)
    app = build_app(mode="paper", bankroll=500.0, config_dir=str(REPO_ROOT / "config"))
    assert isinstance(app.broker, PaperBroker)
    assert app.kalshi_rest_client is None


def test_build_app_live_mode_rejects_missing_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _feed_env(monkeypatch)
    monkeypatch.delenv("BOT_BTC_1HR_KALSHI_API_KEY", raising=False)
    monkeypatch.delenv("BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH", raising=False)
    with pytest.raises(ValueError, match="live mode broker wiring requires"):
        build_app(mode="live", bankroll=500.0, config_dir=str(REPO_ROOT / "config"))


def test_build_app_live_mode_wires_kalshi_broker(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """With creds + rest_url resolved from env, live mode must instantiate
    KalshiBroker against a real httpx client (hard rule #2 prerequisite)."""
    _feed_env(monkeypatch)
    pem_path = tmp_path / "kalshi.pem"
    _write_test_pem(pem_path)
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_API_KEY", "test-key-id")
    monkeypatch.setenv("BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH", str(pem_path))

    app = build_app(mode="live", bankroll=500.0, config_dir=str(REPO_ROOT / "config"))
    try:
        assert isinstance(app.broker, KalshiBroker)
        assert app.kalshi_rest_client is not None
        # httpx normalizes base_url with a trailing slash.
        assert str(app.kalshi_rest_client.base_url).rstrip("/") == "https://example/rest"
    finally:
        # AsyncClient was constructed outside a loop but aclose() requires
        # one — close it inside a one-shot loop so the test doesn't leak
        # the underlying connection pool.
        asyncio.run(app.kalshi_rest_client.aclose())  # type: ignore[union-attr]
