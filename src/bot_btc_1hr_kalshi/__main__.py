"""Process entrypoint. Wires config -> feeds -> risk -> execution -> monitor -> admin.

Usage:
    python -m bot_btc_1hr_kalshi --mode paper [--port 8080] [--bankroll 1000] \
        [--admin-token-env BOT_BTC_1HR_KALSHI_ADMIN_TOKEN]

The current boot path covers `dev` and `paper` modes: it loads config, assembles
the App graph (portfolio, breakers, paper broker, OMS, monitor) and exposes the
admin HTTP surface. Live market-data feeds are wired in a later slice; replay
mode is in `research.replay` (Slice 1G).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import uvicorn

from bot_btc_1hr_kalshi.admin.server import create_app as create_admin_app
from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.config.settings import Mode
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import SystemClock
from bot_btc_1hr_kalshi.obs.logging import configure as configure_logging
from bot_btc_1hr_kalshi.obs.logging import get_logger
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState

DEFAULT_ADMIN_TOKEN_ENV = "BOT_BTC_1HR_KALSHI_ADMIN_TOKEN"  # noqa: S105 — env var name, not a secret
PAPER_LIVE_MODES: tuple[Mode, ...] = ("dev", "paper", "shadow", "live")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bot_btc_1hr_kalshi")
    p.add_argument("--mode", choices=PAPER_LIVE_MODES, required=True)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--host", default="0.0.0.0")  # noqa: S104 — Cloud Run needs this
    p.add_argument("--bankroll", type=float, default=1000.0)
    p.add_argument("--admin-token-env", default=DEFAULT_ADMIN_TOKEN_ENV)
    p.add_argument("--config-dir", default=None)
    return p.parse_args(argv)


def build_app(
    *,
    mode: Mode,
    bankroll: float,
    config_dir: str | None,
) -> App:
    from pathlib import Path

    settings = load_settings(
        mode,
        config_dir=Path(config_dir) if config_dir else None,
    )
    clock = SystemClock()
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=bankroll)
    broker = PaperBroker(clock=clock)
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)
    return App(
        settings=settings,
        clock=clock,
        breakers=breakers,
        portfolio=portfolio,
        oms=oms,
        monitor=monitor,
    )


async def serve(app: App, *, admin_token: str, host: str, port: int) -> None:
    log = get_logger("bot_btc_1hr_kalshi.boot")
    fastapi_app = create_admin_app(app, admin_token=admin_token)
    cfg = uvicorn.Config(
        fastapi_app,
        host=host,
        port=port,
        log_level="info",
        access_log=False,
        lifespan="on",
    )
    server = uvicorn.Server(cfg)
    log.info("boot.serving", mode=app.settings.mode, host=host, port=port)
    await server.serve()


def main(argv: list[str] | None = None) -> int:
    ns = parse_args(argv)
    configure_logging(level="INFO", development=(ns.mode == "dev"))

    if ns.mode == "live" and os.getenv("RISK_COMMITTEE_SIGNED") != "yes":
        sys.stderr.write(
            "live mode requires RISK_COMMITTEE_SIGNED=yes (hard rule #2).\n"
        )
        return 2

    token = os.getenv(ns.admin_token_env)
    if not token:
        sys.stderr.write(f"admin token env var {ns.admin_token_env} is not set.\n")
        return 2

    app = build_app(mode=ns.mode, bankroll=ns.bankroll, config_dir=ns.config_dir)
    asyncio.run(serve(app, admin_token=token, host=ns.host, port=ns.port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
