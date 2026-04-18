"""Process entrypoint. Wires config -> feeds -> risk -> execution -> monitor -> admin.

Usage:
    python -m bot_btc_1hr_kalshi --mode paper [--port 8080] [--bankroll 50] \
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
import signal
import sys
from pathlib import Path
from typing import Any

import httpx
import uvicorn

from bot_btc_1hr_kalshi.admin.server import create_app as create_admin_app
from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.archive.writer import ArchiveWriter
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.config.settings import FeedSettings, Mode
from bot_btc_1hr_kalshi.execution.broker.base import Broker
from bot_btc_1hr_kalshi.execution.broker.kalshi_signer import KalshiSigner
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.broker.shadow import ShadowBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.feedloop import (
    run_forever as run_feed_forever,
)
from bot_btc_1hr_kalshi.feedloop import (
    ws_connect_kalshi_signed,
    ws_connect_websockets,
)
from bot_btc_1hr_kalshi.market_data.bars import MultiTimeframeBus
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.kalshi_rest import kalshi_date_header_probe
from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotOracle
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.activity import ActivityTracker
from bot_btc_1hr_kalshi.obs.clock import SystemClock
from bot_btc_1hr_kalshi.obs.lifecycle import LifecycleEmitter
from bot_btc_1hr_kalshi.obs.logging import configure as configure_logging
from bot_btc_1hr_kalshi.obs.logging import get_logger
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breaker_store import JsonFileBreakerStore, NullBreakerStore
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.risk.clock_drift import ClockDriftMonitor

BREAKER_STATE_PATH_ENV = "BOT_BTC_1HR_KALSHI_BREAKER_STATE_PATH"
ARCHIVE_DIR_ENV = "BOT_BTC_1HR_KALSHI_ARCHIVE_DIR"
COINBASE_WS_URL_ENV = "BOT_BTC_1HR_KALSHI_COINBASE_WS_URL"
KRAKEN_WS_URL_ENV = "BOT_BTC_1HR_KALSHI_KRAKEN_WS_URL"
SERIES_TICKER_ENV = "BOT_BTC_1HR_KALSHI_SERIES_TICKER"
KALSHI_API_KEY_ENV = "BOT_BTC_1HR_KALSHI_API_KEY"
KALSHI_PRIVATE_KEY_PATH_ENV = "BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH"

DEFAULT_ADMIN_TOKEN_ENV = "BOT_BTC_1HR_KALSHI_ADMIN_TOKEN"  # noqa: S105 — env var name, not a secret
PAPER_LIVE_MODES: tuple[Mode, ...] = ("dev", "paper", "shadow", "live")
MODES_WITH_FEED_LOOP: tuple[Mode, ...] = ("paper", "shadow", "live")


def _resolve_feed_url(feed: FeedSettings) -> str | None:
    """Resolve a feed's WS URL from explicit value or env-var indirection."""
    if feed.ws_url:
        return feed.ws_url
    if feed.ws_url_env:
        return os.getenv(feed.ws_url_env)
    return None


def _resolve_rest_url(feed: FeedSettings) -> str | None:
    if feed.rest_url:
        return feed.rest_url
    if feed.rest_url_env:
        return os.getenv(feed.rest_url_env)
    return None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="bot_btc_1hr_kalshi")
    p.add_argument("--mode", choices=PAPER_LIVE_MODES, required=True)
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--host", default="0.0.0.0")  # noqa: S104 — Cloud Run needs this
    # Session bankroll. Each container start resets to this value — stop/start
    # the Cloud Run service to begin a new session. Sized intra-session by
    # fractional-Kelly against remaining bankroll (via Portfolio).
    p.add_argument("--bankroll", type=float, default=50.0)
    p.add_argument("--admin-token-env", default=DEFAULT_ADMIN_TOKEN_ENV)
    p.add_argument("--config-dir", default=None)
    return p.parse_args(argv)


def _broker_for_mode(mode: Mode, *, clock: SystemClock) -> Broker:
    """Select the broker for the given mode.

    dev / paper: local paper broker (in-proc fill simulation).
    shadow:      no-wire shadow broker (records intents only — hard rule #2).
    live:        KalshiBroker class exists at execution/broker/kalshi.py but
                 is not wired here yet - the DI (httpx.AsyncClient + Secret
                 Manager-backed key loading) is a separate change.

    Live is deliberately not wired in this function yet: the Kalshi broker
    class is implemented and tested, but plumbing an httpx client plus
    Secret-Manager-resolved keys through to it is a discrete change. Raise
    explicitly so `--mode live` fails loudly rather than silently running
    against PaperBroker — which would be a hard-rule-#2 violation waiting
    to happen.
    """
    if mode in ("dev", "paper"):
        return PaperBroker(clock=clock)
    if mode == "shadow":
        return ShadowBroker(clock=clock)
    if mode == "live":
        raise NotImplementedError(
            "live broker wiring is not yet complete — KalshiBroker class "
            "exists (execution/broker/kalshi.py) but requires httpx client "
            "+ Secret Manager key loading DI in __main__",
        )
    raise ValueError(f"unknown mode: {mode}")


def build_app(
    *,
    mode: Mode,
    bankroll: float,
    config_dir: str | None,
) -> App:
    settings = load_settings(
        mode,
        config_dir=Path(config_dir) if config_dir else None,
    )
    clock = SystemClock()
    state_path = os.getenv(BREAKER_STATE_PATH_ENV)
    store = JsonFileBreakerStore(state_path) if state_path else NullBreakerStore()
    breakers = BreakerState(store=store)
    portfolio = Portfolio(bankroll_usd=bankroll)
    broker: Broker = _broker_for_mode(mode, clock=clock)
    lifecycle = LifecycleEmitter(clock=clock)
    activity = ActivityTracker(boot_ns=clock.now_ns())
    archive_dir = os.getenv(ARCHIVE_DIR_ENV)
    archive_writer = ArchiveWriter(Path(archive_dir)) if archive_dir else None
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
        lifecycle=lifecycle,
        activity=activity,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)
    return App(
        settings=settings,
        clock=clock,
        breakers=breakers,
        portfolio=portfolio,
        oms=oms,
        monitor=monitor,
        broker=broker,
        lifecycle=lifecycle,
        activity=activity,
        archive_writer=archive_writer,
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

    # Install our own SIGTERM/SIGINT handlers BEFORE uvicorn's so that Cloud
    # Run's 10s shutdown grace period is spent halting trading first and
    # letting in-flight exits drain, rather than killing mid-cycle.
    # Uvicorn installs its handlers from inside .serve(); stub that out and
    # forward to server.should_exit after we halt the app.
    # uvicorn.Server installs its own SIGTERM/SIGINT handlers inside serve();
    # disable that so ours are authoritative.
    setattr(server, "install_signal_handlers", lambda: None)  # noqa: B010
    loop = asyncio.get_running_loop()

    feed_task: asyncio.Task[None] | None = None
    drift_task: asyncio.Task[None] | None = None
    spot_task: asyncio.Task[None] | None = None
    rest_client: httpx.AsyncClient | None = None

    def _on_term() -> None:
        log.warning(
            "shutdown.sigterm_received",
            open_positions=len(app.portfolio.open_positions),
            trading_halted_before=app.trading_halted,
        )
        app.halt(reason="sigterm")
        if feed_task is not None:
            feed_task.cancel()
        if drift_task is not None:
            drift_task.cancel()
        if spot_task is not None:
            spot_task.cancel()
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_term)

    feed_task, drift_task, spot_task, rest_client = _start_feed_loop_if_enabled(
        app, log
    )

    log.info("boot.serving", mode=app.settings.mode, host=host, port=port)
    try:
        await server.serve()
    finally:
        for name, task in (
            ("feed-loop", feed_task),
            ("clock-drift", drift_task),
            ("spot-oracle", spot_task),
        ):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                log.warning("shutdown.task_error", task=name, error=str(exc))
        if rest_client is not None:
            await rest_client.aclose()
        if app.archive_writer is not None:
            app.archive_writer.close()
        if app.portfolio.open_positions:
            log.error(
                "shutdown.open_positions_remain",
                count=len(app.portfolio.open_positions),
                note="hard rule #10: container should not exit with open positions; run scripts/flatten.sh first",
            )


def _start_feed_loop_if_enabled(
    app: App, log: Any,
) -> tuple[
    asyncio.Task[None] | None,
    asyncio.Task[None] | None,
    asyncio.Task[None] | None,
    httpx.AsyncClient | None,
]:
    """Spawn the live feed loop + clock-drift + spot-oracle unless in dev.

    Returns (feed_task, drift_task, spot_task, http_client) so the caller
    can cancel each task and close the client on shutdown. All are None
    when the live loop is disabled.
    """
    if app.settings.mode not in MODES_WITH_FEED_LOOP:
        log.info("boot.feed_loop_disabled", mode=app.settings.mode)
        return None, None, None, None
    if app.broker is None:
        log.error("boot.feed_loop_missing_broker", mode=app.settings.mode)
        return None, None, None, None

    kalshi_ws = _resolve_feed_url(app.settings.feeds.kalshi)
    rest_url = _resolve_rest_url(app.settings.feeds.kalshi)
    coinbase_ws = _resolve_feed_url(app.settings.feeds.coinbase) or os.getenv(COINBASE_WS_URL_ENV)
    kraken_ws = _resolve_feed_url(app.settings.feeds.kraken) or os.getenv(KRAKEN_WS_URL_ENV)
    series_ticker = os.getenv(SERIES_TICKER_ENV, "KXBTC")

    missing = [
        name for name, val in (
            ("kalshi.ws_url", kalshi_ws),
            ("kalshi.rest_url", rest_url),
            ("coinbase.ws_url", coinbase_ws),
            ("kraken.ws_url", kraken_ws),
        ) if not val
    ]
    if missing or kalshi_ws is None or rest_url is None or coinbase_ws is None or kraken_ws is None:
        log.error("boot.feed_loop_missing_config", missing=missing)
        return None, None, None, None

    rest_client = httpx.AsyncClient(base_url=rest_url, timeout=10.0)

    # Kalshi's WS handshake is signed — same KALSHI-ACCESS-* header scheme as
    # REST (see execution/broker/kalshi_signer.py). In dev we may not have
    # creds wired yet, so fall back to the unsigned connector and log a
    # warning; in paper / shadow / live the server rejects an unsigned
    # handshake with HTTP 401, which will surface via the feed reconnect log.
    kalshi_ws_connect: WSConnect = ws_connect_websockets
    api_key = os.getenv(KALSHI_API_KEY_ENV)
    private_key_path = os.getenv(KALSHI_PRIVATE_KEY_PATH_ENV)
    if api_key and private_key_path:
        try:
            pem_bytes = Path(private_key_path).read_bytes()
            signer = KalshiSigner(
                api_key_id=api_key,
                private_key_pem=pem_bytes,
                clock=app.clock,
            )
            kalshi_ws_connect = ws_connect_kalshi_signed(signer)
            log.info(
                "boot.kalshi_ws_signed",
                key_id=api_key[:8] + "…",
                key_path=private_key_path,
            )
        except OSError as exc:
            log.error("boot.kalshi_key_read_failed", path=private_key_path, error=str(exc))
            return None, None, None, rest_client
        except ValueError as exc:
            log.error("boot.kalshi_signer_invalid", error=str(exc))
            return None, None, None, rest_client
    else:
        log.warning(
            "boot.kalshi_ws_unsigned",
            note="BOT_BTC_1HR_KALSHI_API_KEY / _PRIVATE_KEY_PATH not set; WS handshake will be rejected by live Kalshi",
        )

    # Persistent SpotOracle (Slice 6). Owns the Coinbase + Kraken WS for the
    # lifetime of the container, not the hourly session — so hour-rolls no
    # longer start with a blank spot reference (which used to force
    # alphabetical strike tiebreak and pick deep-ITM markets).
    coinbase_feed = SpotFeed(
        name="coinbase",
        ws_url=coinbase_ws,
        clock=app.clock,
        ws_connect=ws_connect_websockets,
        staleness=StalenessTracker(
            name="coinbase",
            clock=app.clock,
            threshold_ms=app.settings.feeds.coinbase.staleness_halt_ms,
        ),
        parse=coinbase_parser(app.clock),
        subscribe=build_coinbase_subscribe(["BTC-USD"]),
    )
    kraken_feed = SpotFeed(
        name="kraken",
        ws_url=kraken_ws,
        clock=app.clock,
        ws_connect=ws_connect_websockets,
        staleness=StalenessTracker(
            name="kraken",
            clock=app.clock,
            threshold_ms=app.settings.feeds.kraken.staleness_halt_ms,
        ),
        parse=kraken_parser(app.clock),
        subscribe=build_kraken_subscribe(["BTC/USD"]),
    )
    spot_oracle = SpotOracle(
        primary=coinbase_feed,
        confirmation=kraken_feed,
        clock=app.clock,
    )
    app.spot_oracle = spot_oracle
    spot_task = asyncio.create_task(spot_oracle.run(), name="spot-oracle")

    # Multi-timeframe bar bus (Slice 7). Timeframes mirror DESIGN.md §5 —
    # 1m/5m/15m for intra-hour features, 1h for the top-down alignment veto,
    # 1d for the 24h runaway-train breaker. Fed from the oracle's primary
    # callback; downstream feature consumers wire in on later slices.
    bar_bus = MultiTimeframeBus(tf_secs=[60, 300, 900, 3600, 86400])
    app.bar_bus = bar_bus
    spot_oracle.subscribe_primary(bar_bus.ingest)

    task = asyncio.create_task(
        run_feed_forever(
            app=app,
            broker=app.broker,
            rest_http_client=rest_client,
            clock=app.clock,
            kalshi_ws_url=kalshi_ws,
            spot_oracle=spot_oracle,
            ws_connect=kalshi_ws_connect,
            rest_base="",  # rest_url already includes /trade-api/v2
            series_ticker=series_ticker,
        ),
        name="feed-loop",
    )

    # Clock-drift monitor (hard rule #5). Uses Kalshi's own `Date` response
    # header as the reference clock — what matters is agreement with the
    # server we're about to sign requests to, not UTC ground truth.
    drift_monitor = ClockDriftMonitor(
        clock=app.clock,
        breakers=app.breakers,
        probe=kalshi_date_header_probe(rest_client),
        interval_sec=30.0,
        threshold_ms=app.settings.risk.clock_drift_halt_ms,
    )
    drift_task = asyncio.create_task(drift_monitor.run(), name="clock-drift")

    log.info(
        "boot.feed_loop_started",
        series_ticker=series_ticker,
        kalshi_ws=kalshi_ws,
        clock_drift_threshold_ms=app.settings.risk.clock_drift_halt_ms,
        spot_staleness_threshold_ms=app.settings.risk.spot_staleness_halt_ms,
    )
    return task, drift_task, spot_task, rest_client


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
