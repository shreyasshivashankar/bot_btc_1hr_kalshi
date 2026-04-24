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
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import httpx
import uvicorn

from bot_btc_1hr_kalshi.admin.server import create_app as create_admin_app
from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.archive.writer import ArchiveWriter
from bot_btc_1hr_kalshi.calendar import (
    CalendarGuard,
    ForexFactoryRefresher,
    ScheduledEvent,
    load_calendar,
)
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.config.settings import FeedSettings, Mode, Settings
from bot_btc_1hr_kalshi.execution.broker.base import Broker
from bot_btc_1hr_kalshi.execution.broker.kalshi import KalshiBroker
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
from bot_btc_1hr_kalshi.market_data.derivatives_oracle import DerivativesOracle
from bot_btc_1hr_kalshi.market_data.feeds.bybit import (
    BYBIT_SOURCE,
    build_bybit_subscribe,
    bybit_liquidation_parser,
    bybit_liquidation_topic,
    bybit_tickers_parser,
    bybit_tickers_topic,
)
from bot_btc_1hr_kalshi.market_data.feeds.coinglass import CoinglassPoller
from bot_btc_1hr_kalshi.market_data.feeds.coinglass_heatmap import (
    CoinglassHeatmapPoller,
)
from bot_btc_1hr_kalshi.market_data.feeds.derivatives import DerivativesFeed
from bot_btc_1hr_kalshi.market_data.feeds.hyperliquid import (
    HYPERLIQUID_SOURCE,
    build_hyperliquid_subscribe,
    hyperliquid_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.kalshi import WSConnect
from bot_btc_1hr_kalshi.market_data.feeds.spot import (
    SpotFeed,
    build_coinbase_subscribe,
    build_kraken_subscribe,
    coinbase_parser,
    kraken_parser,
)
from bot_btc_1hr_kalshi.market_data.feeds.staleness import StalenessTracker
from bot_btc_1hr_kalshi.market_data.feeds.whale_alert import WhaleAlertPoller
from bot_btc_1hr_kalshi.market_data.kalshi_rest import kalshi_date_header_probe
from bot_btc_1hr_kalshi.market_data.spot_oracle import SpotOracle
from bot_btc_1hr_kalshi.market_data.types import (
    LiquidationEvent,
    LiquidationHeatmapSample,
    OpenInterestSample,
    WhaleAlertSample,
)
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
from bot_btc_1hr_kalshi.signal.features import FeatureEngine

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


def _broker_for_mode(
    mode: Mode,
    *,
    settings: Settings,
    clock: SystemClock,
) -> tuple[Broker, httpx.AsyncClient | None]:
    """Select the broker for the given mode.

    dev / paper: local paper broker (in-proc fill simulation).
    shadow:      no-wire shadow broker (records intents only — hard rule #2).
    live:        KalshiBroker with an httpx.AsyncClient + KalshiSigner
                 constructed from Secret-Manager-backed env vars.

    Returns `(broker, client_to_close)`. The caller is responsible for
    `aclose()`-ing the returned client on shutdown; it is None for
    in-proc brokers that hold no network resources.
    """
    if mode in ("dev", "paper"):
        return PaperBroker(clock=clock), None
    if mode == "shadow":
        return ShadowBroker(clock=clock), None
    if mode == "live":
        return _build_kalshi_broker(settings=settings, clock=clock)
    raise ValueError(f"unknown mode: {mode}")


def _build_kalshi_broker(
    *,
    settings: Settings,
    clock: SystemClock,
) -> tuple[KalshiBroker, httpx.AsyncClient]:
    """Instantiate the live Kalshi REST broker from env-var-backed creds.

    Fails loudly on missing API key / private key path / REST URL — live
    mode must not silently degrade to an unauthenticated session. The
    private key is mounted from Secret Manager as a file; we read the
    path from `BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH` (hard rule #4).
    """
    api_key = os.getenv(KALSHI_API_KEY_ENV)
    priv_path = os.getenv(KALSHI_PRIVATE_KEY_PATH_ENV)
    rest_url = _resolve_rest_url(settings.feeds.kalshi)
    missing = [
        name
        for name, val in (
            (KALSHI_API_KEY_ENV, api_key),
            (KALSHI_PRIVATE_KEY_PATH_ENV, priv_path),
            ("kalshi.rest_url (settings or env)", rest_url),
        )
        if not val
    ]
    if missing:
        raise ValueError("live mode broker wiring requires: " + ", ".join(missing))
    # `missing` being empty means each of the three is set; narrow for mypy.
    pem_bytes = Path(cast(str, priv_path)).read_bytes()
    signer = KalshiSigner(
        api_key_id=cast(str, api_key),
        private_key_pem=pem_bytes,
        clock=clock,
    )
    client = httpx.AsyncClient(base_url=cast(str, rest_url), timeout=10.0)
    broker = KalshiBroker(client=client, signer=signer, clock=clock)
    return broker, client


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
    broker, kalshi_rest_client = _broker_for_mode(
        mode,
        settings=settings,
        clock=clock,
    )
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
        kalshi_rest_client=kalshi_rest_client,
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
    calendar_task: asyncio.Task[None] | None = None
    ff_refresh_task: asyncio.Task[None] | None = None
    ff_client: httpx.AsyncClient | None = None
    coinglass_task: asyncio.Task[None] | None = None
    coinglass_client: httpx.AsyncClient | None = None
    heatmap_task: asyncio.Task[None] | None = None
    heatmap_client: httpx.AsyncClient | None = None
    whale_task: asyncio.Task[None] | None = None
    whale_client: httpx.AsyncClient | None = None
    derivatives_oracle_task: asyncio.Task[None] | None = None
    rest_client: httpx.AsyncClient | None = None

    def _on_term() -> None:
        log.warning(
            "shutdown.sigterm_received",
            open_positions=len(app.portfolio.open_positions),
            trading_halted_before=app.trading_halted,
        )
        app.halt(reason="sigterm")
        for t in (
            feed_task,
            drift_task,
            spot_task,
            calendar_task,
            ff_refresh_task,
            coinglass_task,
            heatmap_task,
            whale_task,
            derivatives_oracle_task,
        ):
            if t is not None:
                t.cancel()
        server.should_exit = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _on_term)

    feed_task, drift_task, spot_task, rest_client = _start_feed_loop_if_enabled(app, log)
    calendar_task, ff_refresh_task, ff_client = _start_calendar_if_enabled(app, log)
    coinglass_task, coinglass_client = _start_coinglass_if_enabled(app, log)
    heatmap_task, heatmap_client = _start_coinglass_heatmap_if_enabled(app, log)
    whale_task, whale_client = _start_whale_alert_if_enabled(app, log)
    derivatives_oracle_task = _start_derivatives_oracle_if_enabled(app, log)

    # Supervise the background tasks. If any dies unexpectedly (not via
    # cancellation), the container must restart — silently running without
    # a spot oracle or feed loop produces zombie state (`discovery_waiting_on_spot`
    # forever). Cloud Run's min=1 guarantees a fresh container when uvicorn
    # exits. This matches the SpotOracle docstring's stated contract:
    # "if either one raises unexpectedly we want the App to crash so Cloud
    # Run restarts the container rather than silently running half-deaf."
    def _make_supervisor(
        name: str,
        *,
        halt_on_death: bool = True,
    ) -> Callable[[asyncio.Task[None]], None]:
        def _on_done(task: asyncio.Task[None]) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is None:
                log.error("boot.background_task_exited", task=name)
            else:
                log.error(
                    "boot.background_task_died",
                    task=name,
                    error=str(exc),
                    exc_type=type(exc).__name__,
                )
            if halt_on_death:
                app.halt(reason=f"{name}_died")
                server.should_exit = True

        return _on_done

    # Critical tasks: death of any halts the container (Cloud Run restarts).
    for name, task in (
        ("feed-loop", feed_task),
        ("clock-drift", drift_task),
        ("spot-oracle", spot_task),
        ("calendar-guard", calendar_task),
        ("forex-factory-refresh", ff_refresh_task),
    ):
        if task is not None:
            task.add_done_callback(_make_supervisor(name))
    # Non-critical: Coinglass OI is observational (Slice 11 P2 shadow).
    # Logging its death is sufficient — traps don't gate on OI yet, so
    # a dead poller must not force a container restart (that would let
    # a third-party API outage take the trading graph down with it).
    if coinglass_task is not None:
        coinglass_task.add_done_callback(_make_supervisor("coinglass-oi", halt_on_death=False))
    if heatmap_task is not None:
        heatmap_task.add_done_callback(
            _make_supervisor("coinglass-heatmap", halt_on_death=False)
        )
    # Whale Alert is observational-only (Slice 11 P4 shadow) — treat as
    # non-critical for the same reason as Coinglass: a third-party API
    # hiccup must not take the trading graph down.
    if whale_task is not None:
        whale_task.add_done_callback(_make_supervisor("whale-alert", halt_on_death=False))
    # DerivativesOracle (PR-A: Hyperliquid OI). Observational alongside
    # the Coinglass poller until parity is observed in soak — no trap
    # gates on it yet, so a dropped WS must not halt the trading graph.
    if derivatives_oracle_task is not None:
        derivatives_oracle_task.add_done_callback(
            _make_supervisor("derivatives-oracle", halt_on_death=False)
        )

    log.info("boot.serving", mode=app.settings.mode, host=host, port=port)
    try:
        await server.serve()
    finally:
        for name, task in (
            ("feed-loop", feed_task),
            ("clock-drift", drift_task),
            ("spot-oracle", spot_task),
            ("calendar-guard", calendar_task),
            ("forex-factory-refresh", ff_refresh_task),
            ("coinglass-oi", coinglass_task),
            ("coinglass-heatmap", heatmap_task),
            ("whale-alert", whale_task),
            ("derivatives-oracle", derivatives_oracle_task),
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
        if ff_client is not None:
            await ff_client.aclose()
        if coinglass_client is not None:
            await coinglass_client.aclose()
        if heatmap_client is not None:
            await heatmap_client.aclose()
        if whale_client is not None:
            await whale_client.aclose()
        if rest_client is not None:
            await rest_client.aclose()
        if app.kalshi_rest_client is not None:
            await app.kalshi_rest_client.aclose()
        if app.archive_writer is not None:
            app.archive_writer.close()
        if app.portfolio.open_positions:
            log.error(
                "shutdown.open_positions_remain",
                count=len(app.portfolio.open_positions),
                note="hard rule #10: container should not exit with open positions; run scripts/flatten.sh first",
            )


def _start_calendar_if_enabled(
    app: App,
    log: Any,
) -> tuple[
    asyncio.Task[None] | None,
    asyncio.Task[None] | None,
    httpx.AsyncClient | None,
]:
    """Spin up the tier-1 guard loop + optional Forex Factory refresher.

    Returns `(guard_tick_task, ff_refresh_task, ff_http_client)`. All are
    None when calendar is effectively disabled (no YAML path AND no FF
    fetch). The caller cancels tasks and closes the client on shutdown.

    Design:
    * Static YAML entries (operator-curated) have precedence on name
      collisions with fetched FF entries — operators can override a
      mis-scheduled FF print without editing upstream data.
    * The guard's `_fired` ledger survives refreshes via
      `replace_events`, so a re-fetched FOMC row does not re-fire.
    """
    settings = app.settings.calendar
    # Static YAML path (optional — operator-curated entries).
    static_events: tuple[ScheduledEvent, ...] = ()
    if settings.path:
        cfg_dir = Path.cwd() / "config"
        cal_path = cfg_dir / settings.path
        try:
            static_events = load_calendar(cal_path)
            log.info(
                "boot.calendar_static_loaded",
                path=str(cal_path),
                count=len(static_events),
            )
        except (FileNotFoundError, ValueError) as exc:
            log.error("boot.calendar_static_failed", path=str(cal_path), error=str(exc))
            return None, None, None

    fetch_enabled = settings.fetch_enabled and app.settings.mode in MODES_WITH_FEED_LOOP
    if not static_events and not fetch_enabled:
        log.info("boot.calendar_disabled", reason="no static path and FF fetch disabled")
        return None, None, None

    # Build the guard. `app.tier1_override()` is the single approved
    # automatic trigger (hard rule #8: flatten winners AND losers).
    async def _trigger() -> None:
        outcomes = await app.tier1_override()
        log.warning(
            "calendar.tier1_flatten_executed",
            outcome_count=len(outcomes),
        )

    guard = CalendarGuard(
        clock=app.clock,
        events=static_events,
        trigger=_trigger,
        lead_seconds=settings.lead_seconds,
        cooldown_seconds=settings.cooldown_seconds,
    )
    # Wire the "no new entries inside the blackout window" half of the tier-1
    # policy (docs/RISK.md §Macro-blockers). The guard's own `tick()` loop
    # handles the pre-event flatten; this callback gates fresh entries from
    # `risk.check()` during `[ev.ts_ns - lead, ev.ts_ns + cooldown]`.
    app.oms.attach_calendar_guard(guard.is_blocked)
    static_names = frozenset(e.name for e in static_events)

    async def _guard_loop() -> None:
        while True:
            await guard.tick()
            await asyncio.sleep(settings.tick_interval_sec)

    calendar_task = asyncio.create_task(_guard_loop(), name="calendar-guard")

    ff_refresh_task: asyncio.Task[None] | None = None
    ff_client: httpx.AsyncClient | None = None
    if fetch_enabled:
        ff_client = httpx.AsyncClient(timeout=10.0)

        async def _on_refresh(events: tuple[ScheduledEvent, ...]) -> None:
            # Static YAML entries override FF rows on name collision —
            # operators may have corrected a stale FF schedule.
            merged = list(static_events) + [e for e in events if e.name not in static_names]
            guard.replace_events(merged)
            log.info(
                "calendar.refreshed",
                static=len(static_events),
                fetched=len(events),
                merged=len(merged),
            )

        refresher = ForexFactoryRefresher(
            client=ff_client,
            on_refresh=_on_refresh,
            url=settings.fetch_url,
            interval_sec=settings.fetch_interval_sec,
            tier_1_countries=settings.tier_1_countries,
            tier_1_impacts=settings.tier_1_impacts,
        )
        ff_refresh_task = asyncio.create_task(
            refresher.run(),
            name="forex-factory-refresh",
        )
        log.info(
            "boot.forex_factory_enabled",
            url=settings.fetch_url,
            interval_sec=settings.fetch_interval_sec,
            countries=list(settings.tier_1_countries),
            impacts=list(settings.tier_1_impacts),
        )

    return calendar_task, ff_refresh_task, ff_client


def _start_coinglass_if_enabled(
    app: App,
    log: Any,
) -> tuple[asyncio.Task[None] | None, httpx.AsyncClient | None]:
    """Spin up the Coinglass OI poller (Slice 11 P2 — shadow).

    Observational only: each sample is logged and stashed on
    `App.latest_open_interest`, but traps do not gate on it yet. Gated
    on the same `MODES_WITH_FEED_LOOP` set as the other feeds — dev
    mode stays offline by default. The task supervisor treats this
    poller the same as any other background task: an unexpected death
    halts the app and lets Cloud Run restart it.
    """
    settings = app.settings.feeds.coinglass
    if not settings.enabled:
        log.info("boot.coinglass_disabled", reason="feeds.coinglass.enabled=false")
        return None, None
    if app.settings.mode not in MODES_WITH_FEED_LOOP:
        log.info("boot.coinglass_skipped", mode=app.settings.mode)
        return None, None

    api_key = os.getenv(settings.api_key_env) or None
    if not api_key:
        log.warning(
            "boot.coinglass_unkeyed",
            note=(
                f"{settings.api_key_env} not set; using free-tier unkeyed path "
                "(stricter rate limits, may fail on shared IP)"
            ),
        )

    client = httpx.AsyncClient(timeout=10.0)

    async def _on_sample(sample: OpenInterestSample) -> None:
        app.latest_open_interest = sample

    poller = CoinglassPoller(
        client=client,
        clock=app.clock,
        on_sample=_on_sample,
        base_url=settings.base_url,
        oi_path=settings.oi_path,
        symbol=settings.symbol,
        interval=settings.interval,
        api_key=api_key,
        poll_interval_sec=settings.poll_interval_sec,
    )
    task = asyncio.create_task(poller.run(), name="coinglass-oi")
    log.info(
        "boot.coinglass_enabled",
        base_url=settings.base_url,
        path=settings.oi_path,
        symbol=settings.symbol,
        interval=settings.interval,
        poll_interval_sec=settings.poll_interval_sec,
        keyed=bool(api_key),
    )
    return task, client


def _start_coinglass_heatmap_if_enabled(
    app: App,
    log: Any,
) -> tuple[asyncio.Task[None] | None, httpx.AsyncClient | None]:
    """Spin up the Coinglass liquidation-heatmap poller (Slice 11 P3 —
    shadow). Same contract as `_start_coinglass_if_enabled`: observational
    only, non-critical background task (death logged, not halting). Lives
    on a separate httpx client so a stuck heatmap endpoint cannot share
    connection-pool pressure with the OI poller."""
    settings = app.settings.feeds.coinglass_heatmap
    if not settings.enabled:
        log.info(
            "boot.coinglass_heatmap_disabled",
            reason="feeds.coinglass_heatmap.enabled=false",
        )
        return None, None
    if app.settings.mode not in MODES_WITH_FEED_LOOP:
        log.info("boot.coinglass_heatmap_skipped", mode=app.settings.mode)
        return None, None

    api_key = os.getenv(settings.api_key_env) or None
    if not api_key:
        log.warning(
            "boot.coinglass_heatmap_unkeyed",
            note=(
                f"{settings.api_key_env} not set; heatmap endpoint commonly "
                "requires a paid-tier key and may 401 unkeyed"
            ),
        )

    client = httpx.AsyncClient(timeout=10.0)

    async def _on_sample(sample: LiquidationHeatmapSample) -> None:
        app.latest_liquidation_heatmap = sample

    poller = CoinglassHeatmapPoller(
        client=client,
        clock=app.clock,
        on_sample=_on_sample,
        base_url=settings.base_url,
        heatmap_path=settings.heatmap_path,
        symbol=settings.symbol,
        interval=settings.interval,
        api_key=api_key,
        poll_interval_sec=settings.poll_interval_sec,
    )
    task = asyncio.create_task(poller.run(), name="coinglass-heatmap")
    log.info(
        "boot.coinglass_heatmap_enabled",
        base_url=settings.base_url,
        path=settings.heatmap_path,
        symbol=settings.symbol,
        interval=settings.interval,
        poll_interval_sec=settings.poll_interval_sec,
        keyed=bool(api_key),
    )
    return task, client


def _start_whale_alert_if_enabled(
    app: App,
    log: Any,
) -> tuple[asyncio.Task[None] | None, httpx.AsyncClient | None]:
    """Spin up the Whale Alert poller (Slice 11 P4 — shadow).

    Same contract as the Coinglass pollers: observational only, non-
    critical. Unlike Coinglass, Whale Alert has no unauthenticated tier
    — if the API-key env var is unset we log a warning and skip the
    task entirely rather than fire 401s on every poll.
    """
    settings = app.settings.feeds.whale_alert
    if not settings.enabled:
        log.info("boot.whale_alert_disabled", reason="feeds.whale_alert.enabled=false")
        return None, None
    if app.settings.mode not in MODES_WITH_FEED_LOOP:
        log.info("boot.whale_alert_skipped", mode=app.settings.mode)
        return None, None

    api_key = os.getenv(settings.api_key_env) or None
    if not api_key:
        log.warning(
            "boot.whale_alert_missing_key",
            note=(
                f"{settings.api_key_env} not set; whale-alert poller will not start "
                "(endpoint requires a paid subscription key)"
            ),
        )
        return None, None

    client = httpx.AsyncClient(timeout=10.0)

    async def _on_sample(sample: WhaleAlertSample) -> None:
        app.latest_whale_alert = sample

    poller = WhaleAlertPoller(
        client=client,
        clock=app.clock,
        on_sample=_on_sample,
        api_key=api_key,
        base_url=settings.base_url,
        path=settings.path,
        symbol=settings.symbol,
        min_value_usd=settings.min_value_usd,
        poll_interval_sec=settings.poll_interval_sec,
    )
    task = asyncio.create_task(poller.run(), name="whale-alert")
    log.info(
        "boot.whale_alert_enabled",
        base_url=settings.base_url,
        path=settings.path,
        symbol=settings.symbol,
        min_value_usd=settings.min_value_usd,
        poll_interval_sec=settings.poll_interval_sec,
    )
    return task, client


def _start_derivatives_oracle_if_enabled(
    app: App,
    log: Any,
) -> asyncio.Task[None] | None:
    """Spin up the persistent DerivativesOracle (PR-A Hyperliquid + PR-B Bybit).

    Wires all enabled public-WS derivatives feeds into a single
    `DerivativesOracle` that exposes fail-closed accessors and
    dispatches subscriber callbacks. OI updates still run alongside
    the existing Coinglass HTTP poller — both push into
    `App.latest_open_interest`, most-recent wins — until PR-C retires
    Coinglass on paper-soak parity. Liquidation events are net-new
    (no prior source); the FeatureEngine rolling deque consumes them
    starting in PR-C.

    Disabled when neither `feeds.hyperliquid` nor `feeds.bybit` is
    enabled, or when the mode is not in `MODES_WITH_FEED_LOOP`. In
    either case we still install an empty oracle so consumers can
    always dereference `app.derivatives_oracle` without None-guards.
    """
    hl_settings = app.settings.feeds.hyperliquid
    by_settings = app.settings.feeds.bybit

    if not hl_settings.enabled and not by_settings.enabled:
        log.info(
            "boot.derivatives_oracle_disabled",
            reason="no derivatives feeds enabled",
        )
        app.derivatives_oracle = DerivativesOracle(clock=app.clock)
        return None
    if app.settings.mode not in MODES_WITH_FEED_LOOP:
        log.info("boot.derivatives_oracle_skipped", mode=app.settings.mode)
        app.derivatives_oracle = DerivativesOracle(clock=app.clock)
        return None

    oi_feeds: list[DerivativesFeed[OpenInterestSample]] = []
    liq_feeds: list[DerivativesFeed[LiquidationEvent]] = []
    sources: list[str] = []

    if hl_settings.enabled:
        oi_feeds.append(
            DerivativesFeed(
                name="hyperliquid",
                ws_url=hl_settings.ws_url,
                ws_connect=ws_connect_websockets,
                staleness=StalenessTracker(
                    name="hyperliquid",
                    clock=app.clock,
                    threshold_ms=hl_settings.staleness_halt_ms,
                ),
                parse=hyperliquid_parser(asset=hl_settings.asset, clock=app.clock),
                subscribe=build_hyperliquid_subscribe(),
            )
        )
        sources.append(HYPERLIQUID_SOURCE)

    if by_settings.enabled:
        if by_settings.subscribe_oi:
            oi_feeds.append(
                DerivativesFeed(
                    name="bybit",
                    ws_url=by_settings.ws_url,
                    ws_connect=ws_connect_websockets,
                    staleness=StalenessTracker(
                        name="bybit_oi",
                        clock=app.clock,
                        threshold_ms=by_settings.staleness_halt_ms,
                    ),
                    parse=bybit_tickers_parser(
                        symbol=by_settings.symbol, clock=app.clock
                    ),
                    subscribe=build_bybit_subscribe(
                        bybit_tickers_topic(by_settings.symbol)
                    ),
                )
            )
        if by_settings.subscribe_liquidations:
            liq_feeds.append(
                DerivativesFeed(
                    name="bybit",
                    ws_url=by_settings.ws_url,
                    ws_connect=ws_connect_websockets,
                    staleness=StalenessTracker(
                        name="bybit_liq",
                        clock=app.clock,
                        threshold_ms=by_settings.staleness_halt_ms,
                    ),
                    parse=bybit_liquidation_parser(
                        symbol=by_settings.symbol, clock=app.clock
                    ),
                    subscribe=build_bybit_subscribe(
                        bybit_liquidation_topic(by_settings.symbol)
                    ),
                )
            )
        sources.append(BYBIT_SOURCE)

    oracle = DerivativesOracle(
        oi_feeds=tuple(oi_feeds),
        liq_feeds=tuple(liq_feeds),
        clock=app.clock,
    )
    app.derivatives_oracle = oracle

    def _on_oi(sample: OpenInterestSample) -> None:
        # Both DerivativesOracle (push-based) and CoinglassPoller (HTTP poll)
        # write here during the parity period. Most-recent wins by virtue
        # of who set the slot last; a future "tagged source" model can land
        # in PR-C once Coinglass is retired.
        app.latest_open_interest = sample
        log.info(
            "derivatives_oracle.oi_sample",
            source=sample.source,
            symbol=sample.symbol,
            total_oi_usd=sample.total_oi_usd,
            ts_ns=sample.ts_ns,
        )

    def _on_liq(event: LiquidationEvent) -> None:
        # Observational-only until PR-C wires the FeatureEngine rolling
        # deque. Keeping the log emission means paper-soak has coverage
        # of the stream cadence + side distribution before any trap
        # gates on it.
        log.info(
            "derivatives_oracle.liquidation",
            source=event.source,
            symbol=event.symbol,
            side=event.side,
            price_usd=event.price_usd,
            size_usd=event.size_usd,
            ts_ns=event.ts_ns,
        )

    oracle.subscribe_open_interest(_on_oi)
    oracle.subscribe_liquidations(_on_liq)

    task = asyncio.create_task(oracle.run(), name="derivatives-oracle")
    log.info(
        "boot.derivatives_oracle_enabled",
        sources=sources,
        oi_feed_count=len(oi_feeds),
        liq_feed_count=len(liq_feeds),
    )
    return task


def _start_feed_loop_if_enabled(
    app: App,
    log: Any,
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
        name
        for name, val in (
            ("kalshi.ws_url", kalshi_ws),
            ("kalshi.rest_url", rest_url),
            ("coinbase.ws_url", coinbase_ws),
            ("kraken.ws_url", kraken_ws),
        )
        if not val
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

    # App-scope FeatureEngine (Slice 8, Phase 2). TF labels must match the
    # bar_bus seconds table above. Kept at App scope so 1H/1d accumulator
    # state persists across hourly market rolls (§DESIGN.md #6.3 HTF veto
    # reads `rsi("1h")` which needs ~14h of 1h closes to warm up).
    feature_engine = FeatureEngine(
        timeframes=["1m", "5m", "15m", "1h", "1d"],
        bollinger_period=app.settings.signal.bollinger_period_bars,
        bollinger_std_mult=app.settings.signal.bollinger_std_mult,
    )
    feature_engine.attach(bar_bus)
    app.feature_engine = feature_engine

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
        sys.stderr.write("live mode requires RISK_COMMITTEE_SIGNED=yes (hard rule #2).\n")
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
