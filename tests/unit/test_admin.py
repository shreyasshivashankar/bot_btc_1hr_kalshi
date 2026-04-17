from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from bot_btc_1hr_kalshi.admin.auth import AdminAuth
from bot_btc_1hr_kalshi.admin.server import create_app
from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.execution.broker.base import Fill
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState

TOKEN = "test-admin-token-123"
MARKET = "KBTC-26APR1600-B60000"
REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"


def _auth() -> dict[str, str]:
    return {"X-Admin-Token": TOKEN}


def _features() -> Features:
    return Features(
        regime_trend="flat",
        regime_vol="normal",
        signal_confidence=0.7,
        bollinger_pct_b=-0.8,
        atr_cents=10.0,
        book_depth_at_entry=600.0,
        spread_cents=4,
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def _seed_book() -> L2Book:
    b = L2Book(MARKET)
    b.apply(
        BookUpdate(
            seq=1,
            ts_ns=1_000,
            market_id=MARKET,
            bids=(BookLevel(40, 200),),
            asks=(BookLevel(45, 200),),
            is_snapshot=True,
        )
    )
    return b


def _build_app() -> tuple[App, PaperBroker]:
    settings = load_settings("paper", config_dir=CONFIG_DIR)
    clock = ManualClock(1_000)
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=1_000.0)
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
    app = App(
        settings=settings,
        clock=clock,
        breakers=breakers,
        portfolio=portfolio,
        oms=oms,
        monitor=monitor,
    )
    return app, broker


def _client(app: App) -> TestClient:
    fastapi_app = create_app(app, admin_token=TOKEN)
    return TestClient(fastapi_app)


def test_healthz_no_auth_required() -> None:
    app, _ = _build_app()
    with _client(app) as c:
        r = c.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}


def test_readyz_503_when_no_books() -> None:
    app, _ = _build_app()
    with _client(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert r.json()["detail"]["reason"] == "no_markets_registered"


def test_readyz_200_when_book_valid() -> None:
    app, broker = _build_app()
    book = _seed_book()
    app.register_book(book)
    broker.register_book(book)
    with _client(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 200
        assert r.json() == {"ready": True, "reason": "ok"}


def test_readyz_503_when_breaker_tripped() -> None:
    app, broker = _build_app()
    book = _seed_book()
    app.register_book(book)
    broker.register_book(book)
    app.breakers.set_feed_halt(halted=True)
    with _client(app) as c:
        r = c.get("/readyz")
        assert r.status_code == 503
        assert "feed_staleness" in r.json()["detail"]["reason"]


def test_admin_status_requires_token() -> None:
    app, _ = _build_app()
    with _client(app) as c:
        assert c.get("/admin/status").status_code == 401
        assert c.get("/admin/status", headers={"X-Admin-Token": "wrong"}).status_code == 401


def test_admin_status_returns_operator_view() -> None:
    app, _ = _build_app()
    with _client(app) as c:
        r = c.get("/admin/status", headers=_auth())
        assert r.status_code == 200
        body = r.json()
        assert body["mode"] == "paper"
        assert body["trading_halted"] is False
        assert body["bankroll_usd"] == 1000.0
        assert body["open_positions_count"] == 0


def test_halt_and_resume_round_trip() -> None:
    app, _ = _build_app()
    with _client(app) as c:
        r = c.post("/admin/halt", headers=_auth())
        assert r.status_code == 200
        assert r.json() == {"trading_halted": True}
        assert app.trading_halted is True

        r = c.post("/admin/resume", headers=_auth())
        assert r.status_code == 200
        assert r.json() == {"trading_halted": False}


def test_resume_blocked_when_tier1_active() -> None:
    app, _ = _build_app()
    app.tier1_override_active = True
    app.trading_halted = True
    with _client(app) as c:
        r = c.post("/admin/resume", headers=_auth())
        assert r.status_code == 409


def test_flatten_closes_open_position_via_ioc() -> None:
    app, broker = _build_app()
    book = _seed_book()
    app.register_book(book)
    broker.register_book(book)
    app.portfolio.open_from_fill(
        position_id="p1",
        decision_id="p1",
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=30,
            contracts=50,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with _client(app) as c:
        r = c.post("/admin/flatten", headers=_auth())
        assert r.status_code == 200
        body = r.json()
        assert body["flattened_count"] == 1
        assert body["outcomes"][0]["exit_reason"] == "tier1_flatten"
    assert app.portfolio.open_positions == ()


def test_tier1_override_halts_and_flattens() -> None:
    app, broker = _build_app()
    book = _seed_book()
    app.register_book(book)
    broker.register_book(book)
    app.portfolio.open_from_fill(
        position_id="p1",
        decision_id="p1",
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=30,
            contracts=50,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with _client(app) as c:
        r = c.post("/admin/tier1_override", headers=_auth())
        assert r.status_code == 200
        assert app.tier1_override_active is True
        assert app.trading_halted is True
        assert app.portfolio.open_positions == ()


def test_adminauth_rejects_empty_token() -> None:
    with pytest.raises(ValueError):
        AdminAuth(None)
    with pytest.raises(ValueError):
        AdminAuth("")
