from __future__ import annotations

from collections.abc import Sequence

import pytest

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.execution.broker.base import BrokerPosition, Fill, OrderAck, OrderRequest
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.execution.reconciler import Reconciler
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.risk.breakers import BreakerState


class FakeBroker:
    def __init__(self, positions: Sequence[BrokerPosition] = ()) -> None:
        self.positions = tuple(positions)
        self.fail = False

    async def submit(self, req: OrderRequest) -> OrderAck:  # not used
        raise NotImplementedError

    async def cancel(self, order_id: str) -> bool:
        return False

    async def list_open_orders(self) -> tuple[OrderAck, ...]:
        return ()

    async def list_positions(self) -> tuple[BrokerPosition, ...]:
        if self.fail:
            raise RuntimeError("broker down")
        return self.positions


def _features() -> Features:
    return Features(
        regime_trend="flat", regime_vol="normal", signal_confidence=0.5,
        bollinger_pct_b=-0.5, atr_cents=1.0, book_depth_at_entry=100.0,
        spread_cents=1, spot_btc_usd=60_000.0, minutes_to_settlement=30.0,
    )


def _open_position(portfolio: Portfolio, *, position_id: str, market: str, contracts: int) -> None:
    fill = Fill(
        order_id="o", client_order_id="c", market_id=market,
        side="YES", action="BUY", price_cents=40, contracts=contracts,
        ts_ns=1, fees_usd=0.0,
    )
    portfolio.open_from_fill(
        position_id=position_id, decision_id="d",
        fill=fill, trap="floor_reversion", features_at_entry=_features(),
    )


def _build_app(broker: FakeBroker, settings_patch: dict | None = None) -> App:
    from pathlib import Path

    from bot_btc_1hr_kalshi.config.loader import load_settings

    settings = load_settings("paper", config_dir=Path(__file__).resolve().parents[2] / "config")
    clock = ManualClock(0)
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=1000.0)
    oms = OMS(
        broker=broker,  # type: ignore[arg-type]
        portfolio=portfolio, breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)
    return App(settings=settings, clock=clock, breakers=breakers,
               portfolio=portfolio, oms=oms, monitor=monitor)


async def test_check_once_no_mismatch_no_halt() -> None:
    broker = FakeBroker((BrokerPosition(market_id="M1", side="YES", contracts=5, avg_entry_price_cents=40),))
    app = _build_app(broker)
    _open_position(app.portfolio, position_id="p1", market="M1", contracts=5)
    rec = Reconciler(app=app, broker=broker, interval_sec=60)
    result = await rec.check_once()
    assert not result.halted
    assert result.mismatches == ()
    assert not app.trading_halted


async def test_persistent_mismatch_halts_on_second_tick() -> None:
    broker = FakeBroker((BrokerPosition(market_id="M1", side="YES", contracts=8, avg_entry_price_cents=40),))
    app = _build_app(broker)
    _open_position(app.portfolio, position_id="p1", market="M1", contracts=5)  # delta=3 > 1
    rec = Reconciler(app=app, broker=broker, interval_sec=60)

    # First tick: mismatch observed but NOT yet halted (could be mid-fill race).
    result1 = await rec.check_once()
    assert not result1.halted
    assert len(result1.mismatches) == 1
    assert not app.trading_halted

    # Second tick: same mismatch persists → real desync → halt.
    result2 = await rec.check_once()
    assert result2.halted
    m = result2.mismatches[0]
    assert m.market_id == "M1" and m.local_contracts == 5 and m.broker_contracts == 8
    assert app.trading_halted


async def test_transient_mismatch_resolves_before_halt() -> None:
    """Regression: a mid-flight fill makes broker/local disagree for one tick
    only. The persistence gate must absorb this without halting."""
    # Tick 1: broker still shows 0 (fill processed but not yet in list_positions).
    broker = FakeBroker(())
    app = _build_app(broker)
    _open_position(app.portfolio, position_id="p1", market="M1", contracts=5)
    rec = Reconciler(app=app, broker=broker, interval_sec=60)

    result1 = await rec.check_once()
    assert not result1.halted
    assert len(result1.mismatches) == 1
    assert not app.trading_halted

    # Tick 2: broker has caught up. Mismatch disappears.
    broker.positions = (BrokerPosition(market_id="M1", side="YES", contracts=5, avg_entry_price_cents=40),)
    result2 = await rec.check_once()
    assert not result2.halted
    assert result2.mismatches == ()
    assert not app.trading_halted


async def test_different_mismatch_each_tick_does_not_halt() -> None:
    """Two separate transient mismatches on different markets — neither persists."""
    broker = FakeBroker((BrokerPosition(market_id="M1", side="YES", contracts=3, avg_entry_price_cents=40),))
    app = _build_app(broker)
    # Local has nothing on M1 or M2; broker flips which market it reports.
    rec = Reconciler(app=app, broker=broker, interval_sec=60)
    result1 = await rec.check_once()
    assert not result1.halted

    broker.positions = (BrokerPosition(market_id="M2", side="YES", contracts=3, avg_entry_price_cents=40),)
    result2 = await rec.check_once()
    assert not result2.halted
    assert not app.trading_halted


async def test_tolerance_absorbs_off_by_one() -> None:
    broker = FakeBroker((BrokerPosition(market_id="M1", side="YES", contracts=6, avg_entry_price_cents=40),))
    app = _build_app(broker)
    _open_position(app.portfolio, position_id="p1", market="M1", contracts=5)  # delta=1 == tolerance
    rec = Reconciler(app=app, broker=broker, interval_sec=60, tolerance_contracts=1)
    result = await rec.check_once()
    assert not result.halted


async def test_broker_only_position_detected() -> None:
    """Broker shows a position we don't know about — clear divergence."""
    broker = FakeBroker((BrokerPosition(market_id="Mystery", side="YES", contracts=10, avg_entry_price_cents=50),))
    app = _build_app(broker)
    rec = Reconciler(app=app, broker=broker, interval_sec=60)
    assert not (await rec.check_once()).halted  # first tick: pending
    result = await rec.check_once()
    assert result.halted
    assert result.mismatches[0].market_id == "Mystery"


async def test_local_only_position_detected() -> None:
    broker = FakeBroker(())  # broker reports flat
    app = _build_app(broker)
    _open_position(app.portfolio, position_id="p1", market="M1", contracts=5)
    rec = Reconciler(app=app, broker=broker, interval_sec=60)
    assert not (await rec.check_once()).halted  # first tick: pending
    result = await rec.check_once()
    assert result.halted
    assert result.mismatches[0].local_contracts == 5
    assert result.mismatches[0].broker_contracts == 0


async def test_broker_call_failure_does_not_halt() -> None:
    """We don't trust a flaky broker to have last word — retry next tick."""
    broker = FakeBroker(())
    broker.fail = True
    app = _build_app(broker)
    rec = Reconciler(app=app, broker=broker, interval_sec=60)
    result = await rec.check_once()
    assert not result.halted
    assert not app.trading_halted


def test_invalid_interval_rejected() -> None:
    broker = FakeBroker(())
    app = _build_app(broker)
    with pytest.raises(ValueError, match="interval_sec"):
        Reconciler(app=app, broker=broker, interval_sec=0)


def test_negative_tolerance_rejected() -> None:
    broker = FakeBroker(())
    app = _build_app(broker)
    with pytest.raises(ValueError, match="tolerance"):
        Reconciler(app=app, broker=broker, interval_sec=1, tolerance_contracts=-1)
