from __future__ import annotations

from bot_btc_1hr_kalshi.config.settings import RiskSettings
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.risk import Approve, BreakerState, Reject, RiskInput, check
from bot_btc_1hr_kalshi.signal.types import TrapSignal


def _settings(
    *,
    max_notional: float = 100.0,
    max_daily_loss_pct: float = 0.05,
    kelly_fraction: float = 0.25,
) -> RiskSettings:
    return RiskSettings(
        kelly_fraction=kelly_fraction,
        max_position_notional_usd=max_notional,
        max_daily_loss_pct=max_daily_loss_pct,
    )


def _signal(*, confidence: float = 0.7, entry_price_cents: int = 30) -> TrapSignal:
    return TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=8.0,
        features=Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=confidence,
            bollinger_pct_b=-0.7,
            atr_cents=10.0,
            book_depth_at_entry=200.0,
            spread_cents=2,
            spot_btc_usd=60_000.0,
            minutes_to_settlement=30.0,
        ),
    )


def _req(
    *,
    contracts: int = 10,
    bankroll: float = 1000.0,
    open_notional: float = 0.0,
    daily_pnl: float = 0.0,
    confidence: float = 0.7,
    min_conf: float = 0.5,
    breakers: BreakerState | None = None,
) -> RiskInput:
    return RiskInput(
        signal=_signal(confidence=confidence),
        contracts=contracts,
        bankroll_usd=bankroll,
        open_positions_notional_usd=open_notional,
        daily_realized_pnl_usd=daily_pnl,
        breakers=breakers or BreakerState(),
        now_ns=1_000_000_000,
        min_signal_confidence=min_conf,
    )


def test_approve_happy_path() -> None:
    d = check(_req(contracts=10), _settings())
    assert isinstance(d, Approve)
    assert d.contracts == 10


def test_reject_zero_contracts() -> None:
    d = check(_req(contracts=0), _settings())
    assert isinstance(d, Reject)
    assert d.reason == "zero_contracts"


def test_reject_when_breaker_tripped() -> None:
    b = BreakerState()
    b.set_feed_halt(halted=True)
    d = check(_req(breakers=b), _settings())
    assert isinstance(d, Reject)
    assert "breaker_tripped" in d.reason
    assert "feed_staleness" in d.reason


def test_reject_below_confidence_floor() -> None:
    d = check(_req(confidence=0.3, min_conf=0.5), _settings())
    assert isinstance(d, Reject)
    assert d.reason == "below_confidence_floor"


def test_reject_daily_loss_limit() -> None:
    # daily loss == -5% of $1000 = -$50 → exactly at limit → reject
    d = check(_req(daily_pnl=-50.0, bankroll=1000.0), _settings(max_daily_loss_pct=0.05))
    assert isinstance(d, Reject)
    assert d.reason == "daily_loss_limit"


def test_reject_per_position_notional_cap() -> None:
    # 500 contracts * $0.30 = $150 > $100 cap
    d = check(_req(contracts=500), _settings(max_notional=100.0))
    assert isinstance(d, Reject)
    assert d.reason == "position_notional_cap"


def test_reject_aggregate_exposure_cap() -> None:
    # per-pos cap $100, aggregate 3x = $300. Open already $280, this bet $30 -> $310
    d = check(
        _req(contracts=100, open_notional=280.0),
        _settings(max_notional=100.0),
    )
    assert isinstance(d, Reject)
    assert d.reason == "aggregate_exposure_cap"
