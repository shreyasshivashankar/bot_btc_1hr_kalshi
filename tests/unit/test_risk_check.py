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
    max_correlated_positions: int = 1,
    max_entry_price_cents: int = 75,
) -> RiskSettings:
    return RiskSettings(
        kelly_fraction=kelly_fraction,
        max_position_notional_usd=max_notional,
        max_daily_loss_pct=max_daily_loss_pct,
        max_correlated_positions=max_correlated_positions,
        max_entry_price_cents=max_entry_price_cents,
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
    correlated_count: int = 0,
    entry_price_cents: int = 30,
) -> RiskInput:
    return RiskInput(
        signal=_signal(confidence=confidence, entry_price_cents=entry_price_cents),
        contracts=contracts,
        bankroll_usd=bankroll,
        open_positions_notional_usd=open_notional,
        daily_realized_pnl_usd=daily_pnl,
        breakers=breakers or BreakerState(),
        now_ns=1_000_000_000,
        min_signal_confidence=min_conf,
        correlated_open_positions_count=correlated_count,
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


def test_reject_correlation_cap_at_default_one() -> None:
    """One YES bet already open on this hour → second YES bet (same hour,
    different strike) must be rejected. This is the whole point of the cap:
    three YES bets on adjacent strikes of the same session settle as one
    correlated directional bet on BTC, and aggregate_notional alone lets
    them stack as long as the cash ceiling isn't hit."""
    d = check(_req(correlated_count=1), _settings(max_correlated_positions=1))
    assert isinstance(d, Reject)
    assert d.reason == "correlation_cap"


def test_correlation_cap_allows_when_count_below_cap() -> None:
    """A risk committee that wants to tolerate two concurrent strikes can
    raise the cap. The check is exclusive (count < cap approves)."""
    d = check(_req(correlated_count=1), _settings(max_correlated_positions=2))
    assert isinstance(d, Approve)


def test_correlation_cap_does_not_fire_when_count_is_zero() -> None:
    d = check(_req(correlated_count=0), _settings(max_correlated_positions=1))
    assert isinstance(d, Approve)


def test_correlation_cap_ordered_after_confidence_floor() -> None:
    """Rule-ordering canary: confidence floor must reject BEFORE correlation.
    A low-confidence signal should not surface a correlation reject — the
    decision journal becomes noisy if every below-confidence tick that
    happens to have an open position logs `correlation_cap`."""
    d = check(
        _req(confidence=0.3, min_conf=0.5, correlated_count=5),
        _settings(max_correlated_positions=1),
    )
    assert isinstance(d, Reject)
    assert d.reason == "below_confidence_floor"


def test_reject_premium_cap_above_default() -> None:
    """Slice 11 Phase 3.1: 76¢ entry with default 75¢ cap → reject. The
    inverted-risk trade (pay 76 to make 24) is structurally off-edge even
    when Kelly's (1-p) term makes the math look fine."""
    d = check(_req(entry_price_cents=76), _settings())
    assert isinstance(d, Reject)
    assert d.reason == "premium_cap"


def test_premium_cap_boundary_approves_at_exactly_75() -> None:
    """The cap is exclusive upper bound — 75¢ exactly is still acceptable."""
    d = check(
        _req(contracts=1, entry_price_cents=75),
        _settings(max_notional=100.0),
    )
    assert isinstance(d, Approve)


def test_premium_cap_respects_configured_override() -> None:
    """Risk committee can loosen the cap (e.g. for a higher-price strategy
    slice). 80¢ entry with cap raised to 85 → approve; same entry at
    default 75 would reject."""
    d = check(
        _req(contracts=1, entry_price_cents=80),
        _settings(max_entry_price_cents=85),
    )
    assert isinstance(d, Approve)


def test_premium_cap_ordered_after_confidence_floor() -> None:
    """Journal-noise canary (mirrors correlation-cap ordering test). A
    low-confidence 85¢ signal must reject as below_confidence_floor, not
    premium_cap — else every too-weak tick on an expensive strike floods
    the decision stream with premium_cap rejects."""
    d = check(
        _req(confidence=0.3, min_conf=0.5, entry_price_cents=85),
        _settings(),
    )
    assert isinstance(d, Reject)
    assert d.reason == "below_confidence_floor"


def test_premium_cap_ordered_before_correlation_cap() -> None:
    """Correlation cap is a journaling-sensitive reject too, but premium
    cap fires first: an inverted-risk entry never qualifies regardless of
    how many correlated positions are open. This keeps premium_cap the
    clearer diagnostic when both conditions are true."""
    d = check(
        _req(entry_price_cents=85, correlated_count=5),
        _settings(max_correlated_positions=1),
    )
    assert isinstance(d, Reject)
    assert d.reason == "premium_cap"
