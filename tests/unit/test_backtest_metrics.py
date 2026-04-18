"""Tests for research.backtest metric computations.

Real data is not required — we build synthetic BetOutcomes and verify the
math. The Slice 5 TODO in backtest.py covers the missing CLI driver that
would consume a captured tick archive.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.schemas import BetOutcome, Features
from bot_btc_1hr_kalshi.research.backtest import compute_metrics


def _features() -> Features:
    return Features(
        regime_trend="flat", regime_vol="normal",
        signal_confidence=0.7, bollinger_pct_b=-0.8,
        atr_cents=10.0, book_depth_at_entry=600.0,
        spread_cents=4, spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def _outcome(
    bet_id: str,
    trap: str = "floor_reversion",
    net_pnl: float = 1.0,
    exit_reason: str = "settled",
) -> BetOutcome:
    return BetOutcome(
        bet_id=bet_id, decision_id=bet_id, market_id="M",
        trap=trap, side="YES",  # type: ignore[arg-type]
        opened_at_ns=1, closed_at_ns=2, hold_duration_sec=1.0,
        entry_price_cents=40, exit_price_cents=45, contracts=10,
        gross_pnl_usd=net_pnl, fees_usd=0.0, net_pnl_usd=net_pnl,
        counterfactual_held_pnl_usd=None,
        exit_reason=exit_reason,  # type: ignore[arg-type]
        features_at_entry=_features(),
    )


def test_empty_returns_zeros() -> None:
    m = compute_metrics([])
    assert m.n_bets == 0
    assert m.hit_rate == 0.0
    assert m.sharpe_per_bet == 0.0
    assert m.max_drawdown_usd == 0.0


def test_hit_rate_excludes_zero_pnl() -> None:
    m = compute_metrics([
        _outcome("a", net_pnl=1.0),
        _outcome("b", net_pnl=0.0),   # breakeven — NOT a hit
        _outcome("c", net_pnl=-1.0),
    ])
    assert m.n_bets == 3
    assert m.n_winners == 1
    assert m.hit_rate == 1 / 3


def test_max_drawdown_measured_peak_to_trough() -> None:
    # Cumulative: +10, +15, +5, +12, +7, +2 -> peak 15, trough 2, dd 13
    pnls = [10.0, 5.0, -10.0, 7.0, -5.0, -5.0]
    m = compute_metrics([_outcome(str(i), net_pnl=p) for i, p in enumerate(pnls)])
    assert m.max_drawdown_usd == 13.0
    assert m.total_net_pnl_usd == 2.0


def test_sharpe_zero_when_all_equal_and_zero_for_single_bet() -> None:
    m_single = compute_metrics([_outcome("a", net_pnl=2.0)])
    assert m_single.sharpe_per_bet == 0.0
    m_flat = compute_metrics([
        _outcome("a", net_pnl=1.0), _outcome("b", net_pnl=1.0),
    ])
    assert m_flat.sharpe_per_bet == 0.0


def test_per_trap_attribution() -> None:
    m = compute_metrics([
        _outcome("a", trap="floor_reversion", net_pnl=5.0),
        _outcome("b", trap="floor_reversion", net_pnl=-2.0),
        _outcome("c", trap="ceiling_reversion", net_pnl=1.0),
    ])
    assert m.per_trap_pnl_usd == {"floor_reversion": 3.0, "ceiling_reversion": 1.0}
    assert m.per_trap_count == {"floor_reversion": 2, "ceiling_reversion": 1}


def test_per_exit_reason_count() -> None:
    m = compute_metrics([
        _outcome("a", exit_reason="early_cashout_99"),
        _outcome("b", exit_reason="early_cashout_99"),
        _outcome("c", exit_reason="settled"),
    ])
    assert m.per_exit_reason_count == {"early_cashout_99": 2, "settled": 1}
