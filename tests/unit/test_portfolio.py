from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.execution.broker.base import Fill
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.portfolio import Portfolio

MARKET = "KBTC-26APR1600-B60000"


def _features() -> Features:
    return Features(
        regime_trend="flat",
        regime_vol="normal",
        signal_confidence=0.7,
        bollinger_pct_b=-0.8,
        atr_cents=10.0,
        book_depth_at_entry=100.0,
        spread_cents=2,
        spot_btc_usd=60_000.0,
        minutes_to_settlement=30.0,
    )


def _buy(price_cents: int, contracts: int, ts_ns: int = 1_000, fees: float = 0.0) -> Fill:
    return Fill(
        order_id="o1",
        client_order_id="c1",
        market_id=MARKET,
        side="YES",
        action="BUY",
        price_cents=price_cents,
        contracts=contracts,
        ts_ns=ts_ns,
        fees_usd=fees,
    )


def _sell(price_cents: int, contracts: int, ts_ns: int = 2_000, fees: float = 0.0) -> Fill:
    return Fill(
        order_id="o2",
        client_order_id="c2",
        market_id=MARKET,
        side="YES",
        action="SELL",
        price_cents=price_cents,
        contracts=contracts,
        ts_ns=ts_ns,
        fees_usd=fees,
    )


def test_open_deducts_notional_and_fees() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100, fees=0.5),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    assert p.bankroll_usd == pytest.approx(1000.0 - 30.0 - 0.5)
    assert p.open_positions_notional_usd == pytest.approx(30.0)


def test_close_profit_flows_into_daily_pnl_and_bankroll() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    outcome = p.close(
        position_id="p1",
        exit_fill=_sell(60, 100),
        exit_reason="early_cashout_99",
    )
    assert outcome.gross_pnl_usd == pytest.approx(30.0)
    assert outcome.net_pnl_usd == pytest.approx(30.0)
    assert p.daily_realized_pnl_usd == pytest.approx(30.0)
    assert p.bankroll_usd == pytest.approx(1000.0 + 30.0)
    assert p.open_positions_notional_usd == 0.0


def test_close_with_fees_reduces_net() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100, fees=1.0),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    outcome = p.close(
        position_id="p1",
        exit_fill=_sell(40, 100, fees=1.0),
        exit_reason="soft_stop",
    )
    assert outcome.gross_pnl_usd == pytest.approx(10.0)
    assert outcome.fees_usd == pytest.approx(2.0)
    assert outcome.net_pnl_usd == pytest.approx(8.0)


def test_settle_at_100_pays_full() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    outcome = p.settle(position_id="p1", settlement_cents=100, settled_at_ns=9_000)
    assert outcome.exit_reason == "settled"
    assert outcome.exit_price_cents is None
    assert outcome.gross_pnl_usd == pytest.approx(70.0)
    # -$30 on open, +$100 on settle = +$70 net
    assert p.bankroll_usd == pytest.approx(1070.0)


def test_settle_at_0_loses_entry() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    outcome = p.settle(position_id="p1", settlement_cents=0, settled_at_ns=9_000)
    assert outcome.gross_pnl_usd == pytest.approx(-30.0)
    assert p.bankroll_usd == pytest.approx(970.0)


def test_settle_invalid_price_raises() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with pytest.raises(ValueError):
        p.settle(position_id="p1", settlement_cents=50, settled_at_ns=9_000)


def test_reopening_same_id_raises() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with pytest.raises(ValueError):
        p.open_from_fill(
            position_id="p1",
            decision_id="d1",
            fill=_buy(30, 50),
            trap="floor_reversion",
            features_at_entry=_features(),
        )


def test_closing_unknown_position_raises() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    with pytest.raises(ValueError):
        p.close(
            position_id="ghost",
            exit_fill=_sell(60, 10),
            exit_reason="soft_stop",
        )


def test_partial_close_raises() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with pytest.raises(ValueError):
        p.close(
            position_id="p1",
            exit_fill=_sell(50, 50),  # only 50 of 100
            exit_reason="soft_stop",
        )


def test_reset_daily_pnl() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    p.close(position_id="p1", exit_fill=_sell(50, 100), exit_reason="soft_stop")
    assert p.daily_realized_pnl_usd > 0
    p.reset_daily_pnl()
    assert p.daily_realized_pnl_usd == 0.0
