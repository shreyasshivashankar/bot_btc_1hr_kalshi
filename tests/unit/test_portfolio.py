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


def test_partial_close_shrinks_position_and_emits_proportional_outcome() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100, fees=1.0),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    # Close 40 of 100 contracts at 50c with 0.40 exit fee.
    partial = p.partial_close(
        position_id="p1",
        exit_fill=_sell(50, 40, fees=0.40),
        exit_reason="theta_net_target",
        partial_seq=1,
    )
    # Gross: 40 * (50 - 30) / 100 = $8.00
    assert partial.gross_pnl_usd == pytest.approx(8.0)
    # Entry fee share proportional: 1.0 * 40/100 = $0.40
    assert partial.fees_usd == pytest.approx(0.80)
    assert partial.net_pnl_usd == pytest.approx(7.20)
    assert partial.bet_id == "p1-p1"
    assert partial.contracts == 40

    # Remaining position: 60 contracts, $0.60 entry fees left.
    pos = p.get("p1")
    assert pos is not None
    assert pos.contracts == 60
    assert pos.fees_paid_usd == pytest.approx(0.60)

    # Bankroll: -$30 entry -$1 entry fee +(40 * 0.50 - 0.40) exit = -31 + 19.60
    assert p.bankroll_usd == pytest.approx(1000.0 - 31.0 + 19.60)


def test_partial_close_rejects_full_or_zero_slice() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    with pytest.raises(ValueError, match="partial close must be"):
        p.partial_close(position_id="p1", exit_fill=_sell(50, 0), exit_reason="soft_stop", partial_seq=1)
    with pytest.raises(ValueError, match="partial close must be"):
        p.partial_close(position_id="p1", exit_fill=_sell(50, 100), exit_reason="soft_stop", partial_seq=1)


def test_partial_then_full_close_sums_correctly() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1",
        decision_id="d1",
        fill=_buy(30, 100, fees=1.0),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    p.partial_close(
        position_id="p1", exit_fill=_sell(50, 40), exit_reason="theta_net_target", partial_seq=1,
    )
    final = p.close(position_id="p1", exit_fill=_sell(60, 60), exit_reason="soft_stop")
    # Final leg: 60 * (60-30)/100 = $18 gross, minus remaining $0.60 entry share.
    assert final.gross_pnl_usd == pytest.approx(18.0)
    assert final.fees_usd == pytest.approx(0.60)
    assert final.contracts == 60
    assert p.get("p1") is None


# --- count_correlated_open -------------------------------------------------
# Same-hour correlation counter used by risk.check. Same (side, settlement_ts_ns)
# = one correlated directional bet; the cap prevents stacking multiple strikes
# that would all win/lose together on the same BTC settlement print.

HOUR_A_NS = 1_800_000_000_000_000_000
HOUR_B_NS = 1_800_000_000_000_000_000 + 3_600 * 1_000_000_000


def _buy_side(market: str, side: str, ts_ns: int = 1_000) -> Fill:
    return Fill(
        order_id="o", client_order_id="c", market_id=market,
        side=side,  # type: ignore[arg-type]
        action="BUY", price_cents=30, contracts=10, ts_ns=ts_ns, fees_usd=0.0,
    )


def test_count_correlated_open_zero_when_empty() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    assert p.count_correlated_open(side="YES", settlement_ts_ns=HOUR_A_NS) == 0


def test_count_correlated_open_matches_same_hour_same_side() -> None:
    """Two YES positions on different strikes of the same hour count as 2.
    Same hour = same settlement_ts_ns (all strikes of an hourly series
    settle at the exact same instant)."""
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1", decision_id="d1",
        fill=_buy_side("KXBTC-26APR1817-B75000", "YES"),
        trap="floor_reversion", features_at_entry=_features(),
        settlement_ts_ns=HOUR_A_NS,
    )
    p.open_from_fill(
        position_id="p2", decision_id="d2",
        fill=_buy_side("KXBTC-26APR1817-B75500", "YES"),
        trap="floor_reversion", features_at_entry=_features(),
        settlement_ts_ns=HOUR_A_NS,
    )
    assert p.count_correlated_open(side="YES", settlement_ts_ns=HOUR_A_NS) == 2


def test_count_correlated_open_ignores_different_hour() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1", decision_id="d1",
        fill=_buy_side("KXBTC-26APR1817-B75000", "YES"),
        trap="floor_reversion", features_at_entry=_features(),
        settlement_ts_ns=HOUR_A_NS,
    )
    assert p.count_correlated_open(side="YES", settlement_ts_ns=HOUR_B_NS) == 0


def test_count_correlated_open_ignores_opposite_side() -> None:
    """YES and NO are uncorrelated (one wins when the other loses), so a
    concurrent NO at the same hour doesn't consume the YES-hour quota."""
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1", decision_id="d1",
        fill=_buy_side("KXBTC-26APR1817-B75000", "NO"),
        trap="ceiling_reversion", features_at_entry=_features(),
        settlement_ts_ns=HOUR_A_NS,
    )
    assert p.count_correlated_open(side="YES", settlement_ts_ns=HOUR_A_NS) == 0
    assert p.count_correlated_open(side="NO", settlement_ts_ns=HOUR_A_NS) == 1


def test_count_correlated_open_excludes_closed_positions() -> None:
    p = Portfolio(bankroll_usd=1000.0)
    p.open_from_fill(
        position_id="p1", decision_id="d1",
        fill=_buy_side("KXBTC-26APR1817-B75000", "YES"),
        trap="floor_reversion", features_at_entry=_features(),
        settlement_ts_ns=HOUR_A_NS,
    )
    p.close(position_id="p1", exit_fill=_sell(50, 10), exit_reason="soft_stop")
    assert p.count_correlated_open(side="YES", settlement_ts_ns=HOUR_A_NS) == 0


def test_bankroll_has_no_float_drift_over_many_small_fees() -> None:
    """Regression: 10_000 sequential $0.003 fee deductions must produce an
    exact $30.00 reduction. With plain float arithmetic this drifts in the
    ~1e-11 range; integer-micro storage is exact."""
    p = Portfolio(bankroll_usd=1000.0)
    for i in range(10_000):
        p.open_from_fill(
            position_id=f"p{i}",
            decision_id=f"d{i}",
            fill=_buy(50, 1, fees=0.003),
            trap="floor_reversion",
            features_at_entry=_features(),
        )
        p.settle(position_id=f"p{i}", settlement_cents=100, settled_at_ns=9_000 + i)
    # Per cycle: -$0.50 entry - $0.003 fee + $1.00 settle = +$0.497 net.
    # Over 10_000 cycles: +$4970.00 exactly (would drift ~1e-10 with floats).
    assert p.bankroll_usd == 5970.0
    assert p.daily_realized_pnl_usd == 4970.0
