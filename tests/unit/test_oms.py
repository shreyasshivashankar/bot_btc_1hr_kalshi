from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.config.settings import RiskSettings
from bot_btc_1hr_kalshi.execution import OMS, PaperBroker
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate, TradeEvent
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.portfolio import Portfolio
from bot_btc_1hr_kalshi.risk import BreakerState
from bot_btc_1hr_kalshi.signal.types import TrapSignal

MARKET = "KBTC-26APR1600-B60000"


def _seed_book() -> L2Book:
    b = L2Book(MARKET)
    b.apply(
        BookUpdate(
            seq=1,
            ts_ns=1_000,
            market_id=MARKET,
            bids=(BookLevel(28, 200), BookLevel(27, 400)),
            asks=(BookLevel(32, 200), BookLevel(33, 400)),
            is_snapshot=True,
        )
    )
    return b


def _signal(*, edge: float = 8.0, confidence: float = 0.8) -> TrapSignal:
    return TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=29,
        confidence=confidence,
        edge_cents=edge,
        features=Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=confidence,
            bollinger_pct_b=-0.9,
            atr_cents=10.0,
            book_depth_at_entry=600.0,
            spread_cents=4,
            spot_btc_usd=60_000.0,
            minutes_to_settlement=30.0,
        ),
    )


def _oms(
    *,
    bankroll: float = 1_000.0,
    max_notional: float = 100.0,
    min_conf: float = 0.5,
) -> tuple[OMS, PaperBroker, Portfolio, L2Book, BreakerState, ManualClock]:
    clock = ManualClock(5_000)
    book = _seed_book()
    broker = PaperBroker(clock=clock)
    broker.register_book(book)
    portfolio = Portfolio(bankroll_usd=bankroll)
    breakers = BreakerState()
    risk = RiskSettings(
        kelly_fraction=0.25,
        max_position_notional_usd=max_notional,
        max_daily_loss_pct=0.05,
    )
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=breakers,
        risk_settings=risk,
        min_signal_confidence=min_conf,
        clock=clock,
    )
    return oms, broker, portfolio, book, breakers, clock


async def test_consider_entry_approves_and_rests() -> None:
    oms, _broker, portfolio, _book, _b, _c = _oms()
    result = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert result.decision.approved is True
    assert result.ack is not None
    assert result.ack.status == "resting"
    assert result.decision.sizing.contracts > 0
    # nothing opened yet — waiting for fill
    assert portfolio.open_positions_notional_usd == 0.0


async def test_consider_entry_rejects_below_confidence_floor() -> None:
    oms, _broker, _p, _book, _b, _c = _oms(min_conf=0.9)
    result = await oms.consider_entry(signal=_signal(confidence=0.5), market_id=MARKET)
    assert result.decision.approved is False
    assert result.decision.reject_reason == "below_confidence_floor"
    assert result.ack is None


async def test_consider_entry_rejects_when_breaker_tripped() -> None:
    oms, _broker, _p, _book, breakers, _c = _oms()
    breakers.set_feed_halt(halted=True)
    result = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert result.decision.approved is False
    assert result.decision.reject_reason is not None
    assert "breaker_tripped" in result.decision.reject_reason


async def test_consider_entry_rejects_when_kelly_sizes_zero() -> None:
    # edge 0 -> kelly returns 0 contracts -> risk rejects zero_contracts
    oms, _broker, _p, _book, _b, _c = _oms()
    result = await oms.consider_entry(signal=_signal(edge=0.0), market_id=MARKET)
    assert result.decision.approved is False
    assert result.decision.reject_reason == "zero_contracts"


async def test_full_lifecycle_entry_fill_exit_emits_outcome() -> None:
    oms, broker, portfolio, _book, _b, clock = _oms()
    result = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert result.ack is not None and result.position_id is not None
    contracts = result.decision.sizing.contracts

    clock.advance_ns(1_000_000)
    trade = TradeEvent(
        seq=2,
        ts_ns=clock.now_ns(),
        market_id=MARKET,
        price_cents=29,
        size=contracts,
        aggressor="sell",
        taker_side="YES",
    )
    fills = await broker.match_trade(trade)
    assert len(fills) == 1 and fills[0].contracts == contracts

    oms.on_entry_fill(
        decision_id=result.decision.decision_id,
        fill=fills[0],
        trap=result.decision.trap,
        features_at_entry=result.decision.features,
    )
    assert portfolio.get(result.decision.decision_id) is not None

    clock.advance_ns(60_000_000_000)  # 60s later
    exit_result = await oms.submit_exit(
        position_id=result.decision.decision_id,
        limit_price_cents=28,  # we'll take the 28 bid on IOC
        exit_reason="early_cashout_99",
    )
    assert exit_result.ack.status == "filled"
    assert exit_result.bet_outcome is not None
    assert exit_result.bet_outcome.exit_reason == "early_cashout_99"
    assert exit_result.bet_outcome.entry_price_cents == 29
    assert exit_result.bet_outcome.exit_price_cents == 28
    assert exit_result.bet_outcome.gross_pnl_usd == pytest.approx((28 - 29) * contracts / 100.0)
    assert portfolio.get(result.decision.decision_id) is None


async def test_submit_exit_on_unknown_position_raises() -> None:
    oms, _broker, _p, _book, _b, _c = _oms()
    with pytest.raises(ValueError):
        await oms.submit_exit(
            position_id="nope",
            limit_price_cents=28,
            exit_reason="soft_stop",
        )
