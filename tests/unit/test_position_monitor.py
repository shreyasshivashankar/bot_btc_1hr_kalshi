from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.config.settings import (
    MonitorSettings,
    RiskSettings,
    SoftStopSettings,
)
from bot_btc_1hr_kalshi.execution import OMS, PaperBroker
from bot_btc_1hr_kalshi.execution.broker.base import Fill
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate, TradeEvent
from bot_btc_1hr_kalshi.monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.obs.schemas import Features
from bot_btc_1hr_kalshi.portfolio import Portfolio
from bot_btc_1hr_kalshi.risk import BreakerState

MARKET = "KBTC-26APR1600-B60000"


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


def _book_with_bid(best_bid: int, bid_size: int = 500, best_ask: int = 98) -> L2Book:
    b = L2Book(MARKET)
    b.apply(
        BookUpdate(
            seq=1,
            ts_ns=1_000,
            market_id=MARKET,
            bids=(BookLevel(best_bid, bid_size), BookLevel(best_bid - 1, bid_size)),
            asks=(BookLevel(best_ask, 200),),
            is_snapshot=True,
        )
    )
    return b


def _monitor_settings() -> MonitorSettings:
    return MonitorSettings(
        early_cashout_price_cents=99,
        soft_stop=SoftStopSettings(
            base_fraction=0.3,
            regime_multiplier_high_vol=1.5,
            regime_multiplier_trending=1.3,
            time_multiplier_late_window=0.8,
        ),
        theta_net_book_depth_threshold=100.0,
    )


def _build(bankroll: float = 1000.0) -> tuple[OMS, Portfolio, PositionMonitor, PaperBroker, ManualClock]:
    clock = ManualClock(1_000)
    broker = PaperBroker(clock=clock)
    portfolio = Portfolio(bankroll_usd=bankroll)
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=BreakerState(),
        risk_settings=RiskSettings(
            kelly_fraction=0.25,
            max_position_notional_usd=100.0,
            max_daily_loss_pct=0.05,
        ),
        min_signal_confidence=0.5,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=_monitor_settings())
    return oms, portfolio, monitor, broker, clock


def _open_yes(portfolio: Portfolio, price_cents: int = 30, contracts: int = 10) -> str:
    pid = "pos-1"
    portfolio.open_from_fill(
        position_id=pid,
        decision_id=pid,
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=price_cents,
            contracts=contracts,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="floor_reversion",
        features_at_entry=_features(),
    )
    return pid


async def test_early_cashout_fires_when_bid_at_99() -> None:
    _oms, portfolio, monitor, broker, _clock = _build()
    pid = _open_yes(portfolio)
    book = _book_with_bid(99)
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=30.0, regime_vol="normal", regime_trend="flat"
    )
    assert len(ticks) == 1
    assert ticks[0].action == "early_cashout_99"
    assert portfolio.get(pid) is None  # closed


async def test_theta_net_fires_near_settlement_with_gain() -> None:
    _oms, portfolio, monitor, broker, _clock = _build()
    _open_yes(portfolio, price_cents=30, contracts=10)
    book = _book_with_bid(45, bid_size=500)  # 15c gain, deep book
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=10.0, regime_vol="normal", regime_trend="flat"
    )
    assert ticks[0].action == "theta_net_target"


async def test_soft_stop_fires_on_deep_drawdown() -> None:
    _oms, portfolio, monitor, broker, _clock = _build()
    _open_yes(portfolio, price_cents=30, contracts=10)
    # base_fraction 0.3 -> stop at 30 * 0.7 = 21; bid=20 triggers
    book = _book_with_bid(20)
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=30.0, regime_vol="normal", regime_trend="flat"
    )
    assert ticks[0].action == "soft_stop"


async def test_noop_when_no_exit_triggered() -> None:
    _oms, portfolio, monitor, broker, _clock = _build()
    _open_yes(portfolio, price_cents=30, contracts=10)
    book = _book_with_bid(30)  # flat — no trigger
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=30.0, regime_vol="normal", regime_trend="flat"
    )
    assert ticks[0].action == "noop"


async def test_book_invalid_short_circuits() -> None:
    _oms, portfolio, monitor, _broker, _clock = _build()
    _open_yes(portfolio)
    book = L2Book(MARKET)  # never snapshot
    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=30.0, regime_vol="normal", regime_trend="flat"
    )
    assert ticks[0].action == "skip_book_invalid"


async def test_early_cashout_takes_precedence_over_soft_stop() -> None:
    _oms, portfolio, monitor, broker, _clock = _build()
    _open_yes(portfolio, price_cents=99, contracts=10)  # entry was at 99c (odd but fine)
    book = _book_with_bid(99)  # still 99 bid → early cashout
    broker.register_book(book)
    ticks = await monitor.evaluate(
        book=book, minutes_to_settlement=30.0, regime_vol="normal", regime_trend="flat"
    )
    assert ticks[0].action == "early_cashout_99"


async def test_arb_basis_closed_fires_when_bid_converges_to_fair() -> None:
    """An implied-basis-arb position fires its patient exit the moment
    market price on its side converges to within 3c of current fair
    value. spot==strike → q_yes≈0.5 → fair≈50c; a YES bid at 50 closes
    the basis exactly. Per smart-router routing, arb_basis_closed is a
    *patient* exit — it posts a maker one cent above the bid and rests
    until a counter-aggressor crosses it. The position stays open with
    a registered resting maker until that fill arrives."""
    oms, portfolio, monitor, broker, _clock = _build()
    pid = "pos-arb"
    portfolio.open_from_fill(
        position_id=pid,
        decision_id=pid,
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=30,
            contracts=10,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="implied_basis_arb",
        features_at_entry=_features(),
    )
    book = _book_with_bid(50, bid_size=500)
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert len(ticks) == 1
    assert ticks[0].action == "arb_basis_closed"
    assert portfolio.get(pid) is not None  # rests until counter-aggressor lifts
    assert oms.has_resting_exit(pid)
    assert oms.resting_exit_reason(pid) == "arb_basis_closed"


async def test_arb_basis_closed_skipped_without_spot_context() -> None:
    """When FeedLoop can't hand us a fresh spot (stale oracle), the arb
    exit branch cleanly short-circuits — other priorities still apply."""
    _oms, portfolio, monitor, broker, _clock = _build()
    portfolio.open_from_fill(
        position_id="pos-arb2",
        decision_id="pos-arb2",
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=30,
            contracts=10,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="implied_basis_arb",
        features_at_entry=_features(),
    )
    book = _book_with_bid(50)
    broker.register_book(book)
    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
    )
    # No arb exit without spot context; bid at 50 vs entry 30 is a 20c
    # gain but well above soft-stop so noop.
    assert ticks[0].action == "noop"


async def test_mark_exit_cleared_releases_pending() -> None:
    _oms, _portfolio, monitor, _broker, _clock = _build()
    monitor._pending_exit.add("x1")  # type: ignore[attr-defined]
    monitor.mark_exit_cleared("x1")
    assert "x1" not in monitor._pending_exit  # type: ignore[attr-defined]


def test_adjusted_stop_fraction_respects_multipliers() -> None:
    from bot_btc_1hr_kalshi.monitor.position_monitor import _adjusted_stop_fraction

    base = 0.3
    assert _adjusted_stop_fraction(
        base=base,
        regime_vol="normal",
        regime_trend="flat",
        minutes_to_settlement=30.0,
        high_vol_mult=1.5,
        trending_mult=1.3,
        late_window_mult=0.8,
    ) == pytest.approx(base)
    assert _adjusted_stop_fraction(
        base=base,
        regime_vol="high",
        regime_trend="up",
        minutes_to_settlement=5.0,
        high_vol_mult=1.5,
        trending_mult=1.3,
        late_window_mult=0.8,
    ) == pytest.approx(base * 1.5 * 1.3 * 0.8)


# ---- smart-router exit routing -------------------------------------------


def _open_arb_yes(portfolio: Portfolio, price_cents: int = 30, contracts: int = 10) -> str:
    pid = "pos-arb"
    portfolio.open_from_fill(
        position_id=pid,
        decision_id=pid,
        fill=Fill(
            order_id="o1",
            client_order_id="c1",
            market_id=MARKET,
            side="YES",
            action="BUY",
            price_cents=price_cents,
            contracts=contracts,
            ts_ns=1_000,
            fees_usd=0.0,
        ),
        trap="implied_basis_arb",
        features_at_entry=_features(),
    )
    return pid


def test_classify_exit_route_urgent_vs_patient() -> None:
    from bot_btc_1hr_kalshi.monitor.position_monitor import _classify_exit_route

    assert _classify_exit_route("early_cashout_99") == "ioc"
    assert _classify_exit_route("soft_stop") == "ioc"
    assert _classify_exit_route("tier1_flatten") == "ioc"
    assert _classify_exit_route("theta_net_target") == "maker"
    assert _classify_exit_route("arb_basis_closed") == "maker"


def test_exit_price_for_route_maker_posts_one_above_bid_when_room() -> None:
    from bot_btc_1hr_kalshi.monitor.position_monitor import _exit_price_for_route

    bid = BookLevel(50, 100)
    ask = BookLevel(55, 100)
    price, route = _exit_price_for_route(route="maker", best_bid=bid, best_ask=ask)
    assert route == "maker"
    assert price == 51


def test_exit_price_for_route_degrades_to_ioc_when_spread_too_tight() -> None:
    """`bid+1 >= ask` leaves no room for an inside maker, so we cross."""
    from bot_btc_1hr_kalshi.monitor.position_monitor import _exit_price_for_route

    bid = BookLevel(50, 100)
    ask = BookLevel(51, 100)  # spread of 1 — bid+1 == ask, no room
    price, route = _exit_price_for_route(route="maker", best_bid=bid, best_ask=ask)
    assert route == "ioc"
    assert price == 50  # take the bid


def test_exit_price_for_route_degrades_to_ioc_without_visible_ask() -> None:
    from bot_btc_1hr_kalshi.monitor.position_monitor import _exit_price_for_route

    bid = BookLevel(50, 100)
    price, route = _exit_price_for_route(route="maker", best_bid=bid, best_ask=None)
    assert route == "ioc"
    assert price == 50


def test_exit_price_for_route_ioc_always_takes_bid() -> None:
    from bot_btc_1hr_kalshi.monitor.position_monitor import _exit_price_for_route

    bid = BookLevel(40, 100)
    ask = BookLevel(60, 100)
    price, route = _exit_price_for_route(route="ioc", best_bid=bid, best_ask=ask)
    assert route == "ioc"
    assert price == 40


async def test_patient_arb_exit_posts_resting_maker_then_fills_via_trade() -> None:
    """End-to-end smart-router: arb-basis-closed posts an inside maker at
    bid+1, the broker rests it, and a counter-aggressor TradeEvent at
    that price closes the position via OMS.on_trade_event."""
    oms, portfolio, monitor, broker, clock = _build()
    pid = _open_arb_yes(portfolio, price_cents=30, contracts=10)
    book = _book_with_bid(50, bid_size=500, best_ask=55)
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert ticks[0].action == "arb_basis_closed"
    assert oms.has_resting_exit(pid)
    # Order rests at bid+1 = 51 on the YES sell side; the position is still
    # open. Re-evaluating before a fill should surface the registered
    # resting-maker action so observability can distinguish "parked" from
    # "no signal".
    ticks2 = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert ticks2[0].action == "skip_resting_maker_exit"

    clock.advance_ns(500_000_000)
    # A buy aggressor lifts the YES sell at 51 → resting maker fills
    # entirely → OMS closes the position and clears the registry.
    trade = TradeEvent(
        seq=2,
        ts_ns=clock.now_ns(),
        market_id=MARKET,
        price_cents=51,
        size=10,
        aggressor="buy",
        taker_side="YES",
    )
    unhandled = await oms.on_trade_event(trade)
    assert unhandled == ()  # exit fill, not entry
    assert portfolio.get(pid) is None
    assert not oms.has_resting_exit(pid)


async def test_urgent_early_cashout_escalates_resting_maker_to_ioc() -> None:
    """A patient maker is parked when the bid jumps to 99c. Early cashout
    must cancel the resting maker and resubmit IOC at the cross — the
    99c-window can vanish next tick if a market-maker pulls the bid."""
    oms, portfolio, monitor, broker, clock = _build()
    pid = _open_arb_yes(portfolio, price_cents=30, contracts=10)
    # First tick: bid at 50 → posts patient maker at 51.
    book = _book_with_bid(50, bid_size=500, best_ask=55)
    broker.register_book(book)
    await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert oms.has_resting_exit(pid)

    # Bid jumps to 99 — early cashout escalates. Replace book in place.
    clock.advance_ns(1_000_000)
    book.apply(
        BookUpdate(
            seq=2,
            ts_ns=clock.now_ns(),
            market_id=MARKET,
            bids=(BookLevel(99, 500),),
            asks=(BookLevel(100, 200),),
            is_snapshot=True,
        )
    )
    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert ticks[0].action == "escalated_resting_to_ioc"
    assert portfolio.get(pid) is None  # IOC crossed at 99
    assert not oms.has_resting_exit(pid)


async def test_urgent_soft_stop_escalates_resting_maker_to_ioc() -> None:
    """Same escalation path as early cashout, but driven by drawdown.
    Verifies soft_stop is in the urgent set."""
    oms, portfolio, monitor, broker, clock = _build()
    pid = _open_arb_yes(portfolio, price_cents=30, contracts=10)
    book = _book_with_bid(50, bid_size=500, best_ask=55)
    broker.register_book(book)
    await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert oms.has_resting_exit(pid)

    # Bid collapses to 20 → past stop_price = 30*(1-0.3)=21. soft_stop wins.
    clock.advance_ns(1_000_000)
    book.apply(
        BookUpdate(
            seq=2,
            ts_ns=clock.now_ns(),
            market_id=MARKET,
            bids=(BookLevel(20, 500),),
            asks=(BookLevel(80, 200),),
            is_snapshot=True,
        )
    )
    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert ticks[0].action == "escalated_resting_to_ioc"
    assert portfolio.get(pid) is None
    assert not oms.has_resting_exit(pid)


async def test_patient_maker_degrades_to_ioc_when_spread_too_tight() -> None:
    """When `bid+1 >= ask`, there's no room to post inside; the smart
    router falls back to IOC for that tick. Position closes immediately."""
    oms, portfolio, monitor, broker, _clock = _build()
    pid = _open_arb_yes(portfolio, price_cents=30, contracts=10)
    # bid=50, ask=51 → spread of 1 → no room to make → IOC at 50.
    book = _book_with_bid(50, bid_size=500, best_ask=51)
    broker.register_book(book)

    ticks = await monitor.evaluate(
        book=book,
        minutes_to_settlement=30.0,
        regime_vol="normal",
        regime_trend="flat",
        spot_btc_usd=60_000.0,
        strike_usd=60_000.0,
    )
    assert ticks[0].action == "arb_basis_closed"
    assert portfolio.get(pid) is None  # IOC crossed
    assert not oms.has_resting_exit(pid)
