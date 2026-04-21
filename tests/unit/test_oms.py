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


async def test_consider_entry_applies_inverted_risk_clip_at_60c() -> None:
    """Slice 11 Phase 3.2 end-to-end: a 60¢ entry with clip enabled sizes
    smaller than the same 60¢ entry with clip disabled. Proves the two
    new RiskSettings fields are threaded through consider_entry into
    kelly_contracts — unit tests of kelly_contracts alone can't catch a
    regression where OMS forgets to pass the kwargs.

    Uses a dedicated 60¢ book so the resulting maker order can actually
    rest; entry_price below the premium cap (75¢) so Phase 3.1 doesn't
    intercept first; bankroll large enough to leave Kelly unconstrained
    by the per-position notional cap so the multiplier effect dominates."""
    market = "KBTC-26APR1600-B60500"
    book = L2Book(market)
    book.apply(
        BookUpdate(
            seq=1,
            ts_ns=1_000,
            market_id=market,
            bids=(BookLevel(59, 200),),
            asks=(BookLevel(61, 200),),
            is_snapshot=True,
        )
    )

    def _build(multiplier: float) -> OMS:
        clock = ManualClock(5_000)
        broker = PaperBroker(clock=clock)
        broker.register_book(book)
        return OMS(
            broker=broker,
            portfolio=Portfolio(bankroll_usd=10_000.0),
            breakers=BreakerState(),
            risk_settings=RiskSettings(
                kelly_fraction=0.25,
                max_position_notional_usd=10_000.0,  # unconstrained — let Kelly speak
                max_daily_loss_pct=0.05,
                inverted_risk_threshold_cents=50,
                inverted_risk_kelly_multiplier=multiplier,
            ),
            min_signal_confidence=0.5,
            clock=clock,
        )

    signal_60c = TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=60,
        confidence=0.8,
        edge_cents=8.0,
        features=Features(
            regime_trend="flat",
            regime_vol="normal",
            signal_confidence=0.8,
            bollinger_pct_b=-0.9,
            atr_cents=10.0,
            book_depth_at_entry=200.0,
            spread_cents=2,
            spot_btc_usd=60_000.0,
            minutes_to_settlement=30.0,
        ),
    )

    unclipped_oms = _build(multiplier=1.0)
    clipped_oms = _build(multiplier=0.5)

    unclipped = await unclipped_oms.consider_entry(signal=signal_60c, market_id=market)
    clipped = await clipped_oms.consider_entry(signal=signal_60c, market_id=market)

    assert unclipped.decision.approved and clipped.decision.approved
    assert clipped.decision.sizing.contracts < unclipped.decision.sizing.contracts
    # Recorded Kelly fraction reflects the effective (clipped) allocation,
    # not the raw setting — journal readers can diff to detect clip firings.
    assert clipped.decision.sizing.kelly_fraction == pytest.approx(0.25 * 0.5)
    assert unclipped.decision.sizing.kelly_fraction == pytest.approx(0.25)


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


async def test_exit_client_order_id_is_nonced() -> None:
    """Regression: an exit retry after a lost ack must not collide on
    client_order_id — nonce it with the clock's ns timestamp."""
    from bot_btc_1hr_kalshi.execution.broker.base import OrderRequest

    oms, broker, _p, _book, _b, clock = _oms()
    res = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert res.position_id is not None
    clock.advance_ns(1_000_000)
    trade = TradeEvent(
        seq=2,
        ts_ns=clock.now_ns(),
        market_id=MARKET,
        price_cents=29,
        size=res.decision.sizing.contracts,
        aggressor="sell",
        taker_side="YES",
    )
    fills = await broker.match_trade(trade)
    oms.on_entry_fill(
        decision_id=res.decision.decision_id,
        fill=fills[0],
        trap=res.decision.trap,
        features_at_entry=res.decision.features,
    )

    submitted: list[OrderRequest] = []
    original_submit = broker.submit

    async def capture(req: OrderRequest) -> object:
        submitted.append(req)
        return await original_submit(req)

    broker.submit = capture  # type: ignore[method-assign]

    # Both exits cancel (no bid >= 99) so the position survives for the
    # second attempt; the nonce should still differ between them.
    clock.advance_ns(1_000_000)
    r1 = await oms.submit_exit(position_id=res.position_id, limit_price_cents=99, exit_reason="soft_stop")
    clock.advance_ns(1_000_000)
    r2 = await oms.submit_exit(position_id=res.position_id, limit_price_cents=99, exit_reason="soft_stop")

    assert r1.ack.status == "cancelled"
    assert r2.ack.status == "cancelled"
    assert len(submitted) == 2
    assert submitted[0].client_order_id != submitted[1].client_order_id
    assert submitted[0].client_order_id.startswith(f"exit-{res.position_id}-")


async def test_submit_exit_partial_fill_shrinks_position_and_emits_partial_outcome() -> None:
    """The book can only absorb part of the exit; Kalshi returns partially_filled.
    Portfolio must shrink, BetOutcome must tag as partial, position stays open
    for the remainder."""
    oms, broker, portfolio, book, _b, clock = _oms()
    res = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert res.position_id is not None
    clock.advance_ns(1_000_000)
    trade = TradeEvent(
        seq=2,
        ts_ns=clock.now_ns(),
        market_id=MARKET,
        price_cents=29,
        size=res.decision.sizing.contracts,
        aggressor="sell",
        taker_side="YES",
    )
    fills = await broker.match_trade(trade)
    oms.on_entry_fill(
        decision_id=res.decision.decision_id,
        fill=fills[0],
        trap=res.decision.trap,
        features_at_entry=res.decision.features,
    )
    contracts = res.decision.sizing.contracts
    assert contracts >= 10  # sanity — test depends on this

    # Thin the book: only 1 contract available at the 28 bid for YES exit.
    book.apply(
        BookUpdate(
            seq=3,
            ts_ns=clock.now_ns(),
            market_id=MARKET,
            bids=(BookLevel(28, 1),),
            asks=(BookLevel(32, 200),),
            is_snapshot=True,
        )
    )

    clock.advance_ns(60_000_000_000)
    exit_result = await oms.submit_exit(
        position_id=res.position_id,
        limit_price_cents=28,
        exit_reason="soft_stop",
    )
    assert exit_result.ack.status == "partially_filled"
    assert exit_result.bet_outcome is not None
    assert exit_result.bet_outcome.bet_id == f"{res.position_id}-p1"
    assert exit_result.bet_outcome.contracts == 1

    remaining = portfolio.get(res.position_id)
    assert remaining is not None
    assert remaining.contracts == contracts - 1


async def test_submit_exit_rejected_does_not_mutate_portfolio() -> None:
    """If the broker rejects the exit, no BetOutcome is emitted and the
    portfolio position is unchanged — the monitor retries next tick."""
    oms, broker, portfolio, book, _b, clock = _oms()
    res = await oms.consider_entry(signal=_signal(), market_id=MARKET)
    assert res.position_id is not None
    clock.advance_ns(1_000_000)
    trade = TradeEvent(
        seq=2,
        ts_ns=clock.now_ns(),
        market_id=MARKET,
        price_cents=29,
        size=res.decision.sizing.contracts,
        aggressor="sell",
        taker_side="YES",
    )
    fills = await broker.match_trade(trade)
    oms.on_entry_fill(
        decision_id=res.decision.decision_id,
        fill=fills[0],
        trap=res.decision.trap,
        features_at_entry=res.decision.features,
    )
    contracts_before = portfolio.get(res.position_id).contracts  # type: ignore[union-attr]

    # Force a rejection: empty the book so the broker returns status=cancelled
    # (IOC with no liquidity produces no fills and is cancelled).
    book.apply(
        BookUpdate(
            seq=3,
            ts_ns=clock.now_ns(),
            market_id=MARKET,
            bids=(),
            asks=(BookLevel(32, 200),),
            is_snapshot=True,
        )
    )
    clock.advance_ns(60_000_000_000)
    exit_result = await oms.submit_exit(
        position_id=res.position_id,
        limit_price_cents=28,
        exit_reason="soft_stop",
    )
    assert exit_result.ack.status in ("cancelled", "rejected")
    assert exit_result.bet_outcome is None
    pos = portfolio.get(res.position_id)
    assert pos is not None
    assert pos.contracts == contracts_before


async def test_submit_exit_on_unknown_position_raises() -> None:
    oms, _broker, _p, _book, _b, _c = _oms()
    with pytest.raises(ValueError):
        await oms.submit_exit(
            position_id="nope",
            limit_price_cents=28,
            exit_reason="soft_stop",
        )


def test_aggregate_sell_fill_uses_round_half_up_not_bankers() -> None:
    """Python's built-in round() is banker's rounding — round(0.5) == 0.
    For VWAP cents, we want half-away-from-zero so accumulated fills do not
    drift toward even values over time."""
    from bot_btc_1hr_kalshi.execution.broker.base import Fill
    from bot_btc_1hr_kalshi.execution.oms import _aggregate_sell_fill

    # 50 @ 40¢ + 50 @ 41¢ → notional = 4050¢, total = 100 → exactly 40.5¢.
    # round() gives 40 (banker's), round-half-up gives 41.
    fills = (
        Fill(
            order_id="o",
            client_order_id="c",
            market_id="M",
            side="YES",
            action="SELL",
            price_cents=40,
            contracts=50,
            ts_ns=100,
            fees_usd=0.0,
        ),
        Fill(
            order_id="o",
            client_order_id="c",
            market_id="M",
            side="YES",
            action="SELL",
            price_cents=41,
            contracts=50,
            ts_ns=200,
            fees_usd=0.0,
        ),
    )
    agg = _aggregate_sell_fill(fills)
    assert agg.price_cents == 41
    assert agg.contracts == 100
    # First fill's timestamp anchors the aggregate — this is when the IOC
    # began executing. Changing this re-aligns latency attribution, so pin it.
    assert agg.ts_ns == 100


def test_aggregate_sell_fill_sums_fees_and_contracts() -> None:
    from bot_btc_1hr_kalshi.execution.broker.base import Fill
    from bot_btc_1hr_kalshi.execution.oms import _aggregate_sell_fill

    fills = (
        Fill(
            order_id="o",
            client_order_id="c",
            market_id="M",
            side="YES",
            action="SELL",
            price_cents=30,
            contracts=10,
            ts_ns=1,
            fees_usd=0.10,
        ),
        Fill(
            order_id="o",
            client_order_id="c",
            market_id="M",
            side="YES",
            action="SELL",
            price_cents=31,
            contracts=20,
            ts_ns=2,
            fees_usd=0.20,
        ),
        Fill(
            order_id="o",
            client_order_id="c",
            market_id="M",
            side="YES",
            action="SELL",
            price_cents=32,
            contracts=30,
            ts_ns=3,
            fees_usd=0.30,
        ),
    )
    agg = _aggregate_sell_fill(fills)
    assert agg.contracts == 60
    assert agg.fees_usd == pytest.approx(0.60)
    # weighted avg = (30*10 + 31*20 + 32*30) / 60 = 1880/60 ≈ 31.333 → 31 cents
    assert agg.price_cents == 31
