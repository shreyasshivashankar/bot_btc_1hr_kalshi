from __future__ import annotations

import pytest

from bot_btc_1hr_kalshi.execution.broker import OrderRequest, PaperBroker
from bot_btc_1hr_kalshi.market_data.book import L2Book
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate, TradeEvent
from bot_btc_1hr_kalshi.obs.clock import ManualClock

MARKET = "KBTC-26APR1600-B60000"


def _seed_book(market_id: str = MARKET) -> L2Book:
    b = L2Book(market_id)
    b.apply(
        BookUpdate(
            seq=1,
            ts_ns=1_000,
            market_id=market_id,
            bids=(BookLevel(28, 100), BookLevel(27, 200)),
            asks=(BookLevel(32, 100), BookLevel(33, 200)),
            is_snapshot=True,
        )
    )
    return b


def _req(**overrides: object) -> OrderRequest:
    defaults: dict[str, object] = {
        "client_order_id": "c1",
        "market_id": MARKET,
        "side": "YES",
        "action": "BUY",
        "limit_price_cents": 28,
        "contracts": 10,
        "order_type": "maker",
    }
    defaults.update(overrides)
    return OrderRequest(**defaults)  # type: ignore[arg-type]


async def test_ioc_fills_at_best_ask() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(order_type="ioc", limit_price_cents=32, contracts=10))
    assert ack.status == "filled"
    assert ack.filled_contracts == 10
    assert ack.fills[0].price_cents == 32


async def test_ioc_walks_multiple_levels() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(order_type="ioc", limit_price_cents=33, contracts=150))
    assert ack.status == "filled"
    assert sum(f.contracts for f in ack.fills) == 150
    assert {f.price_cents for f in ack.fills} == {32, 33}


async def test_ioc_cancels_residual_when_limit_too_aggressive() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    # only 100 available at 32; limit 32 with 150 requested -> partial
    ack = await broker.submit(_req(order_type="ioc", limit_price_cents=32, contracts=150))
    assert ack.status == "partially_filled"
    assert ack.filled_contracts == 100
    assert ack.remaining_contracts == 50


async def test_maker_buy_rests_below_best_ask() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(limit_price_cents=29, contracts=10))
    assert ack.status == "resting"
    open_orders = await broker.list_open_orders()
    assert len(open_orders) == 1


async def test_maker_buy_refuses_to_cross_the_spread() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(limit_price_cents=32, contracts=10))  # at best ask
    assert ack.status == "rejected"
    assert ack.reason == "maker_would_cross"


async def test_maker_fills_on_matching_trade() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(limit_price_cents=29, contracts=10))
    assert ack.status == "resting"

    trade = TradeEvent(
        seq=2,
        ts_ns=2_000,
        market_id=MARKET,
        price_cents=29,
        size=4,
        aggressor="sell",
        taker_side="YES",
    )
    fills = await broker.match_trade(trade)
    assert len(fills) == 1 and fills[0].contracts == 4
    remaining = await broker.list_open_orders()
    assert remaining[0].remaining_contracts == 6


async def test_match_trade_skips_opposite_side() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    await broker.submit(_req(side="YES", limit_price_cents=29, contracts=10))
    trade = TradeEvent(
        seq=2,
        ts_ns=2_000,
        market_id=MARKET,
        price_cents=29,
        size=4,
        aggressor="sell",
        taker_side="NO",  # different side -> no fill
    )
    fills = await broker.match_trade(trade)
    assert fills == ()


async def test_cancel_removes_resting() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req(limit_price_cents=29))
    assert await broker.cancel(ack.order_id) is True
    assert await broker.cancel(ack.order_id) is False


async def test_submit_rejects_when_book_invalid() -> None:
    b = L2Book(MARKET)  # never snapshot -> invalid
    broker = PaperBroker(clock=ManualClock(1_000))
    broker.register_book(b)
    ack = await broker.submit(_req())
    assert ack.status == "rejected"
    assert ack.reason and ack.reason.startswith("book_invalid:")


async def test_submit_rejects_unknown_market() -> None:
    broker = PaperBroker(clock=ManualClock(1_000))
    ack = await broker.submit(_req())
    assert ack.status == "rejected"
    assert ack.reason == "unknown_market"


async def test_fee_is_applied_per_contract() -> None:
    b = _seed_book()
    broker = PaperBroker(clock=ManualClock(1_000), fee_per_contract_usd=0.01)
    broker.register_book(b)
    ack = await broker.submit(_req(order_type="ioc", limit_price_cents=32, contracts=10))
    assert ack.fills[0].fees_usd == pytest.approx(0.10)


def test_negative_fee_raises() -> None:
    with pytest.raises(ValueError):
        PaperBroker(clock=ManualClock(0), fee_per_contract_usd=-1.0)
