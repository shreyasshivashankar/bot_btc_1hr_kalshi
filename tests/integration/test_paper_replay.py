"""End-to-end: replay a scripted BTC crash + Kalshi book through the full
signal -> risk -> OMS -> monitor -> BetOutcome pipeline.

This is the canonical happy-path integration: if this breaks we've regressed
something load-bearing. Uses an injected ManualClock so timing is deterministic.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from bot_btc_1hr_kalshi.app import App
from bot_btc_1hr_kalshi.config.loader import load_settings
from bot_btc_1hr_kalshi.execution.broker.paper import PaperBroker
from bot_btc_1hr_kalshi.execution.oms import OMS
from bot_btc_1hr_kalshi.market_data.types import BookLevel, BookUpdate, FeedEvent, SpotTick, TradeEvent
from bot_btc_1hr_kalshi.monitor.position_monitor import PositionMonitor
from bot_btc_1hr_kalshi.obs.clock import ManualClock
from bot_btc_1hr_kalshi.portfolio.positions import Portfolio
from bot_btc_1hr_kalshi.research.replay import ReplayOrchestrator, replay
from bot_btc_1hr_kalshi.risk.breakers import BreakerState
from bot_btc_1hr_kalshi.signal.features import FeatureEngine

REPO_ROOT = Path(__file__).resolve().parents[2]
MARKET = "KBTC-26APR1600-B60000"
NS_PER_SEC = 1_000_000_000


def _book_update(seq: int, ts_ns: int, *, best_bid: int, best_ask: int) -> BookUpdate:
    # All fixture book updates are snapshots — simpler than managing deltas
    # and still exercises the full pipeline. Delta correctness is covered in
    # tests/unit/test_l2_book.py.
    return BookUpdate(
        seq=seq,
        ts_ns=ts_ns,
        market_id=MARKET,
        bids=(BookLevel(best_bid, 500), BookLevel(best_bid - 1, 500)),
        asks=(BookLevel(best_ask, 500), BookLevel(best_ask + 1, 500)),
        is_snapshot=True,
    )


def _spot(ts_ns: int, price_usd: float) -> SpotTick:
    return SpotTick(ts_ns=ts_ns, venue="coinbase", price_usd=price_usd, size=0.01)


def _build_app_and_orch() -> tuple[App, ReplayOrchestrator]:
    settings = load_settings("paper", config_dir=REPO_ROOT / "config")
    clock = ManualClock(0)
    breakers = BreakerState()
    portfolio = Portfolio(bankroll_usd=5_000.0)
    broker = PaperBroker(clock=clock)
    oms = OMS(
        broker=broker,
        portfolio=portfolio,
        breakers=breakers,
        risk_settings=settings.risk,
        min_signal_confidence=settings.signal.min_signal_confidence,
        clock=clock,
    )
    monitor = PositionMonitor(oms=oms, portfolio=portfolio, settings=settings.monitor)
    app = App(
        settings=settings,
        clock=clock,
        breakers=breakers,
        portfolio=portfolio,
        oms=oms,
        monitor=monitor,
    )
    # Tight bands (std_mult=1.0) + short lookback so a single sharp dip produces
    # a strong pct_b signal for the test. Production uses settings.signal.*;
    # this is a pipeline test, not a signal-tuning test. High ATR thresholds
    # keep the crash from being classified as "high vol".
    features = FeatureEngine(
        bollinger_period=10,
        bollinger_std_mult=1.0,
        atr_period=2,
        atr_high_threshold_usd=50_000.0,
        atr_low_threshold_usd=1.0,
    )
    orch = ReplayOrchestrator(
        app=app,
        broker=broker,
        clock=clock,
        market_id=MARKET,
        feature_engine=features,
        strike_usd=60_000.0,
    )
    return app, orch


def _warmup_events(start_ns: int) -> list[FeedEvent]:
    """Seed the book and fill the Bollinger window with quiet spot."""
    out: list[FeedEvent] = [
        _book_update(1, start_ns, best_bid=40, best_ask=45),
    ]
    ts = start_ns
    for i in range(11):
        ts += NS_PER_SEC
        jitter = 10.0 if i % 2 == 0 else -10.0
        out.append(_spot(ts, 60_000.0 + jitter))
    return out


def _crash_events(start_ns: int) -> list[FeedEvent]:
    """One sharp spot plunge -> pct_b far below -1.0 (tight bands) ->
    strong floor-reversion signal."""
    return [_spot(start_ns + NS_PER_SEC, 55_000.0)]


def _fill_and_rebound_events(start_ns: int) -> list[FeedEvent]:
    """Aggressive sell hits our resting maker BUY; book then rebounds so the
    monitor fires early-cashout and the IOC exit walks the 99c bid."""
    ts = start_ns
    out: list[FeedEvent] = [
        # Ask drops to 30c — trap fires and we post a maker BUY @ best_bid (28).
        _book_update(2, ts, best_bid=28, best_ask=30),
    ]
    ts += NS_PER_SEC
    # Sell-aggressor trade at 28c — matches our resting BUY YES @ 28.
    out.append(
        TradeEvent(
            seq=3,
            ts_ns=ts,
            market_id=MARKET,
            price_cents=28,
            size=500,  # more than any reasonable Kelly size at this bankroll
            aggressor="sell",
            taker_side="YES",
        )
    )
    ts += NS_PER_SEC
    # Book rebounds hard — best bid jumps to 99c (early cashout trigger).
    out.append(_book_update(4, ts, best_bid=99, best_ask=100))
    return out


@pytest.mark.integration
async def test_replay_floor_reversion_happy_path_emits_bet_outcome() -> None:
    app, orch = _build_app_and_orch()

    events: list[FeedEvent] = []
    events.extend(_warmup_events(NS_PER_SEC))
    events.extend(_crash_events(events[-1].ts_ns))
    events.extend(_fill_and_rebound_events(events[-1].ts_ns + NS_PER_SEC))

    result = await replay(events, orch)

    assert result.entries_attempted >= 1, f"no entry attempted; rejects={result.reject_reasons}"
    assert result.entries_approved >= 1, f"no entry approved; rejects={result.reject_reasons}"
    assert len(result.fills) >= 1, "expected at least one entry fill"

    # Position was opened and then closed via early-cashout exit.
    assert app.portfolio.open_positions == ()
    # Profit: bought around 30c, early-cashout at 99c.
    assert app.portfolio.daily_realized_pnl_usd > 0.0


@pytest.mark.integration
async def test_replay_no_entry_when_book_invalid() -> None:
    """Hard rule #9: features must be treated as INVALID until the book is
    rebuilt from a REST snapshot. This test sends deltas without a snapshot
    and confirms no trap fires."""
    app, orch = _build_app_and_orch()

    events: list[FeedEvent] = [
        # NOT a snapshot and not preceded by one -> book never becomes valid
        BookUpdate(
            seq=1,
            ts_ns=NS_PER_SEC,
            market_id=MARKET,
            bids=(BookLevel(28, 100),),
            asks=(BookLevel(30, 100),),
            is_snapshot=False,
        ),
    ]
    # Even if spot crashes, no signal should fire.
    ts = NS_PER_SEC
    for i in range(25):
        ts += NS_PER_SEC
        events.append(_spot(ts, 60_000.0 - i * 50.0))

    result = await replay(events, orch)
    assert result.entries_attempted == 0
    assert app.portfolio.open_positions == ()


@pytest.mark.integration
async def test_replay_respects_trading_halt() -> None:
    app, orch = _build_app_and_orch()
    app.halt()

    events: list[FeedEvent] = []
    events.extend(_warmup_events(NS_PER_SEC))
    events.extend(_crash_events(events[-1].ts_ns))
    # Add the book that *would* trigger a trap if we weren't halted.
    events.append(_book_update(2, events[-1].ts_ns + NS_PER_SEC, best_bid=28, best_ask=30))

    result = await replay(events, orch)
    assert result.entries_attempted == 0
    assert app.portfolio.open_positions == ()
