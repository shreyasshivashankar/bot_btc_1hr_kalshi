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
from bot_btc_1hr_kalshi.obs.money import usd_to_micros
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
    return SpotTick(
        ts_ns=ts_ns,
        venue="coinbase",
        price_micros=usd_to_micros(price_usd),
        size=0.01,
    )


def _build_app_and_orch() -> tuple[App, ReplayOrchestrator, FeatureEngine]:
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
        timeframes=["5m", "1h"],
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
    return app, orch, features


def _prewarm_bollinger_quiet(features: FeatureEngine) -> None:
    """Pre-fill the 5m Bollinger window + ATR with a quiet 60k regime.

    Injected directly via `ingest_bar` to avoid ≥50 minutes of synthetic
    replay time before the bar bus fires 10 5m closes — bar aggregation
    is exercised in `tests/unit/test_features.py`; this test covers the
    trap→risk→OMS→monitor path.
    """
    for i in range(11):
        jitter = 10.0 if i % 2 == 0 else -10.0
        close = 60_000.0 + jitter
        features.ingest_bar("5m", close=close, high=close, low=close)


def _inject_crash_close(features: FeatureEngine) -> None:
    """Stamp one extreme 5m close well below the band → pct_b < -1,
    priming the floor-reversion trap for the next book update."""
    features.ingest_bar("5m", close=55_000.0, high=55_000.0, low=55_000.0)


def _prewarm_1h_rsi_bearish(features: FeatureEngine) -> None:
    """Stamp a strictly descending series of 1h closes so the Wilder 1H
    RSI drops near zero — far below the 45 HTF-bearish veto threshold.

    15 deltas satisfy the RSI warmup (period=14); all-down deltas give
    avg_gain=0 ⇒ RSI=0. Only 16 bars are stamped so the 25-bar
    `move_24h_pct` deque remains in warmup and the Runaway Train gate
    (ceiling-only anyway) cannot confound the veto under test.
    """
    for i in range(16):
        close = 60_000.0 - i * 100.0
        features.ingest_bar("1h", close=close, high=close, low=close)


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
    app, orch, features = _build_app_and_orch()
    _prewarm_bollinger_quiet(features)

    # Book snapshot + quiet spot noise — no trap should fire yet (pct_b is
    # near mid-band from the pre-warm).
    pre_crash: list[FeedEvent] = []
    pre_crash.extend(_warmup_events(NS_PER_SEC))
    pre_crash.extend(_crash_events(pre_crash[-1].ts_ns))
    result = await replay(pre_crash, orch)
    assert result.entries_attempted == 0, "pre-crash: pct_b should still be near mid-band"

    # Stamp the crash close → pct_b drops below -1 → floor-reversion primed.
    _inject_crash_close(features)

    post_crash = _fill_and_rebound_events(pre_crash[-1].ts_ns + NS_PER_SEC)
    result = await replay(post_crash, orch)

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
    app, orch, features = _build_app_and_orch()
    # Pre-warm + stamp the crash close so pct_b is in trap-firing range;
    # the book-invalid gate is what must block entry, not cold features.
    _prewarm_bollinger_quiet(features)
    _inject_crash_close(features)

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
    app, orch, features = _build_app_and_orch()
    # Prime the engine so the trap WOULD fire absent the halt — otherwise
    # this test would pass trivially on cold features.
    _prewarm_bollinger_quiet(features)
    _inject_crash_close(features)
    app.halt()

    events: list[FeedEvent] = []
    events.extend(_warmup_events(NS_PER_SEC))
    events.extend(_crash_events(events[-1].ts_ns))
    # Add the book that *would* trigger a trap if we weren't halted.
    events.append(_book_update(2, events[-1].ts_ns + NS_PER_SEC, best_bid=28, best_ask=30))

    result = await replay(events, orch)
    assert result.entries_attempted == 0
    assert app.portfolio.open_positions == ()


@pytest.mark.integration
async def test_replay_htf_veto_blocks_floor_entry_and_stays_silent() -> None:
    """End-to-end proof: HTF RSI veto inside detect_floor_reversion prevents
    a setup that would otherwise fire, and emits no DecisionRecord (Slice 8
    architectural reason: no decision-journal spam from pre-gate rejects).

    Mirror of the happy-path test, except the 1H RSI is pre-warmed to ~0
    (strongly bearish, < 45 default veto). Same 5m setup would otherwise
    enter and close at profit (see happy-path); here it must fail silent.
    """
    app, orch, features = _build_app_and_orch()
    _prewarm_bollinger_quiet(features)
    _prewarm_1h_rsi_bearish(features)
    _inject_crash_close(features)

    # Seed book + a few spot ticks so `_spot_price` is populated — without
    # this the snapshot returns None before the trap is even called, and
    # the test would pass trivially without exercising the HTF path.
    events: list[FeedEvent] = []
    events.extend(_warmup_events(NS_PER_SEC))
    events.extend(_fill_and_rebound_events(events[-1].ts_ns + NS_PER_SEC))
    result = await replay(events, orch)

    # Silent rejection — no attempt, no decision, no position.
    assert result.entries_attempted == 0
    assert result.entries_rejected == 0
    assert result.reject_reasons == []
    assert app.portfolio.open_positions == ()
