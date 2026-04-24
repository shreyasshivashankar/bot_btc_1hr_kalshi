"""FeatureEngine (Slice 8, Phase 2): per-timeframe feature computation driven
by bar closes from the `MultiTimeframeBus`.

The engine owns one `_TimeframeState` per configured TF label. Each state
bundles the pure accumulators from `signal.indicators` — Wilder RSI,
Bollinger (SMA + population-sigma bands), Wilder ATR — plus a short rolling
window of closes used by the coarse `regime_trend` classifier.

Design choices
--------------

* **Bar-driven, not tick-driven.** `update_spot(tick)` is gone. Ticks
  flow into `MultiTimeframeBus`; the bus fires bar closes into
  `FeatureEngine._on_bar`. This decouples the time-series math from the
  tick cadence — the traps and HTF vetoes now read values that only
  move on bar close, matching how a human trader reads the chart.

* **String TF labels in the public API.** Callers write `fe.rsi("1h")`
  rather than `fe.rsi(3600)`. The label→seconds mapping lives here once
  so every caller reads the same dictionary.

* **App-scope lifetime.** The engine's accumulator state must survive
  across hourly market rolls — the 1H RSI alone needs ~14 hours of 1h
  closes to warm up. `__main__` constructs it alongside the bar_bus
  and attaches once; each per-session FeedLoop receives a reference.

* **`move_24h_pct()` is 1h-specific.** 25 rolling 1h closes span exactly
  24 hours at the endpoints; kept here (not in `indicators.py`) because
  it's a cross-TF derivative, not a pure per-TF indicator.

* **`cvd(tf, periods)` is rolling-bar, not single-bar (Slice 9).** Reading
  one just-closed 5m bar exposes the trap to a boundary-lag trap: a
  cascade at 10:09:45 is invisible if we read the 10:00-10:05 bar. By
  anchoring the deque to the 1m TF and summing the last N closed bars,
  the signal updates every minute with a 5-minute (or configurable)
  lookback, cutting boundary lag without the single-bar noise. Fail-open
  on warmup (fewer than N closed bars) matches the HTF gate pattern.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from bot_btc_1hr_kalshi.obs.schemas import RegimeTrend, RegimeVol
from bot_btc_1hr_kalshi.signal.indicators import (
    ATRAccumulator,
    BollingerAccumulator,
    RSIAccumulator,
)

if TYPE_CHECKING:
    from bot_btc_1hr_kalshi.market_data.bars import Bar, MultiTimeframeBus
    from bot_btc_1hr_kalshi.market_data.types import LiquidationEvent

TF_LABEL_TO_SEC: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "1d": 86400,
}
_SEC_TO_LABEL: dict[int, str] = {v: k for k, v in TF_LABEL_TO_SEC.items()}

# 25 1h closes -> `closes[-1] - closes[0]` spans exactly 24 hours. Drives the
# Runaway Train breaker (DESIGN.md §6.3) — a later phase will REST-backfill
# these on cold start so the ~24h warmup disappears.
MOVE_24H_WINDOW_BARS = 25

# Rolling CVD window: the trap gate reads `cvd("1m", periods=5)` = last five
# closed 1m bars, i.e. a 5-minute lookback that updates every 60s. Keeping
# more than 5 here means callers can cheaply ask for shorter slices too
# (periods=1 for the most recent minute, periods=3 for a 3m rolling sum);
# memory cost is ~16 bytes per bar and we only hold this for timeframes
# actually configured on the engine.
CVD_WINDOW_BARS = 10

# Default rolling CVD window used by `_snapshot()` callers (feedloop /
# replay). A separate constant from the deque capacity so a future settings
# knob can surface this without touching the engine's internal capacity.
CVD_ROLLING_PERIODS = 5

# Liquidation deque capacity (PR-C). 10_000 entries at the Bybit cadence
# (typically a few prints per minute even in calm regimes, hundreds per
# minute during a cascade) covers ~hours of normal flow and ~tens of
# minutes during the worst observed bursts. The window-query method
# walks the deque from the right until it crosses the lookback boundary,
# so capacity does not affect the per-call cost — it only bounds the
# upper memory footprint.
LIQUIDATION_DEQUE_CAPACITY = 10_000


@dataclass(slots=True)
class _TimeframeState:
    rsi: RSIAccumulator
    bollinger: BollingerAccumulator
    atr: ATRAccumulator
    # Rolling close window — used only by the coarse `regime_trend`
    # classifier (first-vs-last 5-bar block).
    closes_window: deque[float] = field(default_factory=lambda: deque(maxlen=20))
    last_close: float | None = None
    # Per-bar signed USD flow (Slice 9). One (buy_usd, sell_usd) tuple per
    # closed bar; deque drops the oldest on overflow. Only populated from
    # `_on_bar` (the bus path) because the tuple is taken straight off
    # `Bar.buy_volume_usd` / `sell_volume_usd`. Direct `ingest_bar` calls
    # used by unit tests bypass this lane intentionally — CVD-specific
    # tests use `ingest_bar_flows` (below) to push flows explicitly.
    cvd_window: deque[tuple[float, float]] = field(
        default_factory=lambda: deque(maxlen=10)
    )


class FeatureEngine:
    """Per-timeframe feature engine driven by `MultiTimeframeBus` closes.

    Construction validates that every requested TF label exists in
    `TF_LABEL_TO_SEC`. `attach(bus)` subscribes a single callback per TF
    so the engine state updates on every bar close without the caller
    having to wire each TF manually.
    """

    __slots__ = (
        "_atr_hi_threshold",
        "_atr_lo_threshold",
        "_bars_1h_closes_usd",
        "_bollinger_period",
        "_liquidations",
        "_states",
    )

    def __init__(
        self,
        *,
        timeframes: list[str],
        bollinger_period: int,
        bollinger_std_mult: float,
        atr_period: int = 14,
        rsi_period: int = 14,
        atr_high_threshold_usd: float = 150.0,
        atr_low_threshold_usd: float = 30.0,
    ) -> None:
        if not timeframes:
            raise ValueError("timeframes must not be empty")
        if len(set(timeframes)) != len(timeframes):
            raise ValueError("timeframes must be unique")
        for tf in timeframes:
            if tf not in TF_LABEL_TO_SEC:
                raise ValueError(
                    f"unknown timeframe {tf!r}; known: {sorted(TF_LABEL_TO_SEC)}"
                )
        self._bollinger_period = bollinger_period
        self._atr_hi_threshold = atr_high_threshold_usd
        self._atr_lo_threshold = atr_low_threshold_usd
        self._states: dict[str, _TimeframeState] = {
            tf: _TimeframeState(
                rsi=RSIAccumulator(period=rsi_period),
                bollinger=BollingerAccumulator(
                    period=bollinger_period, std_mult=bollinger_std_mult
                ),
                atr=ATRAccumulator(period=atr_period),
                closes_window=deque(maxlen=bollinger_period),
                cvd_window=deque(maxlen=CVD_WINDOW_BARS),
            )
            for tf in timeframes
        }
        self._bars_1h_closes_usd: deque[float] = deque(maxlen=MOVE_24H_WINDOW_BARS)
        # Rolling liquidation print stream (PR-C). Fed by
        # `DerivativesOracle.subscribe_liquidations(feature_engine.ingest_liquidation)`
        # at startup. Snapshot builders read `liquidation_usd_in_window` to
        # pre-aggregate `LiquidationPressure`. Deque is monotonic in ts_ns
        # within a single feed; cross-feed interleaving is currently
        # impossible because only Bybit emits liquidation events.
        self._liquidations: deque[LiquidationEvent] = deque(
            maxlen=LIQUIDATION_DEQUE_CAPACITY
        )

    @property
    def timeframes(self) -> tuple[str, ...]:
        return tuple(self._states.keys())

    # ---- bus wiring -------------------------------------------------------

    def attach(self, bus: MultiTimeframeBus) -> None:
        """Subscribe to bar closes on `bus` for each configured TF."""
        for label in self._states:
            sec = TF_LABEL_TO_SEC[label]
            if sec not in bus.timeframes:
                raise ValueError(
                    f"timeframe {label!r} (tf_sec={sec}) not registered on bus "
                    f"(bus has {sorted(bus.timeframes)})"
                )
            bus.subscribe(tf_sec=sec, cb=self._on_bar)

    def _on_bar(self, bar: Bar) -> None:
        label = _SEC_TO_LABEL.get(bar.tf_sec)
        if label is None:
            return
        state = self._states.get(label)
        if state is None:
            return
        self.ingest_bar(label, close=bar.close_usd, high=bar.high_usd, low=bar.low_usd)
        # CVD is bar-close-driven on the bus path; direct `ingest_bar` calls
        # from tests don't carry flow context, so the flow deque only moves
        # from here (or `ingest_bar_flows`). This split keeps all existing
        # FeatureEngine unit tests unchanged.
        state.cvd_window.append((bar.buy_volume_usd, bar.sell_volume_usd))

    def ingest_bar(self, tf: str, *, close: float, high: float, low: float) -> None:
        """Route a single bar close into the TF's accumulators.

        Used by the bus callback and directly by unit tests and replay
        harnesses that want to push synthesized bars without an
        intermediate MultiTimeframeBus.
        """
        state = self._states.get(tf)
        if state is None:
            raise KeyError(f"timeframe {tf!r} not configured on this engine")
        state.rsi.ingest(close)
        state.bollinger.ingest(close)
        state.atr.ingest(high=high, low=low, close=close)
        state.closes_window.append(close)
        state.last_close = close
        if tf == "1h":
            self._bars_1h_closes_usd.append(close)

    def ingest_liquidation(self, event: LiquidationEvent) -> None:
        """Append one liquidation print to the rolling deque (PR-C).

        Called from `DerivativesOracle.subscribe_liquidations` at startup
        (push-based, not bar-driven). Replays bypass this — they push
        synthesized events through the same method when reconstructing
        a session from archived ticks. Deque overflow drops the oldest
        event; `liquidation_usd_in_window` walks from the right so the
        stale-end loss is invisible to any reasonable lookback.
        """
        self._liquidations.append(event)

    def ingest_bar_flows(
        self, tf: str, *, buy_volume_usd: float, sell_volume_usd: float
    ) -> None:
        """Push one closed bar's signed USD flow into the TF's CVD deque.

        The bus path (`_on_bar`) does this automatically from `Bar` objects.
        Tests and replay harnesses that want CVD to be live without running
        a full `MultiTimeframeBus` pipeline call this directly.
        """
        state = self._states.get(tf)
        if state is None:
            raise KeyError(f"timeframe {tf!r} not configured on this engine")
        state.cvd_window.append((buy_volume_usd, sell_volume_usd))

    # ---- per-TF readers ---------------------------------------------------

    def last_close(self, tf: str) -> float | None:
        state = self._states.get(tf)
        return state.last_close if state is not None else None

    def rsi(self, tf: str) -> float | None:
        state = self._states.get(tf)
        return state.rsi.value if state is not None else None

    def bollinger_bands(self, tf: str) -> tuple[float, float, float] | None:
        state = self._states.get(tf)
        return state.bollinger.bands if state is not None else None

    def bollinger_pct_b(
        self, tf: str, *, live_price: float | None = None
    ) -> float | None:
        """Percent-B against `live_price` if supplied, else the latest bar
        close on this TF. `None` until both warmups (window-full +
        at-least-one-close) are satisfied.

        Bands stay bar-anchored — they recompute only when a new bar closes
        — but the penetration measure passed through them moves with the
        live tape. Without a live price, pct_b is frozen for up to one full
        bar interval (5 minutes on the 5m TF), which delays floor / ceiling
        / lag-trap fires by exactly that long. The live-price overload lets
        callers feed the spot oracle's most recent tick so the trap arms
        the moment price re-enters / exits a band, not on the next close.
        Fallback to `last_close` keeps backtest replay and unit tests
        deterministic when no live source is available.
        """
        state = self._states.get(tf)
        if state is None:
            return None
        price = live_price if live_price is not None else state.last_close
        if price is None:
            return None
        return state.bollinger.pct_b(price)

    def atr(self, tf: str) -> float | None:
        state = self._states.get(tf)
        return state.atr.value if state is not None else None

    def regime_vol(self, tf: str) -> RegimeVol:
        state = self._states.get(tf)
        if state is None:
            return "normal"
        atr = state.atr.value
        if atr is None:
            return "normal"
        if atr >= self._atr_hi_threshold:
            return "high"
        if atr <= self._atr_lo_threshold:
            return "low"
        return "normal"

    def regime_trend(
        self, tf: str, *, flat_threshold_usd: float = 25.0
    ) -> RegimeTrend:
        """Coarse trend: compare first-vs-last block of the rolling window."""
        state = self._states.get(tf)
        if state is None:
            return "flat"
        n = len(state.closes_window)
        if n < 5:
            return "flat"
        closes = list(state.closes_window)
        block = min(5, n // 2)
        first = sum(closes[:block]) / block
        last = sum(closes[-block:]) / block
        delta = last - first
        if abs(delta) < flat_threshold_usd:
            return "flat"
        return "up" if delta > 0 else "down"

    def cvd(self, tf: str, *, periods: int) -> float | None:
        """Rolling net aggressor-driven USD flow over the last `periods`
        closed bars on `tf`. Positive = net taker buying; negative = net
        taker selling.

        Returns `None` during warmup (fewer than `periods` closed bars)
        so the trap's CVD gate fails open — identical semantics to the
        HTF RSI and Runaway Train gates from Slice 8.

        `periods` is bounded above by this TF's deque capacity; asking
        for more raises `ValueError` so a caller can't silently get a
        shorter window than they asked for.
        """
        if periods <= 0:
            raise ValueError(f"periods must be > 0, got {periods}")
        state = self._states.get(tf)
        if state is None:
            return None
        window = state.cvd_window
        # `deque.maxlen` is typed `int | None`; the `maxlen=CVD_WINDOW_BARS`
        # at construction guarantees it's non-None, but narrow explicitly so
        # mypy --strict doesn't have to infer.
        maxlen = window.maxlen if window.maxlen is not None else CVD_WINDOW_BARS
        if periods > maxlen:
            raise ValueError(
                f"periods={periods} exceeds CVD deque capacity "
                f"{maxlen} for tf={tf!r}"
            )
        if len(window) < periods:
            return None
        # Slice the rightmost `periods` tuples. `deque` slicing requires a
        # list conversion; CVD_WINDOW_BARS is small (10) so the allocation
        # cost is negligible on the hot path.
        recent = list(window)[-periods:]
        buy_total = sum(buy for buy, _ in recent)
        sell_total = sum(sell for _, sell in recent)
        return buy_total - sell_total

    def liquidation_usd_in_window(
        self,
        *,
        now_ns: int,
        lookback_sec: float,
        side: Literal["long", "short"],
        price_min: float | None = None,
        price_max: float | None = None,
    ) -> float:
        """Sum USD notional of liquidations in the trailing window (PR-C).

        Filters by `side` (the liquidated position's direction) and an
        optional `[price_min, price_max]` band — the snapshot builder
        passes `(spot * (1 - window_pct), spot)` for "longs liquidated
        below spot" and `(spot, spot * (1 + window_pct))` for "shorts
        liquidated above spot".

        Returns 0.0 on cold start (empty deque) — there is no warmup
        notion here, since a freshly-empty window legitimately means
        "no liquidation pressure in this band". Trap callers compare
        against a positive USD threshold, so 0.0 fails the gate cleanly.

        Walks the deque right-to-left, breaking out as soon as ts_ns
        falls outside the lookback window. Cost is O(events_in_window)
        rather than O(deque_capacity).
        """
        if lookback_sec <= 0.0:
            raise ValueError(f"lookback_sec must be > 0, got {lookback_sec}")
        cutoff_ns = now_ns - int(lookback_sec * 1_000_000_000)
        total = 0.0
        for event in reversed(self._liquidations):
            if event.ts_ns < cutoff_ns:
                break
            if event.side != side:
                continue
            if price_min is not None and event.price_usd < price_min:
                continue
            if price_max is not None and event.price_usd > price_max:
                continue
            total += event.size_usd
        return total

    # ---- cross-TF derivatives --------------------------------------------

    def move_24h_pct(self) -> float | None:
        """Signed 24h BTC move as a fraction. Drives the Runaway Train breaker.

        Requires the 1h close deque to be full (25 entries → 24h span).
        Returns `None` during warmup — callers treat `None` as "no HTF
        data yet, gate passes through".
        """
        if len(self._bars_1h_closes_usd) < MOVE_24H_WINDOW_BARS:
            return None
        oldest = self._bars_1h_closes_usd[0]
        newest = self._bars_1h_closes_usd[-1]
        if oldest == 0.0:
            return None
        return (newest - oldest) / oldest
