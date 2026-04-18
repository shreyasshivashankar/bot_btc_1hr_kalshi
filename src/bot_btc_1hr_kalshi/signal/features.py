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
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from bot_btc_1hr_kalshi.obs.schemas import RegimeTrend, RegimeVol
from bot_btc_1hr_kalshi.signal.indicators import (
    ATRAccumulator,
    BollingerAccumulator,
    RSIAccumulator,
)

if TYPE_CHECKING:
    from bot_btc_1hr_kalshi.market_data.bars import Bar, MultiTimeframeBus

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


@dataclass(slots=True)
class _TimeframeState:
    rsi: RSIAccumulator
    bollinger: BollingerAccumulator
    atr: ATRAccumulator
    # Rolling close window — used only by the coarse `regime_trend`
    # classifier (first-vs-last 5-bar block).
    closes_window: deque[float] = field(default_factory=lambda: deque(maxlen=20))
    last_close: float | None = None


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
            )
            for tf in timeframes
        }
        self._bars_1h_closes_usd: deque[float] = deque(maxlen=MOVE_24H_WINDOW_BARS)

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

    def bollinger_pct_b(self, tf: str) -> float | None:
        """Percent-B against the latest close on this TF. `None` until both
        warmups (window-full + at-least-one-close) are satisfied."""
        state = self._states.get(tf)
        if state is None or state.last_close is None:
            return None
        return state.bollinger.pct_b(state.last_close)

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
