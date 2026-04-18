"""Pure, stateful indicator accumulators (Slice 8, Phase 1).

Each class is a single-timeframe, single-indicator accumulator that takes
one observation at a time via `ingest(...)` and returns the current value
(or `None` during warmup). No I/O, no clock access, no cross-state
dependencies — the FeatureEngine composes these per configured timeframe.

Why a dedicated module (not private classes in FeatureEngine):
  * The traders' indicators are load-bearing for manual-workflow parity
    — they need to read identically to TradingView / any third-party
    chart. Isolating the math in its own module makes that parity
    auditable in one place.
  * Research + backtest harnesses can import the accumulators directly
    without pulling in the full FeatureEngine / trap graph.

The Wilder's RSI trap
---------------------
Most libraries mis-label EMA- or SMA-smoothed RSI as "Wilder's". True
Wilder uses the Running Moving Average (RMA, a.k.a. Modified Moving
Average) with two distinct phases:

  Warmup (first `period` deltas):
      avg = simple running mean of the observed gains / losses.

  Post-warmup (delta `period + 1` onward):
      avg = (prev_avg * (period - 1) + current) / period

This is mathematically different from an EMA with alpha = 1/period
(the EMA seeds on the first observation and applies smoothing from the
very first step). The RMA's seed-then-switch behaviour produces the RSI
values TradingView displays — any drift from that formula would show up
as the user second-guessing the bot's read against their chart.

Bollinger + ATR notes
---------------------
* Bollinger uses population stddev (not sample), matching TradingView.
  O(1) per ingest via running sum + sum-of-squares, with a `max(var, 0)`
  guard so float noise on a constant-price window doesn't produce a
  tiny negative variance.
* ATR uses Wilder's RMA on bar TRs. The first bar only seeds
  `prev_close` — no TR sample is generated, so `period` TRs need
  `period + 1` bars.
"""

from __future__ import annotations

import math
from collections import deque

__all__ = ["ATRAccumulator", "BollingerAccumulator", "RSIAccumulator"]


class RSIAccumulator:
    """Wilder's RSI(period) on bar closes, using RMA smoothing.

    Two bar closes are required to produce the first delta, so `period`
    deltas need `period + 1` ingests. `value` / `ingest` return `None`
    until that threshold is crossed.

    Edge cases:
      * `avg_loss == 0.0` → RSI is defined as 100.0 (no downside moves).
      * `avg_gain == 0.0 and avg_loss > 0` → RS = 0 → RSI = 0.0.
    """

    __slots__ = ("_avg_gain", "_avg_loss", "_deltas_seen", "_period", "_prev_close")

    def __init__(self, period: int = 14) -> None:
        if period < 2:
            raise ValueError("period must be >= 2")
        self._period = period
        self._prev_close: float | None = None
        self._avg_gain: float = 0.0
        self._avg_loss: float = 0.0
        self._deltas_seen: int = 0

    def ingest(self, close: float) -> float | None:
        if self._prev_close is None:
            self._prev_close = close
            return None
        delta = close - self._prev_close
        gain = delta if delta > 0 else 0.0
        loss = -delta if delta < 0 else 0.0
        self._deltas_seen += 1
        if self._deltas_seen <= self._period:
            n = self._deltas_seen
            self._avg_gain = (self._avg_gain * (n - 1) + gain) / n
            self._avg_loss = (self._avg_loss * (n - 1) + loss) / n
        else:
            p = self._period
            self._avg_gain = (self._avg_gain * (p - 1) + gain) / p
            self._avg_loss = (self._avg_loss * (p - 1) + loss) / p
        self._prev_close = close
        return self.value

    @property
    def value(self) -> float | None:
        if self._deltas_seen < self._period:
            return None
        if self._avg_loss == 0.0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))


class BollingerAccumulator:
    """Rolling SMA + population-stddev bands on bar closes.

    Emits `None` from `ingest` / `bands` until the window is full. O(1)
    per ingest via running sum + running sum-of-squares.
    """

    __slots__ = ("_closes", "_period", "_std_mult", "_sum", "_sum_sq")

    def __init__(self, period: int = 20, std_mult: float = 2.0) -> None:
        if period < 2:
            raise ValueError("period must be >= 2")
        if std_mult <= 0.0:
            raise ValueError("std_mult must be > 0")
        self._period = period
        self._std_mult = std_mult
        self._closes: deque[float] = deque(maxlen=period)
        self._sum: float = 0.0
        self._sum_sq: float = 0.0

    def ingest(self, close: float) -> tuple[float, float, float] | None:
        if len(self._closes) == self._period:
            evicted = self._closes[0]
            self._sum -= evicted
            self._sum_sq -= evicted * evicted
        self._closes.append(close)
        self._sum += close
        self._sum_sq += close * close
        return self.bands

    @property
    def bands(self) -> tuple[float, float, float] | None:
        """`(lower, mid, upper)` or `None` during warmup."""
        if len(self._closes) < self._period:
            return None
        mean = self._sum / self._period
        var = (self._sum_sq / self._period) - (mean * mean)
        # Float noise on a constant-price window can push variance
        # slightly negative; clamp to zero before sqrt.
        std = math.sqrt(max(var, 0.0))
        width = self._std_mult * std
        return (mean - width, mean, mean + width)

    def pct_b(self, close: float) -> float | None:
        """Percent-B: `(close - lower) / (upper - lower)`.

        0.5 → mid-band; <0 → below lower band; >1 → above upper band.
        On a zero-width band (constant window) returns 0.5 by convention.
        """
        b = self.bands
        if b is None:
            return None
        lower, _mid, upper = b
        if upper == lower:
            return 0.5
        return (close - lower) / (upper - lower)


class ATRAccumulator:
    """Wilder's ATR(period) on bar OHLC data.

    True Range: `max(high - low, |high - prev_close|, |low - prev_close|)`.
    The first bar only seeds `prev_close` — no TR sample emerges — so
    `period` TR samples require `period + 1` ingests.
    """

    __slots__ = ("_atr", "_period", "_prev_close", "_samples_seen")

    def __init__(self, period: int = 14) -> None:
        if period < 2:
            raise ValueError("period must be >= 2")
        self._period = period
        self._prev_close: float | None = None
        self._atr: float = 0.0
        self._samples_seen: int = 0

    def ingest(self, high: float, low: float, close: float) -> float | None:
        if self._prev_close is None:
            self._prev_close = close
            return None
        tr = max(
            high - low,
            abs(high - self._prev_close),
            abs(low - self._prev_close),
        )
        self._samples_seen += 1
        if self._samples_seen <= self._period:
            n = self._samples_seen
            self._atr = (self._atr * (n - 1) + tr) / n
        else:
            p = self._period
            self._atr = (self._atr * (p - 1) + tr) / p
        self._prev_close = close
        return self.value

    @property
    def value(self) -> float | None:
        if self._samples_seen < self._period:
            return None
        return self._atr
