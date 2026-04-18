"""Feature engine: rolling Bollinger, ATR, regime classification.

Keeps deque-backed windows plus running sum & sum-of-squares so that `sma`
and `stddev` are O(1) per query rather than O(window). This matters when
the spot feed (Coinbase) bursts hundreds of ticks in a subsecond window and
we compute features on every tick. Numba lands for ATR later if needed.
"""

from __future__ import annotations

import math
from collections import deque

from bot_btc_1hr_kalshi.obs.schemas import RegimeTrend, RegimeVol


class FeatureEngine:
    """Incrementally maintains rolling features from a stream of spot prices."""

    __slots__ = (
        "_atr_hi_threshold",
        "_atr_lo_threshold",
        "_atr_period",
        "_bb_period",
        "_bb_std",
        "_last_price",
        "_price_sum",
        "_price_sum_sq",
        "_prices",
        "_true_ranges",
    )

    def __init__(
        self,
        *,
        bollinger_period: int,
        bollinger_std_mult: float,
        atr_period: int = 14,
        atr_high_threshold_usd: float = 150.0,
        atr_low_threshold_usd: float = 30.0,
    ) -> None:
        if bollinger_period < 2:
            raise ValueError("bollinger_period must be >= 2")
        if bollinger_std_mult <= 0:
            raise ValueError("bollinger_std_mult must be > 0")
        if atr_period < 2:
            raise ValueError("atr_period must be >= 2")
        self._bb_period = bollinger_period
        self._bb_std = bollinger_std_mult
        self._atr_period = atr_period
        self._atr_hi_threshold = atr_high_threshold_usd
        self._atr_lo_threshold = atr_low_threshold_usd
        self._prices: deque[float] = deque(maxlen=bollinger_period)
        self._true_ranges: deque[float] = deque(maxlen=atr_period)
        self._last_price: float | None = None
        # Running sums for O(1) Bollinger stats. Catastrophic cancellation at
        # BTC prices ~60_000 with a ~400-period window gives variance error
        # on the order of 1e-3 USD² — far below any threshold we act on.
        self._price_sum: float = 0.0
        self._price_sum_sq: float = 0.0

    def update_spot(self, price_usd: float) -> None:
        if price_usd <= 0:
            raise ValueError("price_usd must be > 0")
        if self._last_price is not None:
            self._true_ranges.append(abs(price_usd - self._last_price))
        if len(self._prices) == self._bb_period:
            # Window full — evict oldest before insert so sums stay in sync.
            old = self._prices[0]
            self._price_sum -= old
            self._price_sum_sq -= old * old
        self._prices.append(price_usd)
        self._price_sum += price_usd
        self._price_sum_sq += price_usd * price_usd
        self._last_price = price_usd

    # ---- derived values ----

    @property
    def last_price(self) -> float | None:
        return self._last_price

    def sma(self) -> float | None:
        if len(self._prices) < self._bb_period:
            return None
        return self._price_sum / self._bb_period

    def stddev(self) -> float | None:
        n = len(self._prices)
        if n < self._bb_period:
            return None
        mean = self._price_sum / n
        variance = max(0.0, self._price_sum_sq / n - mean * mean)
        return math.sqrt(variance)

    def bollinger_bands(self) -> tuple[float, float, float] | None:
        """Returns (lower, mid, upper). None until the SMA window is full."""
        mean = self.sma()
        std = self.stddev()
        if mean is None or std is None:
            return None
        k = self._bb_std * std
        return (mean - k, mean, mean + k)

    def bollinger_pct_b(self) -> float | None:
        """Percent-B: (price - lower) / (upper - lower). 0.5 = mid-band.

        < 0 means price is below the lower band (possible mean-reversion floor).
        > 1 means price is above the upper band (possible mean-reversion ceiling).
        """
        bands = self.bollinger_bands()
        if bands is None or self._last_price is None:
            return None
        lower, _mid, upper = bands
        if upper == lower:
            return 0.5
        return (self._last_price - lower) / (upper - lower)

    def atr(self) -> float | None:
        if len(self._true_ranges) < self._atr_period:
            return None
        return sum(self._true_ranges) / len(self._true_ranges)

    def regime_vol(self) -> RegimeVol:
        atr = self.atr()
        if atr is None:
            return "normal"
        if atr >= self._atr_hi_threshold:
            return "high"
        if atr <= self._atr_lo_threshold:
            return "low"
        return "normal"

    def regime_trend(self, *, flat_threshold_usd: float = 25.0) -> RegimeTrend:
        """Coarse trend: compare first vs last 5-bar block of the window."""
        n = len(self._prices)
        if n < 5:
            return "flat"
        prices = list(self._prices)
        block = min(5, n // 2)
        first = sum(prices[:block]) / block
        last = sum(prices[-block:]) / block
        delta = last - first
        if abs(delta) < flat_threshold_usd:
            return "flat"
        return "up" if delta > 0 else "down"
