"""Settlement-probability edge model (DESIGN.md §6.2).

Kalshi hourly BTC markets settle YES iff spot is above the strike at the top
of the hour. Under a short-horizon normal approximation:

    P(YES) = Phi( (spot - strike) / sigma_total )

where `sigma_total = sigma_per_minute * sqrt(minutes_remaining)` is the
diffusion scale of the remaining window.

We use the ATR as a pragmatic proxy for sigma_per_minute: the feature engine
computes ATR as the mean absolute one-tick spot change, which closely tracks
a MAD-style volatility estimate at the tick horizon. Interpreting it as a
per-minute stdev is coarse but unit-consistent given the upstream spot-feed
cadence, and is replaced by an explicit realized-vol estimate when the
feature pipeline gains a dedicated sigma estimator.

Given the implied `q_yes`, our edge in cents for a side-specific maker BUY
at `entry_price_cents` is `(q_side * 100) - entry_price_cents`, clipped at
zero (a trap with no edge must not pass sizing).
"""

from __future__ import annotations

import math

from bot_btc_1hr_kalshi.obs.schemas import Side

_MIN_SIGMA_TOTAL = 1e-6
_Q_CLIP_LO = 0.001
_Q_CLIP_HI = 0.999


def settlement_prob_yes(
    *,
    spot_usd: float,
    strike_usd: float,
    sigma_per_minute_usd: float,
    minutes_to_settlement: float,
) -> float:
    """P(spot settles above strike) under a Brownian approximation.

    Returns a probability clipped to [0.001, 0.999]; never 0 or 1 exactly so
    downstream Kelly sizing never divides by zero.
    """
    if sigma_per_minute_usd <= 0 or minutes_to_settlement <= 0:
        # No diffusion left: collapse to a deterministic outcome. Clip so the
        # trap layer still yields a finite (but near-extreme) probability.
        return _Q_CLIP_HI if spot_usd >= strike_usd else _Q_CLIP_LO

    sigma_total = sigma_per_minute_usd * math.sqrt(minutes_to_settlement)
    if sigma_total < _MIN_SIGMA_TOTAL:
        return _Q_CLIP_HI if spot_usd >= strike_usd else _Q_CLIP_LO

    z = (spot_usd - strike_usd) / sigma_total
    q = _normal_cdf(z)
    return min(max(q, _Q_CLIP_LO), _Q_CLIP_HI)


def edge_cents(
    *,
    side: Side,
    entry_price_cents: int,
    q_yes: float,
) -> float:
    """Expected edge in cents for a maker BUY on the given side at `entry_price_cents`."""
    q_side = q_yes if side == "YES" else 1.0 - q_yes
    raw = (q_side * 100.0) - float(entry_price_cents)
    return max(0.0, raw)


def _normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
