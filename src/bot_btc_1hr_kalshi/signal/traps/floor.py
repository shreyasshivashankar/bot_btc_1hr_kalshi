"""Floor-reversion trap.

Fires when:
  1. Kalshi YES best ask is "cheap" (<= FLOOR_MAX_CENTS).
  2. BTC spot is below its lower Bollinger band (pct_b < 0).
  3. Regime is not "high vol" — mean reversion degrades in vol spikes.
  4. Confidence (magnitude of band deviation) clears the configured floor.

Side = YES: we're betting the spot will revert upward, making YES more valuable.

Edge: Normal-CDF settlement probability (DESIGN.md §6.2 / signal/edge_model.py)
minus the maker-entry price in cents. A trap with zero edge is dropped by the
sizer, so we return the signal only when the model says we have >0 cents edge.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

FLOOR_MAX_CENTS = 40


def detect_floor_reversion(
    snap: MarketSnapshot,
    *,
    min_confidence: float,
) -> TrapSignal | None:
    if not snap.book.valid:
        return None

    best_bid = snap.book.best_bid
    best_ask = snap.book.best_ask
    if best_bid is None or best_ask is None or best_ask.price_cents > FLOOR_MAX_CENTS:
        return None

    pct_b = snap.features.bollinger_pct_b
    if pct_b >= 0.0:
        return None

    if snap.features.regime_vol == "high":
        return None

    confidence = min(1.0, abs(pct_b))
    if confidence < min_confidence:
        return None

    # Hard rule #1: maker BUY at best bid.
    entry_price_cents = best_bid.price_cents
    q_yes = settlement_prob_yes(
        spot_usd=snap.spot_btc_usd,
        strike_usd=snap.strike_usd,
        sigma_per_minute_usd=snap.features.atr_cents,
        minutes_to_settlement=snap.minutes_to_settlement,
    )
    edge = edge_cents(side="YES", entry_price_cents=entry_price_cents, q_yes=q_yes)
    if edge <= 0:
        return None

    return TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge,
        features=snap.features,
    )
