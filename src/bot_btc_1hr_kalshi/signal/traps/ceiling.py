"""Ceiling-reversion trap — mirror of floor.

Fires when:
  1. NO best ask is "cheap" (YES best bid is >= 100 - CEILING_MAX_NO_ASK_CENTS,
     i.e. YES is richly priced and therefore NO is cheap to buy).
  2. BTC spot is above its upper Bollinger band (pct_b > 0).
  3. Regime is not "high vol" — mean reversion degrades in vol spikes.
  4. Confidence (|pct_b|) clears the floor.

Side = NO: we bet spot reverts downward -> YES becomes less valuable
-> NO pays off. Entry is a maker BUY on the NO side at NO best bid
(= 100 - YES best ask by parity), honoring hard rule #1.

Edge is the Normal-CDF settlement probability for NO minus the NO entry
price in cents (see signal/edge_model.py).
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

NO_ASK_MAX_CENTS = 40


def detect_ceiling_reversion(
    snap: MarketSnapshot,
    *,
    min_confidence: float,
) -> TrapSignal | None:
    if not snap.book.valid:
        return None

    no_bid = snap.book.best_bid_for("NO")
    no_ask = snap.book.best_ask_for("NO")
    if no_bid is None or no_ask is None or no_ask.price_cents > NO_ASK_MAX_CENTS:
        return None

    pct_b = snap.features.bollinger_pct_b
    if pct_b <= 0.0:
        return None

    if snap.features.regime_vol == "high":
        return None

    confidence = min(1.0, abs(pct_b))
    if confidence < min_confidence:
        return None

    entry_price_cents = no_bid.price_cents
    q_yes = settlement_prob_yes(
        spot_usd=snap.spot_btc_usd,
        strike_usd=snap.strike_usd,
        sigma_per_minute_usd=snap.features.atr_cents,
        minutes_to_settlement=snap.minutes_to_settlement,
    )
    edge = edge_cents(side="NO", entry_price_cents=entry_price_cents, q_yes=q_yes)
    if edge <= 0:
        return None

    return TrapSignal(
        trap="ceiling_reversion",
        side="NO",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge,
        features=snap.features,
    )
