"""Cross-venue lag trap.

Triggers on a very extreme spot move (|pct_b| >= LAG_PCT_B_MIN) while the
Kalshi market still sits in a neutral price zone (neither side priced as
"cheap" — i.e. the ask is in [NEUTRAL_MIN, NEUTRAL_MAX]).

Intuition: Kalshi hourly is thin in the last few minutes and price discovery
can lag the underlying spot. A sharp BTC move past the Bollinger band that
hasn't yet been absorbed into the Kalshi quote is a bet on convergence.

Direction:
  pct_b >> +1: spot rallied → YES should rise → buy YES at YES best bid.
  pct_b << -1: spot crashed → YES should fall → buy NO at NO best bid.

Maker-only entry on both sides (hard rule #1).
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.schemas import Side
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

LAG_PCT_B_MIN = 1.5
NEUTRAL_MIN_CENTS = 40
NEUTRAL_MAX_CENTS = 60
FAIR_VALUE_MID_CENTS = 50.0


def detect_cross_venue_lag(
    snap: MarketSnapshot,
    *,
    min_confidence: float,
) -> TrapSignal | None:
    if not snap.book.valid:
        return None

    pct_b = snap.features.bollinger_pct_b
    if abs(pct_b) < LAG_PCT_B_MIN:
        return None
    if snap.features.regime_vol == "high":
        return None

    side: Side = "YES" if pct_b > 0 else "NO"
    bid = snap.book.best_bid_for(side)
    ask = snap.book.best_ask_for(side)

    if bid is None or ask is None:
        return None
    if not (NEUTRAL_MIN_CENTS <= ask.price_cents <= NEUTRAL_MAX_CENTS):
        return None

    confidence = min(1.0, abs(pct_b) / 2.0)
    if confidence < min_confidence:
        return None

    entry_price_cents = bid.price_cents
    edge_cents = confidence * max(0.0, FAIR_VALUE_MID_CENTS - entry_price_cents)

    return TrapSignal(
        trap="cross_venue_lag",
        side=side,
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge_cents,
        features=snap.features,
    )
