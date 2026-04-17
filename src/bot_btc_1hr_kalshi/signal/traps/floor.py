"""Floor-reversion trap.

Fires when:
  1. Kalshi YES best ask is "cheap" (<= FLOOR_MAX_CENTS).
  2. BTC spot is below its lower Bollinger band (pct_b < 0).
  3. Regime is not "high vol" — mean reversion degrades in vol spikes.
  4. Confidence (magnitude of band deviation) clears the configured floor.

Side = YES: we're betting the spot will revert upward, making YES more valuable.

Notes
-----
* `book.valid` must be True (DESIGN.md §4.2.1 — features are INVALID on seq gap).
* Edge estimate here is intentionally simple — the real edge model in DESIGN.md
  §6.2 uses a Normal CDF on (strike - spot)/(sigma*sqrt(minutes)). Slice 2.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

FLOOR_MAX_CENTS = 40
FAIR_VALUE_MID_CENTS = 50.0


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

    # Hard rule #1: never cross on entry. We post a maker BUY at the best bid;
    # edge reflects distance from fair value at that bid.
    entry_price_cents = best_bid.price_cents
    edge_cents = confidence * max(0.0, FAIR_VALUE_MID_CENTS - entry_price_cents)

    return TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge_cents,
        features=snap.features,
    )
