"""Implied-basis arb trap (DESIGN.md §6.4, Slice 10).

The other traps fire on the underlying's geometry — pct_b against a
Bollinger envelope, or a cross-venue lag. This trap fires on a *pricing*
dislocation: when Kalshi's quoted YES/NO on the current hourly strike is
meaningfully offset from the Normal-CDF fair value implied by spot, ATR,
and minutes-to-settlement (signal/edge_model.py — the same fair-value
engine used elsewhere for edge sizing).

Gates:
  1. Book must be valid and have both a best bid and best ask (hard rule
     #9 + maker-only entry requires a resting bid to post against).
  2. Dead-spot veto: if the rolling 60s primary-spot range exceeds
     `dead_spot_range_usd`, the underlying is actively sweeping and the
     "edge" is adverse selection against fresher Coinbase prints. We also
     fail-closed on an unknown range (cold-start window) — without a
     measurement we cannot prove the tape is quiet.
  3. Mispricing magnitude: either fair_value - YES_ask >= threshold
     (cheap YES, buy YES) or YES_bid - fair_value >= threshold (expensive
     YES, buy NO at NO best bid via parity). Threshold is configured on
     `SignalSettings.arb_basis_threshold_cents` (default 15c).

Sizing/entry:
  * Maker-only (hard rule #1): post at the chosen side's best bid. We do
    NOT cross on entry even when the apparent basis would make a taker
    fill immediately profitable — hard rule is not relaxed for arb.
  * Edge: standard `edge_cents(side, entry_price_cents, q_yes)`. A trap
    with <=0 edge at the post-price is dropped so the sizer can't take a
    negative-edge bet even on a large quoted basis (thin-liquidity traps).

Confidence scales linearly from 0.5 at `threshold_cents` to 1.0 at
`threshold_cents + 10`. That clamps narrow-basis setups to the weakest
tier while letting a loud dislocation compete on edge*confidence against
the other traps in the cross-strike registry. The +10 reference point is
deliberate — the paper-soak diagnostic showed Kalshi quotes swinging
11-99c on a single strike within an hour, so a 25c basis is common
enough that 1.0 confidence shouldn't require a 40c setup.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.obs.schemas import Side
from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

_CONFIDENCE_FLOOR = 0.5
_CONFIDENCE_FULL_AT_EXCESS_CENTS = 10.0


def _confidence_for_basis(basis_cents: float, threshold_cents: int) -> float:
    excess = basis_cents - float(threshold_cents)
    if excess <= 0.0:
        return _CONFIDENCE_FLOOR
    if excess >= _CONFIDENCE_FULL_AT_EXCESS_CENTS:
        return 1.0
    return _CONFIDENCE_FLOOR + (1.0 - _CONFIDENCE_FLOOR) * (
        excess / _CONFIDENCE_FULL_AT_EXCESS_CENTS
    )


def detect_implied_arb(
    snap: MarketSnapshot,
    *,
    basis_threshold_cents: int,
    dead_spot_range_usd: float,
) -> TrapSignal | None:
    if not snap.book.valid:
        return None

    yes_bid = snap.book.best_bid
    yes_ask = snap.book.best_ask
    if yes_bid is None or yes_ask is None:
        return None

    # Dead-spot veto — fail-closed on unknown (cold start) as well. A
    # sweeping underlying invalidates the Normal-CDF fair value against
    # the inbound Kalshi quote; we'd be trading a stale model.
    spot_range = snap.features.spot_range_60s
    if spot_range is None or spot_range > dead_spot_range_usd:
        return None

    q_yes = settlement_prob_yes(
        spot_usd=snap.spot_btc_usd,
        strike_usd=snap.strike_usd,
        sigma_per_minute_usd=snap.features.atr_cents,
        minutes_to_settlement=snap.minutes_to_settlement,
    )
    fair_value_cents = q_yes * 100.0

    yes_ask_basis = fair_value_cents - float(yes_ask.price_cents)
    yes_bid_basis = float(yes_bid.price_cents) - fair_value_cents

    side: Side
    if yes_ask_basis >= float(basis_threshold_cents):
        side = "YES"
        basis_cents = yes_ask_basis
    elif yes_bid_basis >= float(basis_threshold_cents):
        side = "NO"
        basis_cents = yes_bid_basis
    else:
        return None

    # Hard rule #1: maker BUY at the chosen side's best bid.
    entry_level = snap.book.best_bid_for(side)
    if entry_level is None:
        return None
    entry_price_cents = entry_level.price_cents

    edge = edge_cents(side=side, entry_price_cents=entry_price_cents, q_yes=q_yes)
    if edge <= 0:
        return None

    confidence = _confidence_for_basis(basis_cents, basis_threshold_cents)

    return TrapSignal(
        trap="implied_basis_arb",
        side=side,
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge,
        features=snap.features,
    )
