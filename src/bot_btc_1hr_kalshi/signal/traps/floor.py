"""Floor-reversion trap.

Fires when:
  1. Kalshi YES best ask is "cheap" (<= FLOOR_MAX_CENTS).
  2. BTC spot is below its lower Bollinger band (pct_b < 0).
  3. Regime is not "high vol" — mean reversion degrades in vol spikes.
  4. 1H RSI is NOT committed bearish (>= `htf_bearish_veto_rsi`). Going
     long against a declared macro downtrend is fighting the tape — the
     HTF veto lives inside the trap so rejected candidates never hit the
     decision journal (DESIGN.md §6.3, Slice 8).
  5. Rolling-5m CVD is NOT deeply negative (Slice 9). A dip with persistent
     net aggressor selling is a cascade, not a dip to fade — "falling
     knife" veto. Lives inside the trap for the same reason as HTF.
  6. Confidence (|pct_b|, weighted by 5m RSI alignment) clears the floor.
  7. Microstructure (PR-C — shadow): if recent long-side liquidation
     notional below spot exceeds `liquidation_cascade_threshold_usd`, a
     long here is buying into a downside cascade. Aggregated BTC futures
     OI below `oi_compression_threshold_usd` is a conviction-drained tape
     and vetoes both sides. When `enable_microstructure_gating=True`
     these become hard vetoes; when False (default) they tag
     `features.shadow_veto_reason` so the tuning loop can derive
     calibrated thresholds against paper-soak outcomes BEFORE the risk
     committee promotes the gate. No behavior change until a threshold
     is signed off.

Side = YES: we're betting the spot will revert upward, making YES more valuable.

Warmup (rsi_1h / rsi_5m / cvd_1m_usd == None): HTF veto, RSI weighting, and
CVD veto all fail-open — matches pre-Slice-8/9 behavior while accumulators
fill on cold start (1H RSI needs ~14 hours of 1h closes). Microstructure
checks likewise fail-open when the feeds are absent (DerivativesOracle
disabled, cold-start pre-first-event, or transient WS dropout).

Edge: Normal-CDF settlement probability (DESIGN.md §6.2 / signal/edge_model.py)
minus the maker-entry price in cents. A trap with zero edge is dropped by the
sizer, so we return the signal only when the model says we have >0 cents edge.
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

FLOOR_MAX_CENTS = 40

# 5m RSI weight anchors (Slice 8). Deep oversold = full weight; neutral
# (RSI 50) = 0.5 floor. Linear interp in between. Acts as a soft gate —
# the trap still fires on strong pct_b alone, but RSI confirmation
# amplifies its passage through min_confidence.
_RSI_5M_OVERSOLD = 35.0
_RSI_5M_NEUTRAL = 50.0
_RSI_WEIGHT_FLOOR = 0.5


def _floor_rsi_weight(rsi_5m: float | None) -> float:
    if rsi_5m is None:
        return 1.0
    if rsi_5m <= _RSI_5M_OVERSOLD:
        return 1.0
    if rsi_5m >= _RSI_5M_NEUTRAL:
        return _RSI_WEIGHT_FLOOR
    # Linear interp between (OVERSOLD, 1.0) and (NEUTRAL, FLOOR).
    span = _RSI_5M_NEUTRAL - _RSI_5M_OVERSOLD
    decay = (rsi_5m - _RSI_5M_OVERSOLD) / span
    return 1.0 - decay * (1.0 - _RSI_WEIGHT_FLOOR)


def _floor_microstructure_veto(
    snap: MarketSnapshot,
    *,
    liquidation_cascade_threshold_usd: float,
    oi_compression_threshold_usd: float,
) -> str | None:
    """Return a veto reason for the floor (long) setup or None.

    Adverse direction for a long: large recent long-side liquidations
    *below* spot — a downside cascade we'd be buying into. OI
    compression is directionless (conviction drain) so it vetoes long
    and short alike.
    """
    pressure = snap.liquidation_pressure
    if (
        pressure is not None
        and liquidation_cascade_threshold_usd > 0.0
        and pressure.long_usd_below_spot >= liquidation_cascade_threshold_usd
    ):
        return "liquidation_cascade_below"

    oi = snap.open_interest
    if (
        oi is not None
        and oi_compression_threshold_usd > 0.0
        and oi.total_oi_usd < oi_compression_threshold_usd
    ):
        return "oi_compression"

    return None


def detect_floor_reversion(
    snap: MarketSnapshot,
    *,
    min_confidence: float,
    htf_bearish_veto_rsi: float = 45.0,
    cvd_1m_veto_threshold_usd: float = 5_000_000.0,
    enable_microstructure_gating: bool = False,
    liquidation_cascade_threshold_usd: float = 0.0,
    oi_compression_threshold_usd: float = 0.0,
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

    # HTF alignment veto — fail-open during warmup.
    rsi_1h = snap.features.rsi_1h
    if rsi_1h is not None and rsi_1h < htf_bearish_veto_rsi:
        return None

    # Tape Reader veto (Slice 9) — persistent aggressor selling over the
    # rolling 5m window means the dip is a cascade, not a reversion. Fail-
    # open on warmup (cvd None).
    cvd = snap.features.cvd_1m_usd
    if cvd is not None and cvd <= -cvd_1m_veto_threshold_usd:
        return None

    confidence = min(1.0, abs(pct_b)) * _floor_rsi_weight(snap.features.rsi_5m)
    if confidence < min_confidence:
        return None

    # Microstructure (PR-C) — compute reason first so the decision
    # journal carries the tag even when gating is off. Hard-gating is
    # risk-committee-controlled via SignalSettings; the trap itself
    # never chooses to reject without the config saying so.
    micro_reason = _floor_microstructure_veto(
        snap,
        liquidation_cascade_threshold_usd=liquidation_cascade_threshold_usd,
        oi_compression_threshold_usd=oi_compression_threshold_usd,
    )
    if micro_reason is not None and enable_microstructure_gating:
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

    features = (
        snap.features.model_copy(update={"shadow_veto_reason": micro_reason})
        if micro_reason is not None
        else snap.features
    )

    return TrapSignal(
        trap="floor_reversion",
        side="YES",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge,
        features=features,
    )
