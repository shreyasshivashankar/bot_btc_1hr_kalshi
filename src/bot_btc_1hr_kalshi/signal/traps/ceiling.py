"""Ceiling-reversion trap — mirror of floor.

Fires when:
  1. NO best ask is "cheap" (YES best bid is >= 100 - CEILING_MAX_NO_ASK_CENTS,
     i.e. YES is richly priced and therefore NO is cheap to buy).
  2. BTC spot is above its upper Bollinger band (pct_b > 0).
  3. Regime is not "high vol" — mean reversion degrades in vol spikes.
  4. 1H RSI is NOT committed bullish (<= `htf_bullish_veto_rsi`). Shorting
     against a declared macro uptrend is fighting the tape — the HTF
     veto lives inside the trap so rejected candidates never hit the
     decision journal (DESIGN.md §6.3, Slice 8).
  5. |24h move| < `runaway_train_halt_pct`. Parabolic rallies or
     capitulation slides have no mean-reversion edge; skip the trap
     entirely rather than fade them (DESIGN.md §6.3). Applied only to
     the ceiling trap — the floor trap sets its own bar via pct_b and
     the high-vol regime gate.
  6. Rolling-5m CVD is NOT deeply positive (Slice 9). A pump with
     persistent net aggressor buying is a breakout, not a rip to fade
     — mirror of the floor's falling-knife veto.
  7. Confidence (|pct_b|, weighted by 5m RSI alignment) clears the floor.
  8. Microstructure (PR-C — shadow): mirror of the floor's microstructure
     check with adverse direction inverted — for a short setup the
     adverse cascade is large recent short-side liquidations *above*
     spot (upside squeeze in progress). OI compression applies
     symmetrically. When `enable_microstructure_gating=True` the veto is
     hard; when False (default) it only tags `features.shadow_veto_reason`
     so the tuning loop can derive calibrated thresholds from paper-
     soak outcomes before the risk committee promotes the gate.

Side = NO: we bet spot reverts downward -> YES becomes less valuable
-> NO pays off. Entry is a maker BUY on the NO side at NO best bid
(= 100 - YES best ask by parity), honoring hard rule #1.

Warmup (rsi_1h / rsi_5m / move_24h_pct / cvd_1m_usd == None): all
Slice-8/9 gates fail-open — matches pre-slice behavior while
accumulators fill. Microstructure checks likewise fail-open when the
DerivativesOracle feeds are absent.

Edge is the Normal-CDF settlement probability for NO minus the NO entry
price in cents (see signal/edge_model.py).
"""

from __future__ import annotations

from bot_btc_1hr_kalshi.signal.edge_model import edge_cents, settlement_prob_yes
from bot_btc_1hr_kalshi.signal.types import MarketSnapshot, TrapSignal

NO_ASK_MAX_CENTS = 40

_RSI_5M_OVERBOUGHT = 65.0
_RSI_5M_NEUTRAL = 50.0
_RSI_WEIGHT_FLOOR = 0.5


def _ceiling_rsi_weight(rsi_5m: float | None) -> float:
    if rsi_5m is None:
        return 1.0
    if rsi_5m >= _RSI_5M_OVERBOUGHT:
        return 1.0
    if rsi_5m <= _RSI_5M_NEUTRAL:
        return _RSI_WEIGHT_FLOOR
    span = _RSI_5M_OVERBOUGHT - _RSI_5M_NEUTRAL
    decay = (_RSI_5M_OVERBOUGHT - rsi_5m) / span
    return 1.0 - decay * (1.0 - _RSI_WEIGHT_FLOOR)


def _ceiling_microstructure_veto(
    snap: MarketSnapshot,
    *,
    liquidation_cascade_threshold_usd: float,
    oi_compression_threshold_usd: float,
) -> str | None:
    """Return a veto reason for the ceiling (short) setup or None.

    Adverse direction for a short: large recent short-side liquidations
    *above* spot — an upside squeeze cascade we'd be shorting into. OI
    compression is directionless so it vetoes long and short alike.
    """
    pressure = snap.liquidation_pressure
    if (
        pressure is not None
        and liquidation_cascade_threshold_usd > 0.0
        and pressure.short_usd_above_spot >= liquidation_cascade_threshold_usd
    ):
        return "liquidation_cascade_above"

    oi = snap.open_interest
    if (
        oi is not None
        and oi_compression_threshold_usd > 0.0
        and oi.total_oi_usd < oi_compression_threshold_usd
    ):
        return "oi_compression"

    return None


def detect_ceiling_reversion(
    snap: MarketSnapshot,
    *,
    min_confidence: float,
    htf_bullish_veto_rsi: float = 55.0,
    runaway_train_halt_pct: float = 0.05,
    cvd_1m_veto_threshold_usd: float = 5_000_000.0,
    enable_microstructure_gating: bool = False,
    liquidation_cascade_threshold_usd: float = 0.0,
    oi_compression_threshold_usd: float = 0.0,
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

    rsi_1h = snap.features.rsi_1h
    if rsi_1h is not None and rsi_1h > htf_bullish_veto_rsi:
        return None

    # Runaway Train — magnitude-based so both parabolic rallies and
    # capitulation drops are excluded.
    move_24h = snap.features.move_24h_pct
    if move_24h is not None and abs(move_24h) >= runaway_train_halt_pct:
        return None

    # Tape Reader veto (Slice 9) — persistent aggressor buying over the
    # rolling 5m window means the rip is a breakout, not a reversion.
    # Fail-open on warmup (cvd None).
    cvd = snap.features.cvd_1m_usd
    if cvd is not None and cvd >= cvd_1m_veto_threshold_usd:
        return None

    confidence = min(1.0, abs(pct_b)) * _ceiling_rsi_weight(snap.features.rsi_5m)
    if confidence < min_confidence:
        return None

    # Microstructure (PR-C) — compute first so the tag lands on the
    # journal even when gating is off; hard-reject only when the risk
    # committee has flipped the flag.
    micro_reason = _ceiling_microstructure_veto(
        snap,
        liquidation_cascade_threshold_usd=liquidation_cascade_threshold_usd,
        oi_compression_threshold_usd=oi_compression_threshold_usd,
    )
    if micro_reason is not None and enable_microstructure_gating:
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

    features = (
        snap.features.model_copy(update={"shadow_veto_reason": micro_reason})
        if micro_reason is not None
        else snap.features
    )

    return TrapSignal(
        trap="ceiling_reversion",
        side="NO",
        entry_price_cents=entry_price_cents,
        confidence=confidence,
        edge_cents=edge,
        features=features,
    )
