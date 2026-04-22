"""Pydantic schemas for the records that leave the process boundary.

Every DecisionRecord and BetOutcome emitted is validated here before it is
logged/serialized. Schema drift vs the BigQuery table breaks tuning queries
(hard rule #6 / CLAUDE.md "non-negotiable invariants").
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ---- Enums ------------------------------------------------------------------

Side = Literal["YES", "NO"]
RegimeTrend = Literal["up", "down", "flat"]
RegimeVol = Literal["high", "normal", "low"]
TrapName = Literal[
    "floor_reversion",
    "ceiling_reversion",
    "cross_venue_lag",
    "implied_basis_arb",
]
ExitReason = Literal[
    "settled",
    "early_cashout_99",
    "soft_stop",
    "theta_net_target",
    "abandoned_to_settlement",
    "tier1_flatten",
    "arb_basis_closed",
]

# ---- Records ----------------------------------------------------------------


class Features(BaseModel):
    """Frozen snapshot of features at entry. Referenced from BetOutcome."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    regime_trend: RegimeTrend
    regime_vol: RegimeVol
    signal_confidence: float = Field(ge=0.0, le=1.0)
    bollinger_pct_b: float
    atr_cents: float = Field(ge=0.0)
    book_depth_at_entry: float = Field(ge=0.0)
    spread_cents: int = Field(ge=0)
    spot_btc_usd: float = Field(gt=0.0)
    minutes_to_settlement: float = Field(ge=0.0)
    # HTF-alignment fields (Slice 8). `None` during warmup — Wilder's RSI
    # needs `period` bars of history on each TF (~14h on 1h), and the 24h
    # move needs 25 1h closes. Persisted NULLABLE in BigQuery so historical
    # rows written before Slice 8 shipped don't break queries.
    rsi_5m: float | None = Field(default=None, ge=0.0, le=100.0)
    rsi_1h: float | None = Field(default=None, ge=0.0, le=100.0)
    move_24h_pct: float | None = None
    # CVD / Tape Reader (Slice 9). Rolling net aggressor-driven USD flow
    # over the last 5 closed 1m bars. `None` during warmup (first 5 minutes
    # after a bar-bus start). NULLABLE in BigQuery for back-compat with
    # pre-Slice-9 rows.
    cvd_1m_usd: float | None = None
    # Implied-basis-arb dead-spot gate (Slice 10). Rolling 60-second spot
    # range in USD — if the underlying is actively sweeping, the Normal-CDF
    # fair value is stale relative to the incoming print and any apparent
    # basis is adverse-selection bait. `None` during warmup on cold start
    # and NULLABLE in BigQuery for back-compat with pre-arb rows.
    spot_range_60s: float | None = Field(default=None, ge=0.0)
    # Aggregated BTC-futures open interest in USD (Slice 11 P2 — shadow).
    # Sourced from the Coinglass poller; `None` when disabled, during the
    # first poll, or on fetch failure. Observational only — the OI signal
    # is logged into `DecisionRecord.features_at_entry` so a future risk-
    # committee decision to gate entries on OI compression can be justified
    # from soaked data. NULLABLE in BigQuery for back-compat with pre-P2
    # rows (i.e., every historical row written before this shipped).
    open_interest_usd: float | None = Field(default=None, ge=0.0)
    # Microstructure shadow-veto tag (Slice 11 P3). When a trap's micro
    # check (heatmap stop-hunt cluster, OI compression band, etc.) would
    # have rejected the entry, the trap stamps the reason here and still
    # emits the signal — `signal.enable_microstructure_gating` controls
    # whether the reason escalates to a hard reject. This field is how
    # the tuning loop / risk committee discover empirical thresholds
    # from paper-soak data before any behavior change reaches live.
    # `None` when no micro veto applies or gating is disabled and the
    # trap chose not to tag. NULLABLE in BigQuery for back-compat.
    shadow_veto_reason: str | None = None


class Sizing(BaseModel):
    """Sizing inputs/output captured on the decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kelly_fraction: float = Field(ge=0.0, le=1.0)
    edge_cents: float
    variance_estimate: float = Field(ge=0.0)
    notional_usd: float = Field(ge=0.0)
    contracts: int = Field(ge=0)


class DecisionRecord(BaseModel):
    """Emitted for every order decision — whether approved or rejected."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    decision_id: str
    ts_ns: int = Field(ge=0)
    market_id: str
    trap: TrapName
    side: Side
    entry_price_cents: int = Field(ge=0, le=100)
    features: Features
    sizing: Sizing
    approved: bool
    reject_reason: str | None = None


class Position(BaseModel):
    """Open-position state. Broker is authoritative (hard rule #7) — this is
    the local projection used for monitor/risk decisions between reconciles."""

    model_config = ConfigDict(extra="forbid")

    position_id: str
    decision_id: str
    market_id: str
    side: Side
    entry_price_cents: int = Field(ge=0, le=100)
    contracts: int = Field(gt=0)
    opened_at_ns: int = Field(ge=0)


class BetOutcome(BaseModel):
    """Emitted once per closed bet to bot_btc_1hr_kalshi.bet_outcomes (BigQuery).

    Frozen — never mutated after emit.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    bet_id: str
    decision_id: str
    market_id: str
    trap: TrapName
    side: Side
    opened_at_ns: int = Field(ge=0)
    closed_at_ns: int = Field(ge=0)
    hold_duration_sec: float = Field(ge=0.0)
    entry_price_cents: int = Field(ge=0, le=100)
    exit_price_cents: int | None
    contracts: int = Field(gt=0)
    gross_pnl_usd: float
    fees_usd: float = Field(ge=0.0)
    net_pnl_usd: float
    counterfactual_held_pnl_usd: float | None
    exit_reason: ExitReason
    features_at_entry: Features
