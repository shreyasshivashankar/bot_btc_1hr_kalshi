"""Typed config model. Mirrors the structure of config/*.yaml."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Mode = Literal["dev", "paper", "shadow", "live"]


class FeedSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ws_url: str | None = None
    ws_url_env: str | None = None
    rest_url: str | None = None
    rest_url_env: str | None = None
    staleness_halt_ms: int = Field(gt=0, default=2000)


class FeedsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kalshi: FeedSettings
    coinbase: FeedSettings
    kraken: FeedSettings


class RiskSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kelly_fraction: float = Field(gt=0.0, le=0.5)
    max_position_notional_usd: float = Field(gt=0.0)
    max_daily_loss_pct: float = Field(gt=0.0, le=1.0)
    single_trade_drawdown_freeze_pct: float = Field(gt=0.0, le=1.0, default=0.15)
    reconcile_interval_sec: int = Field(gt=0, default=60)
    clock_drift_halt_ms: int = Field(gt=0, default=1000)
    # Hard LastSpot staleness contract (Slice 6). Market discovery and entry
    # decisions refuse to act when the primary spot tick is older than this.
    # Matches clock_drift_halt_ms by design: both gate the trading graph
    # against silent-stale-data-induced decisions.
    spot_staleness_halt_ms: int = Field(gt=0, default=1000)


class SignalSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bollinger_period_bars: int = Field(gt=0)
    bollinger_std_mult: float = Field(gt=0.0)
    min_signal_confidence: float = Field(ge=0.0, le=1.0)
    # Top-down alignment veto (DESIGN.md §6.3, Slice 8). The gate lives
    # inside the traps, not in risk.check, so rejected candidates do not
    # pollute the decision journal. If 1H RSI is bullish (> `htf_bullish_
    # veto_rsi`), reject any 5m SHORT setup; if bearish (< `htf_bearish_
    # veto_rsi`), reject any 5m LONG setup. 45/55 is the standard Wilder
    # dead band around RSI 50.
    htf_bullish_veto_rsi: float = Field(gt=0.0, lt=100.0, default=55.0)
    htf_bearish_veto_rsi: float = Field(gt=0.0, lt=100.0, default=45.0)
    # Runaway Train lockout (DESIGN.md §6.3, Slice 8; applied inside
    # detect_ceiling_reversion). When the rolling 24h move magnitude
    # exceeds this fraction, disable Trap 3 — mean-reversion against a
    # parabolic / capitulation phase has no edge.
    runaway_train_halt_pct: float = Field(gt=0.0, le=1.0, default=0.05)
    # CVD / Tape Reader veto (Slice 9). The floor trap refuses to buy into
    # a dip when the rolling 5-minute net aggressor flow is `<= -threshold`
    # (unrelenting taker selling into the lows); the ceiling trap refuses
    # to short into a pump when flow is `>= +threshold`. USD-denominated so
    # the threshold is regime-robust — a fixed BTC-denominated bar would
    # fire ~2.5x more readily at $40k BTC than at $100k BTC. Default $5M
    # of net rolling-5m flow is "substantial but not catastrophic" — above
    # normal reversion volume, below cascade scale. Fail-open on warmup.
    cvd_1m_veto_threshold_usd: float = Field(gt=0.0, default=5_000_000.0)


class SoftStopSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_fraction: float = Field(gt=0.0, le=1.0)
    regime_multiplier_high_vol: float = Field(gt=0.0)
    regime_multiplier_trending: float = Field(gt=0.0)
    time_multiplier_late_window: float = Field(gt=0.0)


class MonitorSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    early_cashout_price_cents: int = Field(ge=90, le=100)
    soft_stop: SoftStopSettings
    theta_net_book_depth_threshold: float = Field(gt=0.0)


class ExecutionSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    maker_only_entry: bool = True
    ioc_ladder_cents: list[int] = Field(min_length=1)


class TelemetrySettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bet_outcomes_logger: str
    bq_dataset: str
    bq_table: str


class IntegritySettings(BaseModel):
    """Primary/Confirmation integrity gate (docs/DESIGN.md §7.3a).

    Coinbase is primary — its prints drive FeatureEngine directly. Kraken is
    the confirmation venue and vetoes ENTRY only when its directional velocity
    actively contradicts Coinbase over `velocity_window_sec`. Silence on the
    confirmation venue is NOT a veto (low-liquidity venues legitimately
    print intermittently); prolonged silence > `stale_halt_sec` IS a veto
    (fail-closed on a broken feed).
    """

    model_config = ConfigDict(extra="forbid")

    velocity_window_sec: float = Field(gt=0.0, default=1.0)
    active_disagreement_floor_usd: float = Field(gt=0.0, default=25.0)
    stale_halt_sec: float = Field(gt=0.0, default=60.0)


class CalendarSettings(BaseModel):
    """Structured economic-calendar configuration (hard rule #8).

    `path` is relative to the config directory. Absent / empty means the
    guard runs with zero scheduled events — the human kill-switch remains
    the only override path.
    """

    model_config = ConfigDict(extra="forbid")

    path: str | None = None
    lead_seconds: float = Field(gt=0.0, default=60.0)
    tick_interval_sec: float = Field(gt=0.0, default=5.0)


class Settings(BaseModel):
    """Top-level configuration. Loaded from config/{mode}.yaml by loader.py."""

    model_config = ConfigDict(extra="forbid")

    mode: Mode
    feeds: FeedsSettings
    risk: RiskSettings
    signal: SignalSettings
    monitor: MonitorSettings
    execution: ExecutionSettings
    telemetry: TelemetrySettings
    calendar: CalendarSettings = CalendarSettings()
    integrity: IntegritySettings = IntegritySettings()
