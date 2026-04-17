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
    binance: FeedSettings


class RiskSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kelly_fraction: float = Field(gt=0.0, le=0.5)
    max_position_notional_usd: float = Field(gt=0.0)
    max_daily_loss_pct: float = Field(gt=0.0, le=1.0)
    single_trade_drawdown_freeze_pct: float = Field(gt=0.0, le=1.0, default=0.15)
    reconcile_interval_sec: int = Field(gt=0, default=60)
    clock_drift_halt_ms: int = Field(gt=0, default=250)


class SignalSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    bollinger_period_bars: int = Field(gt=0)
    bollinger_std_mult: float = Field(gt=0.0)
    min_signal_confidence: float = Field(ge=0.0, le=1.0)


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
