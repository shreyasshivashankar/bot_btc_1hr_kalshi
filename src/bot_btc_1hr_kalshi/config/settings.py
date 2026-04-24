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


class HyperliquidSettings(BaseModel):
    """Hyperliquid public WS feed for BTC OI (PR-A: DerivativesOracle).

    Replaces the Coinglass HTTP poller with a push-based source —
    `metaAndAssetCtxs` pushes a fresh asset-context snapshot at the
    venue's internal cadence (typically a few seconds), so staleness
    is measured against arrival time rather than against a 30s polling
    cycle. Disabled by default so existing dev/test configs boot
    without a derivatives feed; enable per-mode in YAML.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    asset: str = "BTC"
    # Hard staleness ceiling for `DerivativesOracle.get_open_interest`.
    # 30s = 6x the venue's typical asset-ctx cadence; gives reconnect
    # backoff a window to recover before fail-closing the accessor.
    # Snapshot / telemetry consumers use `_or_none` and tolerate a
    # cold start gracefully.
    staleness_halt_ms: int = Field(gt=0, default=30_000)


class BybitSettings(BaseModel):
    """Bybit V5 public WS feed for BTC OI + liquidations (PR-B).

    Two topics, two WS connections: `tickers.<symbol>` for OI snapshots
    and `liquidation.<symbol>` for discrete liquidation prints. Each is
    toggled independently — a mode can take OI-only or both — since
    the liquidation stream is higher-cadence and a future mode may
    want the OI confirmation without the liquidation deque feed.

    Same disabled-by-default posture as `HyperliquidSettings`. Staleness
    threshold applies to both streams; the liquidation stream is bursty
    and may idle for minutes, so the default of 60s is double the OI
    default to avoid spurious halts during quiet periods.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    ws_url: str = "wss://stream.bybit.com/v5/public/linear"
    # Linear-perp symbol on Bybit V5 — `BTCUSDT`. The parser strips the
    # USDT suffix for the emitted sample / event `symbol` field so
    # downstream consumers see the same `BTC` key that Hyperliquid uses.
    symbol: str = "BTCUSDT"
    subscribe_oi: bool = True
    subscribe_liquidations: bool = True
    staleness_halt_ms: int = Field(gt=0, default=60_000)


class BinanceBackfillSettings(BaseModel):
    """Binance Public Data S3 backfill (PR-B).

    Tool config, not a live feed: the backfill CLI
    (`scripts/binance_public_data_backfill.py`) reads this block to
    pick the dataset + symbol + output directory for each run. The
    process itself does not consume this at boot; surfacing it in
    `config/*.yaml` lets operators pin backfill params against the
    same environment that the live feeds run in.
    """

    model_config = ConfigDict(extra="forbid")

    base_url: str = "https://data.binance.vision"
    # Binance public data tree: `data/futures/um/daily/<dataset>/<symbol>/...`.
    # `metrics` carries historical OI + ratios; `liquidationSnapshot` is
    # the daily liquidation-event dump. Operators pass either via CLI.
    default_dataset: str = "metrics"
    symbol: str = "BTCUSDT"
    # Directory where the CLI lands extracted CSVs. DuckDB reads these
    # natively (`read_csv_auto`), so the Parquet conversion step lives
    # on the analytics side rather than in the backfill hot path.
    output_dir: str = "data/binance_public"


class FeedsSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kalshi: FeedSettings
    coinbase: FeedSettings
    kraken: FeedSettings
    hyperliquid: HyperliquidSettings = HyperliquidSettings()
    bybit: BybitSettings = BybitSettings()
    binance_backfill: BinanceBackfillSettings = BinanceBackfillSettings()


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
    # Correlation cap (multi-strike era). Counts open positions sharing
    # (settlement_ts_ns, side) with the pending signal. Default 3 enables
    # strike laddering: up to N adjacent-strike rungs may co-exist on the
    # same hour-direction, with per-rung Kelly auto-scaled to 1/N of the
    # base fraction inside `OMS.consider_entry` so the laddered total
    # approximates a single full-Kelly bet on the underlying directional
    # thesis. The aggregate-exposure cap (3x per-position cap) remains
    # the hard ceiling on total notional. Set to 1 to disable laddering
    # (legacy single-rung behavior); higher values widen the ladder and
    # shrink each rung. See docs/RISK.md §4.
    max_correlated_positions: int = Field(ge=1, default=3)
    # Premium cap (Slice 11 Phase 3.1). Hard reject on entries above this
    # price in cents. Kelly's math tolerates 75¢ entries (the (1-p) term
    # compensates mechanically) but the risk/reward is inverted: paying
    # 75¢ to make 25¢ means any single loss wipes three wins. This is a
    # monotonic tightening — raising the floor of acceptable bets, not
    # repricing the edge. Applies only to BUY-side (YES or NO) entries;
    # the trap emits a single-side signal, so the price is always the
    # cost of the leg being bought.
    max_entry_price_cents: int = Field(gt=0, lt=100, default=75)
    # Inverted-risk sizing clip (Slice 11 Phase 3.2). When entry price is
    # at/above `inverted_risk_threshold_cents`, multiply the fractional
    # Kelly allocation by `inverted_risk_kelly_multiplier`. At 50¢+ the
    # dollar-loss-per-contract exceeds the dollar-win, and Kelly's raw
    # sizing grows with price (the (1-p) term shrinks the denominator).
    # This clip halves exposure in that regime — a monotonic tightening,
    # not a retuning of the edge. Set multiplier=1.0 to disable without
    # removing the field. Paired with the premium cap (3.1): 3.1 draws
    # the upper bound, 3.2 scales exposure in the band below it.
    inverted_risk_threshold_cents: int = Field(gt=0, lt=100, default=50)
    inverted_risk_kelly_multiplier: float = Field(gt=0.0, le=1.0, default=0.5)


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
    # Implied-basis arb (DESIGN.md §6.4, Slice 10). Fair value comes from the
    # same Normal-CDF settlement model the other traps use for edge; when
    # Kalshi's best quote on either side is mispriced by `arb_basis_threshold
    # _cents` or more vs fair, we maker-buy the cheap side. The dead-spot
    # gate kills the trap whenever the underlying's 60s range exceeds
    # `arb_dead_spot_range_usd` — a sweeping spot means fair value is stale
    # relative to the inbound print and the "edge" is adverse selection.
    arb_basis_threshold_cents: int = Field(gt=0, default=15)
    arb_dead_spot_range_usd: float = Field(gt=0.0, default=20.0)
    # Microstructure gating (PR-C — shadow plumbing). When
    # `enable_microstructure_gating=False` (DEFAULT), the floor/ceiling
    # traps evaluate the liquidation-cascade + OI checks below but only
    # *tag* the outgoing signal's `features.shadow_veto_reason` — the
    # trade still proceeds. The risk committee uses the tagged telemetry
    # to pick empirical thresholds before flipping the switch. When True,
    # any non-None shadow veto reason hard-rejects the trap. Hard rule
    # #2's backtest → paper → shadow → live progression governs the
    # flip: do not toggle this to True until the tuning loop has real
    # soak data justifying the specific thresholds below.
    enable_microstructure_gating: bool = False
    # Liquidation-cascade window — fraction of spot above (for the
    # ceiling trap) and below (for the floor trap) within which the
    # snapshot builder accumulates recent liquidation USD into
    # `LiquidationPressure`. 0.005 = 0.5% — narrow enough to filter out
    # distant prints that have no microstructural bearing on the
    # immediate decision. Read by `feedloop._build_snapshots` when
    # populating `MarketSnapshot.liquidation_pressure`.
    liquidation_window_pct: float = Field(gt=0.0, le=1.0, default=0.005)
    # Lookback window (seconds) for the liquidation deque scan. 60s
    # captures the burst horizon — most cascades unfold in the first
    # 30-90 seconds — without including stale prints from prior regimes.
    liquidation_lookback_sec: float = Field(gt=0.0, default=60.0)
    # Liquidation cascade USD threshold. The trap rejects (or shadow-
    # tags) when adverse-side liquidation USD inside the window above
    # exceeds this value. 0.0 disables without removing the plumbing —
    # the committee sets a real threshold once shadow-soak produces
    # a distribution of liquidation pressure to anchor against.
    liquidation_cascade_threshold_usd: float = Field(ge=0.0, default=0.0)
    # Open-interest compression floor (USD). When total aggregated BTC
    # futures OI drops below this level, the trap tags a compression
    # veto: very low OI often follows a liquidation cascade, and mean-
    # reversion immediately post-cascade is structurally different from
    # reversion in normal regimes. 0.0 disables without removing the
    # plumbing — the committee sets a real threshold once shadow-soak
    # produces an OI distribution to anchor against.
    oi_compression_threshold_usd: float = Field(ge=0.0, default=0.0)


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
    guard runs with zero static (YAML) events; the Forex Factory fetcher
    (Slice 11 P1) can still populate the guard if `fetch_enabled`. The
    human kill-switch remains the backstop in either case.
    """

    model_config = ConfigDict(extra="forbid")

    path: str | None = None
    lead_seconds: float = Field(gt=0.0, default=60.0)
    # Post-event blackout (docs/RISK.md §Macro-blockers). New entries stay
    # rejected for this long AFTER ev.ts_ns — pairs with `lead_seconds`
    # (pre-event flatten) to cover the full volatility skirt around CPI /
    # FOMC / NFP prints. `risk.check()` reads the combined window via
    # `CalendarGuard.is_blocked(now_ns)`.
    cooldown_seconds: float = Field(gt=0.0, default=1800.0)
    tick_interval_sec: float = Field(gt=0.0, default=5.0)
    # Forex Factory auto-refresh (Slice 11 P1 — Macro Blockers). When
    # enabled, a background task polls `fetch_url` every `fetch_interval_sec`
    # and hot-swaps the guard's event list. Static YAML (`path`) still
    # takes precedence — names colliding between sources resolve to the
    # YAML entry (operators may override an FF-scheduled event that is
    # wrong or stale). Allow-lists default to US/High-impact because
    # that's the set of prints historically associated with the BTC
    # vol spikes we flatten for; widen only with risk-committee sign-off.
    fetch_enabled: bool = False
    fetch_url: str = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    fetch_interval_sec: float = Field(gt=0.0, default=1800.0)
    tier_1_countries: tuple[str, ...] = ("USD",)
    tier_1_impacts: tuple[str, ...] = ("High",)


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
