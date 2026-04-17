# bot_btc_1hr_kalshi — System Design Specification

**Status:** Draft v1.0
**Owner:** Shrey
**Last reviewed:** 2026-04-16
**Classification:** Internal — contains live trading logic

---

## 1. Executive Summary

bot_btc_1hr_kalshi is an autonomous trading system that quotes and takes positions in **Kalshi's hourly BTC range markets**. Each market resolves YES/NO based on whether BTC settles above a strike at the top of the hour. Contracts are capped at $1 (100¢), pay $1 on correct resolution, and expire worthless otherwise.

The system exploits two inefficiencies:

1. **Spatial edge (primary):** Kalshi strikes mispriced relative to structural volatility bands on the underlying spot market. Edge is derived from market microstructure, not latency.
2. **Temporal edge (secondary):** Kalshi's Real-Time Index (RTI) lag during violent spot moves. Demoted from primary due to 2026 HFT competition — treated as opportunistic, not core.

The core risk principle is **asymmetric capital preservation**: we accept frequent small wins in exchange for hard-capped tail losses. We never seek to maximize per-trade return; we seek to maximize Sharpe-adjusted compounded growth under a fixed daily drawdown constraint.

---

## 2. Strategy Thesis & Edge Hypothesis

### 2.1 Why this edge should exist

- Kalshi order books for hourly BTC are thinner than Coinbase/Binance spot. Market makers must price wide to hedge inventory risk across a volatile underlying.
- Kalshi's contract structure (binary, capped at 100¢) creates nonlinear payoff asymmetry: buying YES at 30¢ when the true implied probability is 45% offers +50% ROI with hard-bounded loss.
- The RTI is a weighted average of external spot prices with a ~200–900ms smoothing window. During discrete shocks (liquidations, CPI prints), Kalshi's quoted strikes lag the true probability for 2–15 seconds.
- Spot-market volatility regimes (Bollinger expansion/contraction) are statistically persistent on the 5m–1h horizon, per decades of published options microstructure research.

### 2.2 Edge decay & validation

- Edge is assumed to decay. Parameters are **refit weekly via walk-forward optimization** over a rolling 90-day window.
- Strategy is decommissioned if **rolling 30-day paper Sharpe < 0.8** or **rolling 30-day hit rate < 48%**.
- A shadow-mode copy of the strategy runs continuously, producing the decisions the live system *would* make had it been deployed; divergence beyond 5% PnL over 7 days triggers a review.

### 2.3 Explicit non-edges (do not build around these)

- We do NOT have a latency edge vs co-located Kalshi HFTs.
- We do NOT have alternative data (order flow from exchanges, dealer positioning).
- We do NOT have a model of BTC direction — we only model *volatility regimes and microstructure dislocation*.

---

## 3. System Architecture

### 3.1 High-level dataflow

```
┌────────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐
│ Kalshi WS feed     │────▶│              │     │              │     │            │
│ Coinbase WS feed   │────▶│ Market Data  │────▶│ Signal       │────▶│ Risk       │
│ Binance WS feed    │────▶│ (normalize,  │     │ (regime,     │     │ (sizing,   │
│ News feed (Tier1)  │────▶│ L2 book, RTI │     │ traps,       │     │ circuit    │
│ NTP / PTP clock    │────▶│ calc, clock) │     │ features)    │     │ breakers)  │
└────────────────────┘     └──────────────┘     └──────────────┘     └─────┬──────┘
                                                                           │ Approve
                                                                           ▼
┌────────────────────┐     ┌──────────────┐     ┌──────────────┐     ┌────────────┐
│ Prometheus metrics │◀────│ Observability│◀────│ Portfolio /  │◀────│ Execution  │
│ Alert router       │◀────│ (logs,       │     │ OMS          │     │ (SOR, IOC, │
│ Decision journal   │◀────│ metrics,     │◀────│ (positions,  │◀────│ maker      │
│ (Parquet → S3)     │     │ traces)      │     │ PnL, recon.) │     │ placement) │
└────────────────────┘     └──────────────┘     └──────────────┘     └────────────┘
```

Each module is an **isolated process or async task** communicating over typed in-memory channels (`asyncio.Queue`) in dev and a local message bus (ZeroMQ / Redis Streams) in prod. This lets us replay a captured feed through the exact same signal/risk/execution stack used in live.

### 3.2 Determinism & time

- **All timestamps are event-sourced.** Components receive `MarketEvent(ts_ns, ...)` and must never call wall-clock time directly.
- **Nanosecond precision** across the pipeline. Store as `int64` nanoseconds since epoch.
- **NTP/chrony** runs on the host; clock skew monitored via Prometheus. >250ms drift halts trading.
- **Backtest determinism:** identical event stream + identical config → identical orders, bit-for-bit. This is gated by a CI test that replays a golden scenario and diffs the decision journal.

### 3.3 Language / runtime

- Python 3.12, `asyncio`-based event loop. `uvloop` in prod.
- Hot paths (book walking, band calculation, RTI reconstruction) in `numpy` + `numba`. If any hot path exceeds a 1ms budget, it is rewritten in Cython or Rust via `pyo3`.
- State serialization via `msgpack` (internal) or `parquet` (persistent).

---

## 4. Market Data Layer

### 4.1 Sources

| Feed           | Purpose                        | Primary / Secondary | Staleness SLO |
| -------------- | ------------------------------ | ------------------- | ------------- |
| Kalshi WS      | Order book, RTI, fills         | Primary             | 500ms         |
| Coinbase WS    | BTC-USD L2 book, trades        | Primary             | 300ms         |
| Binance WS     | BTCUSDT L2 book, trades        | Secondary (fail-over) | 300ms       |
| Kraken WS      | BTC-USD L2 book, trades        | Tertiary            | 500ms         |
| Economic calendar (ForexFactory / TradingEconomics structured JSON) | Scheduled macro releases (CPI, FOMC, NFP, PCE) | Single source | Poll 60s |
| Human kill-switch (HTTP admin endpoint) | Unscheduled qualitative shocks | Manual | n/a |

**No NLP on unstructured news.** v1.0 flagged NewsAPI / TreeOfAlpha NLP as the Tier 1 trigger. This was removed: parsing real-time crypto headlines with NLP is too prone to spoofing and false positives (a spoofed tweet parsed as "geopolitical shock" would force bot_btc_1hr_kalshi to liquidate profitable positions via IOC at severe slippage). Replaced by two deterministic channels:

1. **Structured economic calendar.** Scheduled releases (CPI, FOMC, NFP, PCE, rate decisions) are known in advance. Response is **pre-emptive, not reactive**: the risk engine auto-flattens at T-60s before a calendar event with impact=High. No NLP, no parsing — just a clock and a date.
2. **Human kill-switch.** For unscheduled qualitative events (exchange hacks, geopolitical shocks), an authenticated HTTP admin endpoint (`POST /admin/tier1_override`, see §15.4) lets the operator flatten the book in one call. This is the only supported path for NLP-class events.

### 4.2 Responsibilities

- **Normalization:** unify tick format across venues (`Trade`, `Quote`, `BookSnapshot`, `BookDelta`).
- **L2 book reconstruction:** maintain sorted-level order book per venue; track top-of-book, mid, microprice, and 10-level depth.
- **Microprice:** `(bid*ask_size + ask*bid_size) / (bid_size + ask_size)` — used in preference to mid when book is skewed.
- **Composite spot:** volume-weighted average of Coinbase, Binance mid-prices (excluding outliers >3σ from 1m median). This is our ground truth for the underlying.
- **RTI reconstruction:** maintain our own mirror of Kalshi's RTI to detect lag. Divergence of our composite spot vs Kalshi RTI is the primary lag-arbitrage signal.
- **Fail-over:** if primary feed stale >2s, halt trading, fail over to secondary, re-validate last-known book with REST snapshot before resuming.

### 4.2.1 Sequence integrity & book invalidation

Every venue WS feed carries a monotonically increasing sequence number. A dropped packet leaves the local book *silently corrupted* — and every downstream feature (microprice, depth, OFI, band-penetration confirmation) becomes untrustworthy. This is a single point of failure and is handled explicitly:

1. **Per-feed gap detector.** Each `BookDelta` message has `seq_n`. If `seq_n ≠ last_seq + 1`, the feed is marked `TAINTED`.
2. **Feature invalidation.** On `TAINTED`, all book-derived features for that venue (microprice, top-of-book, depth, OFI) are marked `INVALID` in the feature store. The signal layer does not consume `INVALID` features — pending trap evaluations for the current 15s regime bucket are dropped.
3. **Recovery.** The feed handler unsubscribes, fetches a fresh REST snapshot, rebuilds the book from scratch, resumes WS from the snapshot sequence, and only then clears the `TAINTED` flag.
4. **Recovery SLO:** 1.5s p95. If recovery takes >3s, the feed is failed over to secondary.
5. **Reconnection storm breaker.** If a feed tripped `TAINTED` ≥3 times in a rolling 5 minutes, it is quarantined for 10 minutes (secondary used). Repeated gaps suggest a broken session or venue outage, not noise.

The gap detector runs as a bytewise check in the WS handler — it must not rely on parsed message semantics.

### 4.3 Storage

- All raw ticks written to local Parquet partitioned by `date/venue/symbol` via a write-ahead buffer (batched every 500ms). Sync to S3 nightly.
- Tick store is the source of truth for backtests and post-mortems.

---

## 5. Signal Layer

### 5.1 Regime detection (Phase 1 of the original spec, formalized)

We compute a **regime vector** `R = (trend, vol, session_bias, microstructure)` each 15s:

- **trend:** `price - SMA_200(1h)` normalized by `ATR(1d)`. Buckets: strong-down (<-1.5), down, neutral (±0.5), up, strong-up (>1.5).
- **vol:** ratio of `ATR(15m)` to `ATR(4h)`. Buckets: compressed (<0.7), normal, expanding (>1.3), chaotic (>2.0).
- **session_bias:** signed z-score of cumulative session VWAP deviation. Identifies where heavy capital sits.
- **microstructure:** `BB_width(20, 2σ, 15m)` as % of price — primary gating for entry aggressiveness.

Every trap is gated by a **regime compatibility matrix** (see §11 Appendix A). E.g. Trap 3 (Ceiling Bleed) is disabled when `trend = strong-up`.

### 5.2 Traps (entry logic)

#### Trap 1 — Lag Arbitrage (Opportunistic / Secondary)

- **Trigger:** composite spot moves >$40 in <60s AND Kalshi RTI lags our reconstruction by >$15 for >200ms.
- **Action:** buy the strike where true buffer (|composite_spot − strike|) > $50 but Kalshi prices the contract tight (<30¢ for a >70% implied prob side).
- **Guard:** max 1 concurrent lag trade. If filled, immediately set dynamic-theta exit. If not filled in 3s, cancel.
- **Known adversarial risk:** HFTs will only fill us when *they* know the move is continuing — this is **adverse selection**. Mitigation: require 2 independent confirmations (e.g., spot move on Coinbase AND Binance with aligned order flow imbalance).

#### Trap 2 — Floor Bounce (Primary)

- **Precondition:** 1H macro bias ∈ {bullish, neutral}. BB width ≥ historical 40th percentile (not compressed to death).
- **Trigger:** 5m composite spot breaks `LowerBand(20, 2σ)` AND 5m RSI(14) < 35 AND order-flow imbalance in last 30s > 0 (buyers absorbing).
- **Action:** buy YES at strike below the structural floor where ROI ≥ 30% (entry ≤ 76¢).
- **Confidence score:** linear combination of RSI distance below 35, band penetration depth, and OFI — scales Kelly fraction (§7).

#### Trap 3 — Ceiling Bleed (Primary)

- **Precondition:** 1H macro bias ∈ {bearish, neutral}. 24h move < 5% (Runaway Train Lockout).
- **Trigger:** 5m composite spot breaks `UpperBand(20, 2σ)` AND 5m RSI(14) > 65 AND OFI < 0.
- **Action:** buy NO at strike above the structural ceiling where ROI ≥ 30%.

### 5.3 Feature store

All features (regime components, RSI, BB width, OFI, spot-RTI basis, book depth, spread, volatility cones) are computed by a single `FeatureEngine` and persisted to Parquet on every tick-bucket close. This is the input to:

- Live signal layer
- Backtest engine (replay same computation)
- Post-hoc analysis / model research

Features are versioned. Adding a feature requires incrementing `FeatureEngine.VERSION` and re-running backtests.

---

## 6. Risk Management Framework

Risk is **the most important module** and its output is authoritative. Every order routes through `risk.check(order, portfolio_state, market_state) → Approve | Reject(reason)`. Risk is a pure function with no I/O, which makes it unit-testable.

### 6.1 Position sizing — Fractional Kelly × ATR

Base allocation = **2% of daily margin** per trade.

Scaling:

- **Volatility adjustment:** scale by `clamp(ATR_15m_median / ATR_15m_current, 0.5, 2.0)`. Tight bands → size up to 4%; chaotic → down to 1%.
- **Confidence adjustment:** multiply by `signal_confidence ∈ [0.3, 1.0]` derived from the trap.
- **Kelly cap:** the implied Kelly fraction is `f* = (p·b − q) / b` where p = historical hit rate for this trap, q = 1−p, b = avg win / avg loss. We cap actual allocation at **½ × f\*** (fractional Kelly is standard — full Kelly is provably aggressive under parameter uncertainty).

### 6.2 Portfolio-level limits

| Limit                          | Value        | Enforcement               |
| ------------------------------ | ------------ | ------------------------- |
| Max concurrent positions       | 3            | Block new entries         |
| Max gross exposure / hour      | 25% of margin| Block new entries         |
| Max daily loss (soft)          | 5% of margin | Warn, reduce sizing by 50%|
| Max daily loss (hard)          | 10% of margin| Halt new entries, hold    |
| Single-trade max loss          | 15% of margin| Drawdown Freeze (60 min)  |
| Correlation cap (YES+YES same hour) | 1 position | Block duplicates     |
| Open orders cap                | 10           | Block new orders          |

### 6.3 Circuit breakers

Each runs as an independent async task. Any breaker trips → `SystemHalt` event → execution drains.

- **Drawdown Freeze:** single-trade loss >15% of daily margin → 60min API lockout.
- **Runaway Train Lockout:** 24h BTC move >5% → Trap 3 disabled for the rest of the session.
- **Top-Down Alignment Veto:** 1H RSI > 55 vetoes any 5m short setup, and vice versa.
- **Clock drift breaker:** NTP offset >250ms → halt.
- **Data staleness breaker:** primary feed silent >2s → halt (see §4 fail-over).
- **Consecutive loss breaker:** 3 consecutive losses in <30min → pause 15min.
- **Reconciliation breaker:** local vs broker position mismatch >1 contract → halt, page operator.
- **Tier 1 News Override:** flatten the entire book via IOC sells. 2h stabilization window (no new entries).

### 6.4 VaR / stress

- **Historical VaR (95%, 1h):** computed at session open from trailing 180 days. If current gross exposure × portfolio beta > VaR, block new entries.
- **Stress scenarios:** replay top 5 worst historical hours (e.g., FTX collapse, March 2020, Binance depeg). Strategy must survive all scenarios with ≤25% drawdown in backtest CI.

---

## 7. Execution & Market Microstructure

### 7.1 Order types

- **Entry:** POST-ONLY limit orders (reject if cross). Never take on entry.
- **Exit (normal):** limit sell at dynamic-theta target (see §7.3).
- **Exit (emergency / end-of-hour):** Aggressive Limit IOC escalation ladder defined in §7.3.2. Terminal states (Abandon to Settlement vs Take Any Bid) in §7.3.3.

### 7.2 Smart Order Router (SOR)

- Scans Level 2 depth before any fill attempt. If desired size exceeds top-3 levels' aggregate, split into child orders over 2–5s with jitter to avoid signaling.
- **Queue-position model:** estimated time-to-fill at a given price level, given observed queue consumption rate. This is optimized purely for **fill probability**, not for rebates.
- **Fee schedule is config, not code.** Kalshi's fee schedule (per-contract fees, volume tiers, resolution fees) lives in `config/fees.yaml`, versioned and loaded at startup. The cost model consumes this config; no fee constant is ever hardcoded in routing logic.
- **No maker-rebate optimization.** v1.0 had SOR "prefer levels where we're the first maker" for rebate capture. This is removed: Kalshi's hourly BTC contracts do not pay a maker-taker rebate in the 2026 fee schedule, so optimizing for it adds routing cycles for zero edge. If this changes, update `config/fees.yaml` and re-enable in the router with explicit math.

### 7.3 Dynamic Theta Net (exit target)

Replaces the original fixed 92–95¢ target with a time-decayed curve:

| Minutes into hour | Target exit price | Rationale                                  |
| ----------------- | ----------------- | ------------------------------------------ |
| 00–45             | 92¢               | Normal regime — collect full edge          |
| 45–50             | 88¢               | Liquidity begins thinning                  |
| 50–55             | 85¢               | Aggressive — take what exists              |
| 55–60             | IOC escalation ladder (see §7.3.2) | Liquidity void — formal decision tree |

- If position is profitable but target not hit by XX:55, enter the IOC escalation ladder (§7.3.2).
- If position is underwater at XX:55, same — but parameters differ (see §7.3.3).

### 7.3.1 Book-depth deterioration metric (preventative)

To avoid discovering a liquidity void at XX:55, we measure it starting at XX:48:

- **Definition:** `top3_depth_decline_pct = 1 − (top3_depth_now / top3_depth_median_prev_15m)` measured minute-over-minute on the Kalshi side of the book we need to exit through.
- **Behavior:** if `> 50%` for two consecutive minutes, **pull the dynamic theta target forward by one tier** (e.g., the XX:50 target applies at XX:48). This gets us out while liquidity still exists.
- Metric is logged every minute to `bot_btc_1hr_kalshi_book_depth_deterioration_pct` for monitoring.

### 7.3.2 IOC escalation ladder at XX:55+

After the dynamic theta expires, exit follows a bounded escalation. Each rung has a timeout; unfilled → next rung.

| Rung | Price                  | Timeout | Notes                                           |
| ---- | ---------------------- | ------- | ----------------------------------------------- |
| 1    | `best_bid − 3¢`, IOC   | 400ms   | Retry up to 3× with recomputed best_bid         |
| 2    | `best_bid − 6¢`, IOC   | 400ms   | Retry up to 2×                                  |
| 3    | `best_bid` or any live bid (market-equivalent IOC crossing the full spread) | 400ms | Last-chance aggressive take |
| 4    | **Terminal decision** — see §7.3.3 | — | No live bid, or all rungs failed |

Rungs 1–3 are bounded to total ≤ 2 seconds wall clock. This leaves XX:57–58 for the terminal decision if needed.

### 7.3.3 Terminal decision: Abandon vs Any-Bid

If rung 3 returns no fill (book is a ghost town), we choose explicitly between two formal terminal states. The choice is driven by the **current settlement probability** `p_settle` — the probability the contract resolves in our favor, estimated from current composite spot, strike, and remaining-time volatility cone.

| p_settle | Terminal state         | Rationale                                                              |
| -------- | ---------------------- | ---------------------------------------------------------------------- |
| > 0.60   | **ABANDON_TO_SETTLEMENT** | Holding has positive EV — lock margin, let it settle                |
| < 0.40   | **TAKE_ANY_BID**       | Fire IOC at any live bid (even 1¢). Bounded loss beats guaranteed zero |
| 0.40–0.60 | **ABANDON_TO_SETTLEMENT** | Default: guaranteed slippage is worse than EV-neutral binary        |

`ABANDON_TO_SETTLEMENT` is a **formal state in the OMS** — not a failure mode. When set:
- Position margin remains locked; risk engine excludes this capital from the next session until Kalshi resolves.
- The bet-outcome telemetry emits `exit_reason="abandoned_to_settlement"` with `p_settle` and book depth at abandonment.
- A follow-up event `exit_reason="settled_win"` or `"settled_loss"` is emitted when Kalshi publishes the resolution, closing the correlation chain.

`TAKE_ANY_BID` crosses whatever spread exists. Slippage is logged but not treated as an emergency.

`p_settle` calculation: `Normal.cdf((strike − spot) / (σ_1m × sqrt(minutes_remaining)))` with `σ_1m` = realized 1-minute volatility from the last 15m, sign adjusted for YES/NO direction. Implemented in `execution/settlement_prob.py`.

### 7.4 Slippage model (for backtest and shadow mode)

- Backtests assume maker fills ONLY if limit price was touched AND we can reconstruct plausible queue position given observed cancellations. This prevents backtest overfit to phantom fills.
- Taker fills apply a cost model: `slippage = f(size, top_3_depth, realized_volatility_30s)`. Calibrated weekly from live fills.

### 7.4a Active Position Monitor

Every open position is watched by an always-on `PositionMonitor` coroutine, tick-by-tick on the Kalshi market feed. It is separate from the entry signal layer — its only job is to exit. It evaluates three independent exit conditions on every Kalshi quote update:

#### A. Theta Net target hit
Position price ≥ current dynamic-theta target (§7.3). Fire limit sell at target. Normal path.

#### B. Profit-taking early cash-out (99% probability)
If Kalshi quotes the position at **≥ 99¢** (for a YES long; ≤ 1¢ for a NO long), cash out immediately via aggressive IOC at `best_bid − 1¢`. The remaining 1¢ of upside is not worth the capital lock-up through settlement.

Rationale: capital recycles into the next hour's market faster than waiting for Kalshi resolution (which credits cash T+0 but only after 4 PM ET settlement cycles on some markets). This is the single biggest driver of realized Sharpe when the strategy is hot — winners compound fastest when capital is freed.

Configurable threshold: `EARLY_CASHOUT_THRESHOLD_CENTS = 99`. Lowering this (e.g., to 97) trades marginal EV for faster recycling and is valid; raising above 99 is not permitted (liquidity at 100¢ is non-existent on binaries).

#### C. Soft stop-loss (relaxed, adaptive)
The hard drawdown freeze (15% daily margin, §6.3) is preserved and inviolable. Below that, we apply a **soft stop** that triggers a normal-path exit *before* the hard freeze trips — so we lose 8¢ on a trade instead of 40¢.

Soft stop is **not a fixed percentage**. It is derived per-trade from the entry context:

```
soft_stop_cents = entry_price_cents × (1 − SOFT_STOP_FRACTION)
  where SOFT_STOP_FRACTION = base_fraction × regime_multiplier × time_multiplier
```

- `base_fraction = 0.35` default (tunable, §16.2 tuning loop).
- `regime_multiplier`: 0.7 in `compressed` vol (tighter stop), 1.3 in `chaotic` (wider — avoid getting stopped by noise).
- `time_multiplier`: 1.0 at XX:00, linearly tightening to 0.5 by XX:45 (we run out of time for a reversal).

Trigger condition: last observed Kalshi mid for our position is ≤ `soft_stop_cents` for at least 2 consecutive seconds (debounce against single-tick wicks). On trigger, the monitor cancels any resting theta-net limit and fires an IOC exit at `best_bid − 3¢`.

This is **not** a circuit breaker — it is a normal exit path, no lockout, no halt. The bot continues trading the next hour.

#### Priority
Conditions evaluate in order: B (early cash-out) > A (theta net) > C (soft stop). If multiple conditions could fire in the same tick, B wins (we prefer taking guaranteed profit). Only one exit order is in flight per position at a time; state transitions are logged with correlation_id.

#### What this replaces
Removed from v1.0: the implicit assumption that positions just sit at a resting 92¢ limit until filled or XX:55. That "set-and-forget" model blows up when Kalshi prices the position at 98¢ at XX:30 and then reverses — we leave money on the table, or worse, watch it go to 40¢ without an active exit decision.

### 7.5 Reconciliation

- Every 60s: pull authoritative position + order state from Kalshi REST. Compare to local OMS.
- Mismatch of >0 but ≤1 contract: log and self-heal from broker state.
- Mismatch >1 contract: halt, page operator. Do NOT attempt auto-correction.

---

## 8. Research & Validation Framework

### 8.1 Backtest engine

- **Event-sourced tick replay.** Same binaries as live; only the broker and clock are mocked.
- **Walk-forward optimization:** 90-day train window, 14-day out-of-sample test. Rolls weekly. Report: Sharpe, Sortino, max DD, Calmar, hit rate, avg win / avg loss, trade count, turnover.
- **No look-ahead test:** unit test verifies that every feature computed at time `t` uses only data with `ts ≤ t`. CI fails if violated.
- **Realistic fill assumptions:** see §7.4.

### 8.2 Paper trading

- Live market data + simulated fills against real book. Minimum 48 hours green before a change is eligible for shadow.
- PnL must match backtest PnL for the same period within 10% (validates no live-only bugs).

### 8.3 Shadow mode

- Runs the candidate strategy *alongside* production on the same live feed. Records the orders it *would* have sent. No real execution.
- Requires 24h of shadow data with divergence <5% in decisions before promotion to live.

### 8.4 Promotion gate

```
Develop → backtest (green) → paper (≥48h green) → shadow (≥24h green) → live (capped size for 1 week) → full size
```

Each gate has a checklist in `docs/RUNBOOK.md`. A merge to `main` does not trigger any promotion — promotion is a separate, signed-off action.

---

## 9. Observability & Ops

### 9.1 Structured logging

- JSON logs via `structlog`. Every log line has: `ts_ns`, `component`, `event`, `correlation_id`, `severity`.
- **Decision Journal:** every signal, risk decision, and order carries a `correlation_id` that threads through all logs and persists to a daily Parquet file. This file is the audit trail for PnL attribution and post-mortems.

### 9.2 Metrics (Prometheus)

Required metrics:

- `bot_btc_1hr_kalshi_order_latency_ms` histogram by venue and stage (decide → submit → ack → fill).
- `bot_btc_1hr_kalshi_feed_staleness_ms` gauge per feed.
- `bot_btc_1hr_kalshi_pnl_usd` gauge (realized, unrealized, fees).
- `bot_btc_1hr_kalshi_drawdown_pct` gauge.
- `bot_btc_1hr_kalshi_position_count` gauge.
- `bot_btc_1hr_kalshi_circuit_breaker_state` gauge per breaker.
- `bot_btc_1hr_kalshi_clock_offset_ms` gauge.
- `bot_btc_1hr_kalshi_risk_reject_total` counter by reason.
- `bot_btc_1hr_kalshi_reconciliation_mismatch` counter.

### 9.3 Alerting

- **P1 (page):** any halt, reconciliation mismatch, clock drift breach, feed down on primary+secondary.
- **P2 (email):** daily loss > soft cap, hit rate drift, latency SLO breach.
- **P3 (dashboard):** individual trade losses, maker rejection rate.

### 9.4 Runbook-driven ops

Every page has a runbook entry in `docs/RUNBOOK.md` with: symptom, likely cause, diagnostic commands, recovery procedure, escalation. If a new failure mode occurs in prod, writing the runbook entry is part of closing the incident.

---

## 10. Failure Modes & Recovery

| Failure                                   | Detection                         | Automatic response                        | Operator action                |
| ----------------------------------------- | --------------------------------- | ----------------------------------------- | ------------------------------ |
| Primary feed down                         | Staleness >2s                     | Fail-over to secondary, re-validate book  | Investigate feed               |
| All spot feeds down                       | All stale >2s                     | Halt, flatten book via IOC                | Wait for data restoration      |
| Kalshi API unavailable                    | REST 5xx or WS disconnect >10s    | Halt, hold positions (cannot exit)        | Escalate; manual hedge via spot |
| Clock drift >250ms                        | NTP offset check                  | Halt                                      | Fix NTP                        |
| Position mismatch >1 contract             | 60s reconciliation                | Halt, page                                | Manual reconcile + RCA         |
| Consecutive losses (3 in 30m)             | PnL tracker                       | Pause new entries 15m                     | Review regime, possibly disable trap |
| Drawdown freeze tripped                   | Single trade loss >15%            | 60m API lockout                           | RCA before resume              |
| Daily hard cap hit                        | Daily realized PnL < −10% margin  | Halt new entries, hold/flatten at XX:55   | End-of-day review              |
| Unexpected exception in signal/risk       | Unhandled exception               | Halt, alert                               | Fix + add regression test      |

**Crash recovery:** on restart, the system reads broker state first, reconstructs local OMS, reconciles, and only then resumes. It never trusts persisted local state blindly.

---

## 11. Deployment — GCP Serverless

bot_btc_1hr_kalshi runs entirely on **Google Cloud Platform managed services**. No VMs, no GKE, no EC2-class compute to patch. The trade-off (~50–200ms extra latency on cold-start paths) is acceptable because our edge is spatial (microstructure), not temporal — see §2.3.

### 11.1 Component map

| Concern                  | GCP service                                       | Notes                                                        |
| ------------------------ | ------------------------------------------------- | ------------------------------------------------------------ |
| Long-running trading process | **Cloud Run** (service, not job)              | `min-instances=1`, `max-instances=1`, CPU always allocated   |
| Secrets (Kalshi API key) | **Secret Manager**                                | Mounted as env vars at container boot                        |
| Non-sensitive config     | **Cloud Run env vars**                            | Settable via console or `env.yaml` import                    |
| Structured logs          | **Cloud Logging**                                 | JSON via stdout → auto-ingested; 5-day retention log bucket  |
| Bet-outcome telemetry    | **BigQuery** (via Log Router sink)                | 5-day partition expiration for auto-cleanup                  |
| Scheduler (daily ops)    | **Cloud Scheduler**                               | Triggers reconciliation, daily close, walk-forward re-fit    |
| Operator control         | **HTTP admin endpoints** + `gcloud` CLI scripts   | IAM-gated, no public ingress                                 |
| Historical tick storage  | **Cloud Storage** (GCS bucket)                    | Parquet tick archive, nightly sync                           |
| Metrics (optional)       | **Cloud Monitoring** (custom metrics)             | Prometheus-equivalent gauges/counters via OpenTelemetry      |

### 11.2 Cloud Run service model

Why Cloud Run (not Cloud Functions):
- Cloud Functions caps execution at 60 min (Gen 2 HTTP). Our bot runs continuously. Not viable.
- Cloud Run supports `min-instances=1` with CPU always allocated — the container runs 24×7, event loop intact, WS sessions stable.
- Cloud Run is fully managed. No node pools, no patching, no SSH.

Service config (see `deploy/cloudrun.yaml`):
- `min-instances: 1`, `max-instances: 1` — single-writer semantics; trading state is not horizontally scalable.
- `cpu: 2`, `memory: 2Gi` — headroom for numpy hot paths.
- `cpu-boost: true` — faster cold start on deploy.
- `execution-environment: gen2` — full Linux compatibility for `uvloop`, `numba`.
- **CPU always allocated** (not request-based billing) — the event loop processes WS messages when no HTTP request is active.
- Ingress: `internal-and-cloud-load-balancing`. Public internet cannot reach the admin port.
- Service account has minimum scopes: Secret Manager accessor, Log Writer, BigQuery Data Editor (for bet outcomes only), Cloud Storage Object Admin (tick bucket only).

### 11.3 Secrets & environment

**Secret Manager** holds:
- `BOT_BTC_1HR_KALSHI_API_KEY`
- `BOT_BTC_1HR_KALSHI_API_SECRET`
- `BOT_BTC_1HR_KALSHI_COINBASE_API_KEY` (optional, for authenticated feeds)
- `BOT_BTC_1HR_KALSHI_BINANCE_API_KEY` (optional)
- `BOT_BTC_1HR_KALSHI_ADMIN_TOKEN` — bearer token for admin endpoints

Secrets are mounted as env vars at container start via `--set-secrets` on deploy. They never appear in the container image, the YAML manifest, or logs.

**Env vars (non-secret)** are defined in `deploy/env.example.yaml`. Settable via console or `gcloud run services update --env-vars-file=env.yaml`. See §11.6 for the full list.

### 11.4 HTTP admin endpoints (single-tenant, IAM-gated)

The service exposes a small HTTP admin surface on `:8080` alongside the trading loop. All endpoints require `Authorization: Bearer $BOT_BTC_1HR_KALSHI_ADMIN_TOKEN` **and** IAM `roles/run.invoker` on the Cloud Run service. Two-layer auth.

| Endpoint                  | Method | Purpose                                                        |
| ------------------------- | ------ | -------------------------------------------------------------- |
| `/healthz`                | GET    | Liveness: returns 200 if event loop heartbeat < 5s old         |
| `/readyz`                 | GET    | Readiness: 200 only if all feeds green, risk engine initialized|
| `/admin/status`           | GET    | JSON: open positions, PnL, breaker states, feed health         |
| `/admin/halt`             | POST   | Soft-halt: no new entries; existing positions continue to exit |
| `/admin/resume`           | POST   | Clear soft-halt                                                |
| `/admin/tier1_override`   | POST   | Immediately flatten entire book via IOC (see §4.1)             |
| `/admin/flatten`          | POST   | Alias for tier1_override, explicit intent                      |

Admin actions emit audit log entries to `bot_btc_1hr_kalshi.admin_audit` log with the invoker identity resolved from the IAM principal on the Cloud Run request.

### 11.5 Start / Stop control

Two orthogonal controls:

1. **Soft pause (trading logic only)**: `POST /admin/halt` — container keeps running, feeds stay warm, but no new entries are placed. Existing positions continue to exit normally. Use this for most day-to-day pausing. Scripts: `scripts/halt.sh`, `scripts/resume.sh`.

2. **Hard stop (container scaled to zero)**: `gcloud run services update bot-btc-1hr-kalshi --min-instances=0 --max-instances=0` — container terminates. No billing, no WS sessions, no monitoring of open positions. **Only safe when there are zero open positions**; the start script verifies this via `/admin/status` before scaling to zero. Scripts: `scripts/start.sh`, `scripts/stop.sh`.

Hard stop with open positions is intentionally blocked — scaling to zero mid-trade orphans positions that can then decay without oversight. If the operator really must do this, they must first `/admin/flatten`.

### 11.6 Environment variables

Declared in `deploy/env.example.yaml`. Categories:

- **Deployment mode:** `BOT_BTC_1HR_KALSHI_MODE ∈ {paper, shadow, live}`, `BOT_BTC_1HR_KALSHI_ENV ∈ {dev, staging, prod}`
- **Risk policy overrides:** `BOT_BTC_1HR_KALSHI_BASE_ALLOCATION_PCT`, `BOT_BTC_1HR_KALSHI_SOFT_STOP_FRACTION`, `BOT_BTC_1HR_KALSHI_EARLY_CASHOUT_THRESHOLD_CENTS`
- **Feed endpoints:** `BOT_BTC_1HR_KALSHI_WS_URL`, `BOT_BTC_1HR_KALSHI_COINBASE_WS_URL`, etc. — documented so we can repoint at staging endpoints
- **Logging:** `BOT_BTC_1HR_KALSHI_LOG_LEVEL`, `BOT_BTC_1HR_KALSHI_BET_OUTCOMES_BQ_DATASET`, `BOT_BTC_1HR_KALSHI_BET_OUTCOMES_BQ_TABLE`
- **GCP context:** `BOT_BTC_1HR_KALSHI_GCP_PROJECT`, `BOT_BTC_1HR_KALSHI_GCP_REGION` (auto-populated by Cloud Run but overridable for local dev)
- **Clock/NTP:** `BOT_BTC_1HR_KALSHI_MAX_CLOCK_DRIFT_MS`

All params are validated at startup via `pydantic-settings`; any missing required var fails the container and the revision is rolled back.

### 11.7 Infrastructure provisioning

One-time bootstrap via `deploy/setup_gcp.sh`:

1. Enable APIs: `run`, `secretmanager`, `logging`, `bigquery`, `cloudscheduler`, `cloudbuild`.
2. Create service account `bot-btc-1hr-kalshi-runtime@<project>.iam.gserviceaccount.com` with minimum-scope roles.
3. Create secrets in Secret Manager (values entered interactively).
4. Create Cloud Logging log bucket `bot-btc-1hr-kalshi-bets-5d` with `retentionDays=5` in the same region.
5. Create a log sink routing `logName:"projects/<project>/logs/bot_btc_1hr_kalshi.bet_outcomes"` to:
   - The `bot-btc-1hr-kalshi-bets-5d` log bucket (for Logs Explorer queries).
   - A BigQuery dataset `bot_btc_1hr_kalshi_bet_outcomes` with `defaultPartitionExpirationMs=5*24*3600*1000` (5 days, for SQL queries).
6. Exclude `bot_btc_1hr_kalshi.bet_outcomes` from the default `_Default` sink to avoid dual storage and cost.
7. Create GCS bucket `bot-btc-1hr-kalshi-tick-archive-<project>` with lifecycle rule (move to COLDLINE at 30d, delete at 365d).

The script is idempotent: rerunning it detects existing resources and skips creation.

---

## 12. Bet-Outcome Telemetry & Tuning Loop

The only logs the operator queries regularly are **closed-bet outcomes**. Every other log (feed staleness, breaker events, reconciliation noise) is operational — useful for incident response, not parameter tuning. We separate the two.

### 12.1 Bet-outcome record schema

Written at the moment a position is closed (or when Kalshi resolution is received for abandoned positions). One record per bet. Log name: `projects/<project>/logs/bot_btc_1hr_kalshi.bet_outcomes`.

```json
{
  "severity": "INFO",
  "jsonPayload": {
    "event": "bet_closed",
    "correlation_id": "c2b1-9f...",
    "strategy_version": "v1.0.3",
    "session_id": "2026-04-16-15",
    "trap": "floor_bounce",
    "kalshi_market": "BTCD-26APR1600-B75000",
    "side": "YES",
    "strike_usd": 75000,
    "entry_ts": "2026-04-16T15:23:14.892Z",
    "entry_price_cents": 34,
    "position_size_contracts": 50,
    "exit_ts": "2026-04-16T15:54:31.201Z",
    "exit_price_cents": 91,
    "exit_reason": "early_cashout_99",
    "hold_duration_sec": 1876,
    "realized_pnl_usd": 28.50,
    "fees_usd": 0.75,
    "net_pnl_usd": 27.75,
    "features_at_entry": {
      "regime_trend": "neutral",
      "regime_vol": "expanding",
      "rsi_5m": 31.2,
      "bb_width_pct": 0.82,
      "ofi_30s": 0.35,
      "buffer_usd": 62.00,
      "signal_confidence": 0.72
    },
    "features_at_exit": {
      "rsi_5m": 48.5,
      "buffer_usd": 91.00,
      "p_settle": 0.94
    },
    "sizing_inputs": {
      "base_pct": 0.02,
      "vol_multiplier": 1.4,
      "kelly_fraction_half": 0.038,
      "final_pct": 0.028
    }
  }
}
```

`exit_reason` is the tuning loop's primary dimension:
- `theta_net_target` — hit normal target
- `early_cashout_99` — §7.4a.B
- `soft_stop` — §7.4a.C
- `dynamic_theta_45` / `dynamic_theta_50` / `dynamic_theta_55` — late-hour forced target
- `ioc_rung_1` / `ioc_rung_2` / `ioc_rung_3` — escalation rungs
- `abandoned_to_settlement` — §7.3.3
- `settled_win` / `settled_loss` — resolution of abandoned position
- `tier1_flatten` — macro override triggered
- `reconciliation_halt` — forced close during incident

### 12.2 Tuning loop — reaching convergence

The goal: use 5 days of outcome logs to **converge on parameter settings** that maximize risk-adjusted PnL. This is not ML — it's deliberate, weekly, human-driven parameter adjustment informed by data.

Weekly procedure (see `docs/RUNBOOK.md` for exact queries):

1. **Per-trap hit rate and EV.** If a trap's hit rate < 48% or EV < 0 over 5 days, either disable it or tighten its preconditions.
2. **Per-exit-reason distribution.** If `soft_stop` fires > 20% of trades, `SOFT_STOP_FRACTION` may be too tight. If `abandoned_to_settlement` resolves as `settled_loss` more than `settled_win`, the `p_settle > 0.60` abandon threshold is too aggressive — raise it.
3. **Regime conditional PnL.** Group by `features_at_entry.regime_vol`. If chaotic-vol trades lose money net, disable traps in that regime.
4. **Early-cashout EV check.** Compare `early_cashout_99` realized PnL against the theoretical PnL if we had held to theta target. If holding would have produced materially more (accounting for capital lock-up cost), raise the threshold to 97¢ or 95¢.
5. **Book-depth deterioration validation.** Compare trades where the XX:48 pull-forward fired to those where it didn't. Confirm the mechanism reduces late-hour slippage.

Parameter changes go into `config/params.yaml`, versioned in git. A PR labeled `param-tune` references the BigQuery query IDs that motivated the change. This creates an audit trail: every parameter change has a data-driven rationale.

### 12.3 Log retention & cleanup

- `bot_btc_1hr_kalshi.bet_outcomes` log → Cloud Logging bucket `bot-btc-1hr-kalshi-bets-5d` (5-day retention, auto-expiry) **AND** BigQuery table `bot_btc_1hr_kalshi_bet_outcomes.outcomes` (5-day partition expiration).
- All other `bot_btc_1hr_kalshi.*` logs → default `_Default` bucket (30-day GCP default).
- **No manual cleanup required.** Cloud Logging bucket retention and BigQuery partition expiration enforce cleanup automatically.
- If 5 days proves insufficient for tuning (needing month-over-month trends), a second sink can archive to GCS Parquet for cheap cold storage without violating the "hot data = 5 days" principle.

### 12.4 Querying

Two paths, both documented in `docs/RUNBOOK.md`:

- **Logs Explorer (GCP console):** natural language-ish filter like `logName:"bot_btc_1hr_kalshi.bet_outcomes" AND jsonPayload.trap="floor_bounce"`. Fast, no setup. Best for one-off investigations.
- **BigQuery (recommended for tuning):** SQL over the 5-day partitioned table. Examples:

  ```sql
  -- Hit rate by trap, last 5 days
  SELECT trap, COUNT(*) n,
         AVG(CAST(net_pnl_usd > 0 AS INT64)) hit_rate,
         SUM(net_pnl_usd) total_pnl,
         AVG(hold_duration_sec) avg_hold_sec
  FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
  WHERE exit_reason NOT IN ('abandoned_to_settlement')
  GROUP BY trap;
  ```

  ```sql
  -- Exit reason distribution
  SELECT exit_reason, COUNT(*) n, SUM(net_pnl_usd) total_pnl
  FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
  GROUP BY exit_reason
  ORDER BY n DESC;
  ```

---

## 13. Appendix A — Regime × Trap Compatibility Matrix

| Regime trend    | Vol state   | Trap 1 (Lag) | Trap 2 (Floor) | Trap 3 (Ceiling) |
| --------------- | ----------- | ------------ | -------------- | ---------------- |
| strong-down     | any         | ✅           | ⚠️ reduce size | ✅               |
| down            | normal/expanding | ✅      | ✅             | ✅               |
| neutral         | compressed  | ❌ no-trade  | ⚠️ reduce size | ⚠️ reduce size   |
| neutral         | normal      | ✅           | ✅             | ✅               |
| neutral         | expanding   | ✅           | ✅             | ✅               |
| neutral         | chaotic     | ✅           | ❌             | ❌               |
| up              | normal/expanding | ✅      | ✅             | ✅               |
| strong-up       | any         | ✅           | ✅             | ❌ Lockout       |

"⚠️ reduce size" = 50% of baseline allocation.

---

## 14. Appendix B — Dependencies

Runtime:
- `python>=3.12`
- `uvloop`, `aiohttp`, `websockets` — async I/O
- `numpy`, `numba`, `pandas`, `pyarrow` — numerics and Parquet
- `pydantic` — typed config and message schemas
- `structlog` — logging
- `prometheus_client` — metrics

Dev:
- `pytest`, `pytest-asyncio`, `hypothesis` — testing
- `mypy`, `ruff`, `black` — static analysis / style
- `pre-commit` — enforce checks locally

Any new runtime dependency requires:
1. License review (permit only: MIT, Apache-2.0, BSD).
2. Update to this appendix.
3. Addition to `pyproject.toml` with pinned minor version.

---

## 15. Appendix C — Open Questions / Future Work

- **Dealer positioning model:** use Kalshi maker flow to infer dealer inventory and bias strike selection.
- **Cross-market hedging:** for large positions, hedge residual delta via perpetuals (adds execution complexity + margin considerations).
- **Bayesian regime model:** replace hardcoded bucketing with HMM / changepoint detection.
- **Latency arb productionization:** only if we can co-locate or get FPGA-grade feed handlers — not in scope for v1.
- **Multi-asset extension:** ETH hourlies share microstructure traits; reuse most of the stack.

---

## 16. Change log

| Date       | Version | Author | Change |
| ---------- | ------- | ------ | ------ |
| 2026-04-16 | 1.0     | Shrey  | Initial spec. Incorporates 5 critical fixes: IOC exits, flatten-on-Tier1, dynamic theta target, lag-arb demotion, fractional-Kelly sizing. |
| 2026-04-16 | 1.1     | Shrey  | Architecture review patches: (§4.1) replace NLP news with structured calendar + human kill-switch; (§4.2.1) sequence-gap detector invalidates all book-derived features on taint; (§7.2) strip maker-rebate optimization, keep queue model, move fee schedule to config; (§7.3) split into book-depth deterioration metric + IOC escalation ladder + Abandon-to-Settlement terminal decision tree; (§7.4a) active position monitor with 99% early cash-out and relaxed adaptive soft stop; (§11) GCP serverless deployment on Cloud Run; (§12) bet-outcome telemetry to BigQuery with 5-day retention + tuning loop procedure. |
