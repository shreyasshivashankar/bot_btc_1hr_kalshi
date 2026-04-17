# bot_btc_1hr_kalshi — BTC 1H Kalshi Market Maker

Event-driven trading agent targeting Kalshi's BTC-hourly prediction markets. Edge hypothesis: **structural mean-reversion at Bollinger extremes** combined with **opportunistic cross-venue price lag**, sized by a **fractional-Kelly risk engine** and clipped by hard circuit breakers.

The full specification lives in `docs/DESIGN.md` — read it before modifying strategy, risk, or execution code. `docs/RUNBOOK.md` covers ops. `docs/RISK.md` is the risk committee policy (changes require sign-off). `docs/DEPLOYMENT.md` covers GCP provisioning and the start/stop model.

**Runtime:** GCP Cloud Run (serverless, no VM to manage). `min-instances=max-instances=1`, CPU always allocated. Secrets via Secret Manager, env vars via Cloud Run console or `deploy/env.yaml` import. See `docs/DESIGN.md` §11.

## Project layout

```
src/bot_btc_1hr_kalshi/
  market_data/   Feed handlers (Kalshi WS, Coinbase/Binance spot), L2 book, RTI, seq-gap detector
  signal/        Regime detection, traps (floor/ceiling/lag), feature engineering
  risk/          Position sizing (frac-Kelly × ATR), circuit breakers, VaR, exposure caps
  execution/     OMS, smart order router, IOC escalation ladder, settlement_prob, reconciliation
  portfolio/     Positions, PnL attribution, Greeks (theta decay), daily margin state
  monitor/       PositionMonitor (always-on): early cash-out (99¢), adaptive soft stop, theta net
  research/      Backtest engine (tick replay), walk-forward, param sweeps, shadow mode
  obs/           Structured logs, bet-outcome telemetry, admin audit, metrics
  admin/         HTTP endpoints (/healthz, /readyz, /admin/{halt,resume,flatten,tier1_override,status})
  config/        YAML params per env (dev/paper/prod); secrets via env vars only
  calendar/      Structured economic calendar poller (scheduled pre-emptive flatten)

deploy/
  Dockerfile
  cloudrun.yaml         Service manifest (min=max=1, CPU always allocated)
  env.example.yaml      Non-secret env vars (importable via gcloud)
  setup_gcp.sh          One-time infra bootstrap (APIs, secrets, log bucket, BQ sink)

scripts/
  start.sh stop.sh status.sh      # container lifecycle (Cloud Run min-instances)
  halt.sh resume.sh flatten.sh    # trading logic control (HTTP admin)
  tier1_override.sh               # human kill-switch
  query_bets.sh                   # canned BigQuery queries from RUNBOOK
```

## Commands

```
make test           # unit + integration (no network)
make replay         # replay captured tick data through live engine
make backtest       # walk-forward backtest, prints Sharpe / maxDD / hit rate
make paper          # live market data, simulated fills, no real orders
make live           # PRODUCTION — requires RISK_COMMITTEE_SIGNED=yes
make reconcile      # reconcile local OMS state vs Kalshi broker state
```

## Hard rules — do NOT violate without risk sign-off

1. **Never cross the spread on entry.** Maker-only limit orders. Exits may cross via IOC.
2. **Never deploy untested code to `live`.** Must pass: backtest → paper (≥48h) → shadow (≥24h) → live.
3. **Never bypass the drawdown freeze.** 15% single-trade loss = 60min API lockout. No override.
4. **Never hardcode secrets.** Kalshi keys load from Secret Manager (`BOT_BTC_1HR_KALSHI_API_KEY`, `BOT_BTC_1HR_KALSHI_API_SECRET`). Admin endpoints require `BOT_BTC_1HR_KALSHI_ADMIN_TOKEN`.
5. **Never use `datetime.now()` in trading logic.** Always pass the clock injected via the event loop — backtests need deterministic time.
6. **Every order decision must emit a `DecisionRecord`** (trap, features, sizing inputs, expected edge). Every closed bet must emit a `BetOutcome` record to `bot_btc_1hr_kalshi.bet_outcomes`. No silent trades.
7. **Position state is authoritative from the broker, not local memory.** Reconcile every 60s; halt on mismatch >1 contract.
8. **Tier 1 news override → flatten the book (winners AND losers).** No PnL-conditional liquidation. No NLP triggers — only structured calendar (pre-emptive at T−60s) or human kill-switch.
9. **Never invalidate book-derived features silently on a sequence gap.** On WS seq gap, mark features `INVALID` until REST snapshot rebuilds the book.
10. **Never scale container to zero with open positions.** `stop.sh` refuses; flatten first via `scripts/flatten.sh`.

## Non-negotiable invariants enforced in code

- `risk.check()` must gate every order submission. It is a pure function returning `Approve | Reject(reason)`.
- `execution.submit()` returns only after the broker acknowledges — no fire-and-forget.
- Clock drift >250ms vs NTP → halt.
- Market data staleness >2s on primary feed → halt (fail-over to secondary feed, re-validate).
- `PositionMonitor` evaluates every open position on every Kalshi tick with priority: early-cashout (≥99¢) > theta-net-target > adaptive-soft-stop. Only one exit order in flight per position.
- Every `BetOutcome` log record is schema-validated via pydantic before emit — drift between code and BigQuery table breaks tuning queries.

## Style

- Python 3.12+, type-checked with `mypy --strict` in `src/`. `ruff` for lint.
- `asyncio` for I/O; `numpy`/`numba` for hot paths (book walking, VWAP).
- Every module has a unit test. Every trap has a replay test against a canned scenario.
- No new dependencies without updating `docs/DESIGN.md` § Appendix B (Dependencies).

## Slice 5 — TODO (not implemented)

Research tooling is **deferred**. The current codebase ships Slices 1-4 (feeds, signal/risk/execution, calendar, ops, Cloud Run deploy). `src/bot_btc_1hr_kalshi/research/replay.py` exists as a minimal tick-replay orchestrator used by unit tests, but the full research layer is not built yet.

Still to do under Slice 5:
- **Backtest engine**: tick-by-tick replay harness producing Sharpe / maxDD / hit-rate / per-trap PnL attribution. `make backtest` is a placeholder.
- **Walk-forward param sweeps**: train/validate splits over the captured tick archive (`gs://bot-btc-1hr-kalshi-tick-archive-*`), with deterministic seeds.
- **Shadow mode**: runs the live decision path against live market data but routes orders to a no-op broker; emits DecisionRecords only. Required by hard rule #2 (≥24h shadow before `make live`).
- **Param config surface**: YAML-driven sweep definitions + a results table in BigQuery alongside `bet_outcomes`.

Do not add any of these without reading `docs/DESIGN.md` §10 (Research & Validation) first — the walk-forward methodology is load-bearing for the risk-committee sign-off path.
