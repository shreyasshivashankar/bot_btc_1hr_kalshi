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
4. **Never hardcode secrets.** Kalshi key id loads from Secret Manager as env var `BOT_BTC_1HR_KALSHI_API_KEY`; the RSA private key is mounted from Secret Manager as a file and the app reads its path from `BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH` (same pattern locally — env var points at the PEM on disk). Admin endpoints require `BOT_BTC_1HR_KALSHI_ADMIN_TOKEN`.
5. **Never use `datetime.now()` in trading logic.** Always pass the clock injected via the event loop — backtests need deterministic time.
6. **Every order decision must emit a `DecisionRecord`** (trap, features, sizing inputs, expected edge). Every closed bet must emit a `BetOutcome` record to `bot_btc_1hr_kalshi.bet_outcomes`. No silent trades.
7. **Position state is authoritative from the broker, not local memory.** Reconcile every 60s; halt on mismatch >1 contract.
8. **Tier 1 news override → flatten the book (winners AND losers).** No PnL-conditional liquidation. No NLP triggers — only structured calendar (pre-emptive at T−60s) or human kill-switch.
9. **Never invalidate book-derived features silently on a sequence gap.** On WS seq gap, mark features `INVALID` until REST snapshot rebuilds the book.
10. **Never scale container to zero with open positions.** `stop.sh` refuses; flatten first via `scripts/flatten.sh`.

## Non-negotiable invariants enforced in code

- `risk.check()` must gate every order submission. It is a pure function returning `Approve | Reject(reason)`.
- `execution.submit()` returns only after the broker acknowledges — no fire-and-forget.
- Clock drift >1000ms vs Kalshi server time → halt. Probe anchors on the RFC 7231 `Date` header (1-second truncated); `kalshi_date_header_probe` shifts readings by +500 ms to center zero drift on zero, giving a ±500 ms noise floor — the 1000 ms threshold sits comfortably above this and well below Kalshi's ~5 s signed-request tolerance.
- Market data staleness >2s on primary feed → halt (fail-over to secondary feed, re-validate).
- `PositionMonitor` evaluates every open position on every Kalshi tick with priority: early-cashout (≥99¢) > theta-net-target > adaptive-soft-stop. Only one exit order in flight per position.
- Every `BetOutcome` log record is schema-validated via pydantic before emit — drift between code and BigQuery table breaks tuning queries.

## Style

- Python 3.12+, type-checked with `mypy --strict` in `src/`. `ruff` for lint.
- `asyncio` for I/O; `numpy`/`numba` for hot paths (book walking, VWAP).
- Every module has a unit test. Every trap has a replay test against a canned scenario.
- No new dependencies without updating `docs/DESIGN.md` § Appendix B (Dependencies).

## Build status

**Shipped.** Slices 1-4 complete (feeds, signal/risk/execution, calendar, Cloud Run ops), plus:

- **Shadow mode (Slice 4F)** — `ShadowBroker` at `execution/broker/shadow.py`, wired in `__main__._broker_for_mode` under `BOT_BTC_1HR_KALSHI_MODE=shadow` / `make shadow`. Satisfies hard rule #2's shadow gate: runs the full live decision pipeline against real feeds, emits `shadow.submit_intent` events, no orders reach Kalshi.
- **Research skeletons (Slice 5 partial)** — `research/backtest.py` (Sharpe / maxDD / hit-rate math), `research/walkforward.py` (anchored walk-forward splits), `research/divergence.py` (decision-stream comparator), `research/replay.py` (tick orchestrator). The logic is tested; drivers are partial — see below.
- **Tick archive** — writer/reader for hour-partitioned JSONL archive at `src/bot_btc_1hr_kalshi/archive/`; `make backtest` CLI exists in `research/backtest_cli.py`.
- **Kalshi REST broker class** — `execution/broker/kalshi.py` implements the `Broker` protocol (signed orders/cancels/positions); tested via `httpx.MockTransport`.

**Remaining work before `make live` can graduate past the paper/shadow gates:**

- **Live broker wiring in `__main__._broker_for_mode("live")`** — currently raises `NotImplementedError`. Needs `httpx.AsyncClient` + Secret Manager–backed key loading feeding into `KalshiBroker`. The class is ready; the DI wiring is not.
- **Live tick capture into the GCS archive** — without ongoing capture, backtests run on synthetic fixtures only; risk-committee sign-off will want replay against real captured hours.
- **Param-sweep driver** — thread YAML param candidates through `RiskSettings` and emit a results table. Split generator and metrics are ready.
- **Divergence harness entrypoint** — ties `research/divergence.py`'s tested comparator to a live-vs-replay side-by-side run. Comparator ready; harness wiring not.
- **Benchmark attribution** — per-regime, per-time-of-day breakouts in the backtest report. Blocked on captured regime-labeled tick data.

Do not extend these without reading `docs/DESIGN.md` §10 (Research & Validation) first — the walk-forward methodology is load-bearing for the risk-committee sign-off path.
