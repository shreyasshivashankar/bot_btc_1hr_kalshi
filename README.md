# bot_btc_1hr_kalshi

Autonomous trading agent for **Kalshi's BTC hourly prediction markets**. Each market resolves YES/NO on whether BTC settles above a strike at the top of the hour; contracts are capped at $1 and pay $1 on correct resolution.

Status: Slices 1–4 shipped (feeds, signal/risk/execution, calendar, ops, Cloud Run deploy) plus shadow mode (Slice 4F) and Slice 5 research skeletons (backtest metrics, walk-forward splits, divergence comparator — logic tested, full drivers + live tick capture still to wire). See `CLAUDE.md` § Build status.

---

## What it does

- **Subscribes** to the current open Kalshi BTC hourly market (auto-discovered via REST each hour), Coinbase spot, and Binance spot.
- **Builds features** from the L2 book and spot volatility regime (Bollinger bands, RTI lag).
- **Fires trap strategies** (floor / ceiling mean-reversion + opportunistic cross-venue lag-arb).
- **Sizes bets** via fractional-Kelly against the *remaining* session bankroll, clipped by a per-position notional cap.
- **Monitors open positions** on every tick: early cash-out at 99¢ ≫ theta-net-target ≫ adaptive soft-stop.
- **Emits** a `DecisionRecord` for every evaluation and a `BetOutcome` for every closed bet — both structured JSON, mirrored to BigQuery via a Cloud Logging sink (7-day retention) for weekly parameter tuning.

The edge thesis is **structural mean-reversion at Bollinger extremes** plus secondary lag-arb — not direction prediction, not latency. Full rationale in `docs/DESIGN.md` §2.

---

## Runtime

- **Python 3.12 +** / asyncio, numpy/numba hot paths, pydantic-typed config.
- **GCP Cloud Run**, `min=max=1`, CPU always allocated. Secrets via Secret Manager. Application logs + bet outcomes both capped at 7-day retention. Monthly budget alert at $80.
- **One container ↔ one session.** The bot resets bankroll on container start (default `$50`); intra-session it sizes adaptively on remaining bankroll. Stop + start the service to begin a new session.

---

## Operating the bot

**All runtime commands — deploy, start, stop, halt, resume, flatten, view logs, prune logs, run BigQuery tuning queries — live in [`docs/RUNBOOK.md`](docs/RUNBOOK.md).** Start there.

Highlights for quick reference (details in the runbook):

| Task                                  | Script                          |
| ------------------------------------- | ------------------------------- |
| Deploy / boot / scale-to-1            | `./scripts/start.sh`            |
| Scale to zero (only if flat)          | `./scripts/stop.sh`             |
| Current status snapshot               | `./scripts/status.sh`           |
| Pause new entries (keep monitoring)   | `./scripts/halt.sh`             |
| Resume new entries                    | `./scripts/resume.sh`           |
| Flatten the book (Tier-1 kill-switch) | `./scripts/flatten.sh`          |
| Human override                        | `./scripts/tier1_override.sh`   |
| Tail / stream / filter logs           | `./scripts/view_logs.sh [mode]` |
| Prune old logs                        | `./scripts/prune_logs.sh`       |
| Run canned BigQuery tuning queries    | `./scripts/query_bets.sh [q1…]` |

---

## Promotion gates (hard rule — do not skip)

`dev` → backtest → **paper (≥48h)** → **shadow (≥24h)** → risk-committee sign-off → **live**. Checklist in `docs/RUNBOOK.md` (Gates 1–4). `make live` refuses to boot without `RISK_COMMITTEE_SIGNED=yes`.

---

## Project layout

```
src/bot_btc_1hr_kalshi/
  market_data/   Feed handlers (Kalshi WS+REST, Coinbase, Binance), L2 book, RTI, seq-gap detector
  signal/        Regime detection, trap logic, feature engineering
  risk/          Fractional-Kelly sizing, circuit breakers, exposure caps
  execution/     OMS, broker adapters (paper / shadow / live), IOC escalation ladder
  portfolio/     Positions, PnL attribution, daily margin state
  monitor/       PositionMonitor — always-on exit logic
  obs/           Structured logging, bet-outcome schemas, activity tracker
  admin/         HTTP admin surface (/healthz, /readyz, /admin/*)
  config/        Typed pydantic settings; YAML per mode
  calendar/      Structured economic-calendar poller (pre-emptive flatten)
  archive/       Hour-partitioned JSONL tick archive
  research/      Replay harness, backtest metrics, walk-forward splits, divergence comparator (Slice 5 skeletons; drivers + live tick capture pending)

deploy/        Dockerfile, Cloud Run manifest, setup_gcp.sh, env.example.yaml
config/        paper.yaml, prod.yaml
scripts/       Runtime + ops scripts (see table above)
tests/         unit + integration
```

---

## Documentation map

| File                  | Purpose                                                                         |
| --------------------- | ------------------------------------------------------------------------------- |
| `docs/DESIGN.md`      | **Read before changing strategy/risk/execution.** Full system spec, edge thesis. |
| `docs/RUNBOOK.md`     | Day-to-day operations, all CLI commands, BigQuery tuning queries, incidents.    |
| `docs/RISK.md`        | Risk-committee policy. Changes require sign-off.                                |
| `docs/DEPLOYMENT.md`  | GCP provisioning, deploy mechanics, cost breakdown, log retention table.        |
| `CLAUDE.md`           | Non-negotiable invariants (hard rules #1–10) and coding conventions.            |

---

## Hard rules (summary — full list in `CLAUDE.md`)

1. Never cross the spread on entry. Maker-only limit orders.
2. Never deploy untested code to live. Backtest → paper → shadow → live.
3. 15% single-trade loss = 60-minute lockout. No override.
4. Secrets load from Secret Manager only.
5. Never call `datetime.now()` in trading logic — use the injected clock.
6. Every decision emits a `DecisionRecord`; every closed bet emits a `BetOutcome`.
7. Broker state is authoritative. Reconcile every 60s; halt on mismatch.
8. Tier-1 news override flattens everything — winners and losers alike.
9. Seq-gap on WS → mark book-derived features `INVALID` until REST re-snap.
10. Never scale to zero with open positions.
