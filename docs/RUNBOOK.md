# bot_btc_1hr_kalshi — Operational Runbook

Every paging alert must have a runbook entry here. Keep entries short: symptom, check, fix, escalate.

---

## Quick start: launch in paper mode on Cloud Run

Paper mode = live Kalshi + spot market data, **simulated fills against the local L2 book**, no real orders at the exchange. Every decision (approved or rejected) and every simulated closed bet is emitted as a structured log record and mirrored to BigQuery via the logging sink — those are the rows the weekly parameter-tuning query reads.

One-time setup (first deploy only):
```bash
./deploy/setup_gcp.sh          # enables APIs, creates Secret Manager entries, BQ dataset + sink, log bucket
```

Set secrets + non-secret env:
```bash
# Secrets — via Secret Manager; referenced by Cloud Run at runtime.
# PRIVATE_KEY is stored and mounted as a file (PEM), not an env var.
gcloud secrets versions add BOT_BTC_1HR_KALSHI_ADMIN_TOKEN --data-file=<(openssl rand -hex 32)
gcloud secrets versions add BOT_BTC_1HR_KALSHI_API_KEY     --data-file=<(echo -n "$KALSHI_KEY_ID")
gcloud secrets versions add BOT_BTC_1HR_KALSHI_PRIVATE_KEY --data-file=kalshi_private_key.pem

# Non-secrets — imported from deploy/env.yaml
gcloud run services update bot-btc-1hr-kalshi \
  --env-vars-file=deploy/env.yaml
```

Deploy + start:
```bash
gcloud run deploy bot-btc-1hr-kalshi \
  --source=. \
  --region="${BOT_BTC_1HR_KALSHI_GCP_REGION:-us-central1}" \
  --set-env-vars=BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH=/secrets/kalshi/kalshi-private-key \
  --set-secrets=BOT_BTC_1HR_KALSHI_ADMIN_TOKEN=BOT_BTC_1HR_KALSHI_ADMIN_TOKEN:latest \
  --set-secrets=BOT_BTC_1HR_KALSHI_API_KEY=BOT_BTC_1HR_KALSHI_API_KEY:latest \
  --set-secrets=/secrets/kalshi/kalshi-private-key=BOT_BTC_1HR_KALSHI_PRIVATE_KEY:latest \
  --args="--mode,paper,--bankroll,50"
# Note: each container start resets session bankroll to the --bankroll value.
# Stop + start (or redeploy) to begin a new $50 session.
./scripts/start.sh               # scales min=max=1 (idempotent if already running)
```

Verify it's trading:
```bash
./scripts/status.sh              # should show mode=paper, trading_halted=false, activity.seconds_since_last_tick < 5
./scripts/view_logs.sh stream    # live-tails logs; look for "decision" and "market_discovery.selected"
```

Toggle on/off without redeploy:
```bash
./scripts/halt.sh                # stop new entries; existing positions still monitored
./scripts/resume.sh              # resume entries
./scripts/stop.sh                # scale container to zero (only if no open positions)
./scripts/start.sh               # scale back up
```

Market discovery is automatic: the feed loop hits `GET /trade-api/v2/markets?series_ticker=KXBTC&status=open` at each hour boundary, picks the market whose expiration is the next top-of-hour, and subscribes to its WS book. Override the series via `BOT_BTC_1HR_KALSHI_SERIES_TICKER` if Kalshi renames it.

### Flipping paper → live

Live mode is identical to paper except `--mode live` and `RISK_COMMITTEE_SIGNED=yes` must be set. Hard rule #2 requires 48h paper + 24h shadow + risk committee sign-off before flipping. See Gate 1-4 below.

---

## Promotion checklist (Dev → Live)

### Gate 1: Backtest
- [ ] Walk-forward 90d/14d over most recent 12 months: Sharpe ≥ 0.8, max DD ≤ 20%, hit rate ≥ 48%
- [ ] Passes stress suite (top-5 historical worst hours)
- [ ] Decision-journal diff vs prior release is reviewed and expected
- [ ] Determinism test green (bit-for-bit replay)

### Gate 2: Paper (≥48h)
- [ ] Live market data, simulated fills, no real orders
- [ ] PnL within 10% of concurrent backtest PnL
- [ ] Zero unhandled exceptions in logs
- [ ] No breaker false-positives

### Gate 3: Shadow (≥24h)
- [ ] Decision divergence vs prod < 5% by notional
- [ ] Latency metrics within SLO
- [ ] Reconciliation clean

### Gate 4: Live (capped 1 week)
- [ ] Start at 25% of normal size
- [ ] Daily review each of first 5 days
- [ ] Scale to full size only after a clean week

---

## Incident: Primary feed down

**Symptom:** `bot_btc_1hr_kalshi_feed_staleness_ms{feed="coinbase"}` > 2000 for >10s.

**Auto-response:** system fails over to secondary, re-validates book with REST snapshot.

**Check:**
1. Curl the feed health endpoint.
2. Check venue status page.
3. Inspect `logs/market_data.json` for disconnect reason.

**Fix:** let auto fail-over handle it. If both primary and secondary fail → auto-halt+flatten; page operator.

---

## Incident: Reconciliation mismatch

**Symptom:** `bot_btc_1hr_kalshi_reconciliation_mismatch > 0`, halt triggered.

**DO NOT** attempt automatic reconciliation of mismatches >1 contract.

**Check:**
1. Pull broker position from Kalshi REST manually.
2. Compare to local OMS snapshot (`data/oms/YYYY-MM-DD.parquet`).
3. Walk order journal for the hour — identify the divergence event.

**Fix:**
1. If broker is source of truth and difference is benign (e.g., partial fill not acked): update local OMS from broker, resume.
2. If we cannot explain the difference: do not resume trading. Write post-mortem before restart.

---

## Incident: Drawdown Freeze

**Symptom:** single trade closed at >15% daily margin loss. API locked 60min.

**This is working as intended.** Do NOT override the freeze.

**During the freeze:**
1. Write incident note in `docs/incidents/YYYY-MM-DD-drawdown.md`.
2. Identify which trap fired, capture the decision journal entry.
3. Flag for review at end of session.

**After freeze:** resume only if RCA complete. If cause is strategy-level (not execution), consider disabling that trap until fixed.

---

## Incident: Clock drift

**Symptom:** `bot_btc_1hr_kalshi_clock_offset_ms > 250`, halt.

**Check:** `chronyc tracking` / `ntpq -p` on the host.

**Fix:**
1. Verify NTP daemon is running.
2. Switch to a backup NTP source if primary is unreachable.
3. Once offset < 100ms stable for 60s, resume.

---

## Incident: Tier 1 news override

**Symptom:** auto-flatten triggered by news classifier.

**Response:**
1. Verify the news event is real (false positives are possible).
2. If false positive: log, improve classifier, resume after stabilization window.
3. If true positive: system is doing its job. Monitor the flattening fills for slippage.

---

## Routine: Daily close checklist

- [ ] Download daily decision journal to S3
- [ ] Verify reconciliation = 0
- [ ] PnL attribution report generated
- [ ] Circuit breaker event log reviewed
- [ ] Update daily margin for tomorrow from ending equity

---

## Routine: Weekly ops

- [ ] Walk-forward re-fit of parameters
- [ ] Rolling Sharpe / hit rate dashboard review
- [ ] Slippage model recalibration from live fills
- [ ] Tick store nightly → S3 sync verified for the week

---

## Routine: Monthly ops

- [ ] Full stress replay on updated scenario library
- [ ] Dependency audit (security advisories)
- [ ] Policy review (`docs/RISK.md` vs realized behavior)

---

## Operator: Start / Stop / Status (GCP Cloud Run)

All commands assume `gcloud` is authenticated to the correct project and `BOT_BTC_1HR_KALSHI_GCP_PROJECT` / `BOT_BTC_1HR_KALSHI_SERVICE_NAME` are set in your shell. See `scripts/*.sh` for the wrapped versions.

### Soft halt (preferred — container keeps running, no new entries)
```bash
./scripts/halt.sh     # POST /admin/halt  (requires BOT_BTC_1HR_KALSHI_ADMIN_TOKEN)
./scripts/resume.sh   # POST /admin/resume
```
Use this for intra-session pauses. Existing positions continue to exit normally. No cold-start penalty when resuming.

### Hard stop (scale container to zero — billing stops)
```bash
./scripts/status.sh   # checks for open positions first
./scripts/stop.sh     # gcloud run services update --min-instances=0 --max-instances=0
./scripts/start.sh    # gcloud run services update --min-instances=1 --max-instances=1
```
**Prerequisite:** zero open positions. `stop.sh` refuses to scale down if `/admin/status` reports any open position. If you must proceed, flatten first:
```bash
./scripts/flatten.sh  # POST /admin/flatten  → IOC escalation ladder
```

### Emergency Tier-1 override (human kill-switch for unscheduled shocks)
```bash
./scripts/tier1_override.sh "reason string"
```
Flattens the book immediately. 2-hour stabilization follows (no new entries). Logs the operator identity and reason to `bot_btc_1hr_kalshi.admin_audit`.

### Status check (no side effects)
```bash
./scripts/status.sh
```
Prints: mode (paper/shadow/live), halt state, open positions, session PnL, circuit breaker states, feed health.

### Viewing logs
```bash
./scripts/view_logs.sh              # last 100 entries, 60m window
./scripts/view_logs.sh bets         # bet_outcome records only
./scripts/view_logs.sh decisions    # per-tick DecisionRecords
./scripts/view_logs.sh errors       # severity >= ERROR
./scripts/view_logs.sh stream       # live tail (Ctrl-C to stop)
./scripts/view_logs.sh bets --minutes 1440   # last 24h of closed trades
```
Long-term closed-trade analysis lives in BigQuery (`scripts/query_bets.sh`); these modes are for the recent Cloud Logging window.

### Log retention / cleanup
Cloud Logging retains entries for 14 days by default (policy set by `deploy/setup_gcp.sh`). To prune on demand (e.g. after a noisy debug run):
```bash
./scripts/prune_logs.sh              # delete entries older than 14 days
./scripts/prune_logs.sh --days 7     # tighter window
./scripts/prune_logs.sh --dry-run    # preview what would be deleted
```
`bet_outcomes` records are mirrored to BigQuery via the logging sink — pruning Cloud Logging does **not** delete the BQ rows, so tuning queries stay intact.

### Tick archive capture (for `make backtest`)

On Cloud Run, the archive directory is backed by a GCS FUSE mount declared in `deploy/cloudrun.yaml` — the `tick-archive` volume mounts bucket `bot-btc-1hr-kalshi-tick-archive-$PROJECT_ID` at `/app/data/archive`, and `BOT_BTC_1HR_KALSHI_ARCHIVE_DIR` points at that path. `ArchiveWriter` appends hour-partitioned JSONL files (`events-YYYY-MM-DDTHH.jsonl`) directly into the bucket; no rsync cron, no local scratch directory, no hand-off step. Each hour-roll closes the prior file, which finalizes the GCS object — that is the only mid-run persistence checkpoint, so a hard crash (SIGKILL / instance rotation) loses up to the in-flight hour of ticks. Graceful SIGTERM runs through `serve()`'s finally block and finalizes the current file too.

Prerequisites (one-time, enforced in `deploy/setup_gcp.sh`):
- Bucket `bot-btc-1hr-kalshi-tick-archive-$PROJECT_ID` exists.
- Runtime service account has `roles/storage.objectAdmin` on that bucket (FUSE needs both read and write — `objectViewer` + `objectCreator` won't cover overwrite on append-within-hour).

Locally, point `BOT_BTC_1HR_KALSHI_ARCHIVE_DIR` at any writable directory; the writer has no FUSE-specific code paths.

Once you have ≥2 weeks of captured ticks, run backtests:

```bash
MARKET=KBTC-26APR1600-B60000 STRIKE_USD=60000 \
  FROM=2026-04-01T00 TO=2026-04-15T00 \
  make backtest
```

`make backtest` prints Sharpe / maxDD / hit-rate / per-trap PnL from the captured stream.

### External watchdog (hard rule #3 — no single point of halt)

`/admin/status` returns an `activity` block an external poller uses to detect a wedged event loop:

```json
"activity": {
  "boot_ns": 1713312000000000000,
  "uptime_seconds": 3612.4,
  "last_tick_ns": 1713315612400000000,
  "last_decision_ns": 1713315610100000000,
  "seconds_since_last_tick": 0.03,
  "seconds_since_last_decision": 2.33
}
```

Configure a Cloud Scheduler job every 60 s that `curl`s `/admin/status` with the admin token and pages oncall (or POSTs `/admin/halt`) if:

- `seconds_since_last_tick > 30` during market hours (feed wedged), **or**
- `seconds_since_last_decision > 900` during market hours (decision loop wedged even if feeds tick).

Values are `null` until the first tick/decision of a run — treat null as OK only during the first minute of `uptime_seconds`. The in-process breakers cover feed staleness too (halt at 2 s); the watchdog exists for the case where the in-process code itself is stuck and can't halt itself.

---

## Bet-outcome log queries (for parameter tuning)

All queries run against BigQuery dataset `bot_btc_1hr_kalshi_bet_outcomes.outcomes` (7-day retention). Same data is queryable in **Cloud Logging → Logs Explorer** with filter `logName:"projects/$PROJECT/logs/bot_btc_1hr_kalshi.bet_outcomes"` for one-off investigations.

### Q1 — Hit rate & PnL by trap
```sql
SELECT
  trap,
  COUNT(*) AS n,
  ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) AS hit_rate,
  ROUND(SUM(net_pnl_usd), 2) AS total_pnl_usd,
  ROUND(AVG(net_pnl_usd), 3) AS avg_pnl_per_trade,
  ROUND(AVG(hold_duration_sec), 0) AS avg_hold_sec
FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
WHERE exit_reason != 'abandoned_to_settlement'   -- exclude not-yet-resolved
GROUP BY trap
ORDER BY total_pnl_usd DESC;
```

### Q2 — Exit reason distribution
```sql
SELECT
  exit_reason,
  COUNT(*) AS n,
  ROUND(SUM(net_pnl_usd), 2) AS total_pnl_usd,
  ROUND(AVG(net_pnl_usd), 3) AS avg_pnl
FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
GROUP BY exit_reason
ORDER BY n DESC;
```
Tuning triggers:
- `soft_stop` > 20% of trades → loosen `SOFT_STOP_FRACTION` (raise toward 0.45).
- `ioc_rung_2` or `ioc_rung_3` > 5% → book depth is thinning earlier than XX:48 detects; pull forward sooner.
- `abandoned_to_settlement` with majority `settled_loss` → raise the abandon threshold `p_settle` from 0.60 → 0.65.

### Q3 — Regime-conditioned PnL (find regimes to disable)
```sql
SELECT
  features_at_entry.regime_trend AS trend,
  features_at_entry.regime_vol AS vol,
  COUNT(*) AS n,
  ROUND(SUM(net_pnl_usd), 2) AS total_pnl_usd,
  ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) AS hit_rate
FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
GROUP BY trend, vol
HAVING n >= 10
ORDER BY total_pnl_usd ASC;    -- worst regimes surface first
```
Any (trend, vol) bucket with n ≥ 20 and negative total_pnl_usd over 7 days → disable that cell in the regime×trap matrix (DESIGN §13) for the next session.

### Q4 — Early cash-out EV validation
```sql
-- Compare realized vs would-have-held outcomes.
-- The `held_to_target_pnl_usd` column is synthesized at close time by replaying the post-exit path.
SELECT
  COUNT(*) AS n,
  ROUND(SUM(net_pnl_usd), 2) AS realized_total,
  ROUND(SUM(counterfactual_held_pnl_usd), 2) AS counterfactual_total,
  ROUND(SUM(counterfactual_held_pnl_usd) - SUM(net_pnl_usd), 2) AS missed_upside
FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
WHERE exit_reason = 'early_cashout_99';
```
If `missed_upside` > 30% of realized over 7 days, the threshold is too aggressive — consider raising from 99 to 97.

### Q5 — Confidence score calibration
```sql
SELECT
  ROUND(features_at_entry.signal_confidence, 1) AS conf_bucket,
  COUNT(*) AS n,
  ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) AS realized_hit_rate,
  ROUND(AVG(net_pnl_usd), 3) AS avg_pnl
FROM `bot_btc_1hr_kalshi_bet_outcomes.outcomes`
GROUP BY conf_bucket
ORDER BY conf_bucket;
```
Confidence should monotonically track hit rate. If not, the confidence formula needs reweighting.

---

## Tuning procedure — weekly, data-driven

Run every Monday on prior-week data (rolls with the 7-day retention window):

1. Run Q1–Q5 in BigQuery. Capture results into `docs/tuning/YYYY-MM-DD.md`.
2. Identify the **one** highest-signal change. Never tune more than one parameter per week — you can't attribute PnL deltas to multiple simultaneous changes.
3. Open a PR labeled `param-tune`. PR body must reference the BigQuery query IDs that motivated the change. See `docs/DESIGN.md` §12.2.
4. After merge, deploy via `gcloud run services update --env-vars-file=deploy/env.yaml`. Cloud Run auto-creates a new revision; traffic shifts immediately.
5. Monitor the next 48h for regression. If the change degrades realized Sharpe, revert via `gcloud run services update-traffic --to-revisions=<prev-revision>=100` — instant rollback, no redeploy.

Convergence criterion: **no parameter has moved by >5% for 4 consecutive weeks**. At that point, assume we are at a local optimum; tuning cadence relaxes to monthly.

---

## Incident: Book feed sequence gap

**Symptom:** `bot_btc_1hr_kalshi.market_data` log shows `seq_gap_detected feed=coinbase gap=47`.

**Auto-response:** feature store marks `coinbase.*` features as `INVALID`. Feed handler unsubscribes, fetches REST snapshot, rebuilds book, resumes.

**Check:**
1. Is recovery SLO met? Logs Explorer: `jsonPayload.event="feed_recovery_complete"` — duration should be < 1.5s p95.
2. Is this venue flapping? Query:
   ```
   logName:"bot_btc_1hr_kalshi.market_data" jsonPayload.event="seq_gap_detected" jsonPayload.feed="coinbase"
   ```
   If ≥ 3 in 5 minutes → feed is quarantined for 10 minutes (auto).

**Fix:**
- Single gap: no action needed.
- Repeated gaps across multiple venues same time → likely network issue on the Cloud Run instance; `scripts/status.sh` to verify egress, escalate to GCP support if persistent.

---

## Incident: Abandoned-to-Settlement

**Symptom:** bet-outcome log shows `exit_reason="abandoned_to_settlement"`.

**Response:**
1. Check `p_settle` at abandonment in the log record — confirm it was ≥ 0.60 (correct path) or in the 0.40–0.60 default zone.
2. Margin is locked until Kalshi resolution. Do NOT attempt to force-close the position manually — doing so at midnight liquidity is strictly worse than waiting for resolution.
3. When resolution arrives, a follow-up `settled_win` or `settled_loss` record closes the correlation chain. Reconcile PnL the next session open.

**Limit:** if ≥ 2 abandoned positions are outstanding simultaneously, the bot auto-halts for end-of-session review. See `docs/RISK.md` §3.3.

---

## Incident: Admin endpoint 403

**Symptom:** `halt.sh` / `status.sh` returns 403.

**Check:**
1. `gcloud run services get-iam-policy bot-btc-1hr-kalshi` — your principal must have `roles/run.invoker`.
2. `echo $BOT_BTC_1HR_KALSHI_ADMIN_TOKEN` — bearer token must match Secret Manager value.
3. Logs: `logName:"bot_btc_1hr_kalshi.admin_audit"` — does the request appear? If yes with 403, token mismatch. If no, IAM denial.

**Fix:** rotate token via `gcloud secrets versions add BOT_BTC_1HR_KALSHI_ADMIN_TOKEN --data-file=-` and redeploy the service to pick it up.
