# bot_btc_1hr_kalshi — GCP Deployment Guide

End-to-end setup for running bot_btc_1hr_kalshi on Google Cloud Platform using only managed services. No VMs, no Kubernetes, no patching.

Prerequisite knowledge: basic `gcloud` usage, a billing-enabled GCP project, and either the `gcloud` CLI installed locally or access to the Cloud Shell.

---

## 0. One-time: Set project and region

```bash
export BOT_BTC_1HR_KALSHI_GCP_PROJECT="your-project-id"
export BOT_BTC_1HR_KALSHI_GCP_REGION="us-central1"          # or us-east1, europe-west1, etc.
export BOT_BTC_1HR_KALSHI_SERVICE_NAME="bot-btc-1hr-kalshi"

gcloud config set project $BOT_BTC_1HR_KALSHI_GCP_PROJECT
gcloud config set run/region $BOT_BTC_1HR_KALSHI_GCP_REGION
```

Region choice: pick the closest to Kalshi's API ingress. Kalshi's endpoints are US-based, so `us-east1` or `us-east4` minimize WS round-trip. US-central is a reasonable default.

---

## 1. Run the bootstrap script

All infra (APIs, IAM, secrets, log buckets, BigQuery dataset, GCS bucket) is created by one idempotent script. Run it and answer the prompts:

```bash
./deploy/setup_gcp.sh
```

What it does (in order):

1. Enables required APIs: `run`, `secretmanager`, `logging`, `bigquery`, `cloudscheduler`, `artifactregistry`, `cloudbuild`.
2. Creates a dedicated runtime service account `bot-btc-1hr-kalshi-runtime@$PROJECT.iam.gserviceaccount.com` with minimum-scope roles:
   - `roles/secretmanager.secretAccessor` (read secrets at boot)
   - `roles/logging.logWriter` (emit logs)
   - `roles/bigquery.dataEditor` (bet-outcome log routing)
   - `roles/storage.objectAdmin` (tick-archive bucket only, conditional IAM)
3. Creates secrets in Secret Manager (you'll paste values interactively):
   - `BOT_BTC_1HR_KALSHI_API_KEY` — Kalshi key id (UUID)
   - `BOT_BTC_1HR_KALSHI_PRIVATE_KEY` — Kalshi RSA private key (PEM). Mounted as a **file**, not an env var (see §Secret mount below).
   - `BOT_BTC_1HR_KALSHI_ADMIN_TOKEN` (generated with `openssl rand -hex 32` if you press enter)
4. Sets the project's `_Default` log bucket to **7-day retention** (application / operational logs auto-expire).
5. Creates a **7-day retention log bucket** `bot-btc-1hr-kalshi-bets-7d` in your region for bet-outcome records.
6. Creates the BigQuery dataset `bot_btc_1hr_kalshi_bet_outcomes` with `default_partition_expiration=604800` (7 days in seconds).
7. Creates a log sink `bot-btc-1hr-kalshi-bet-outcomes-sink` routing `logName="projects/$PROJECT/logs/bot_btc_1hr_kalshi.bet_outcomes"` to:
   - The `bot-btc-1hr-kalshi-bets-7d` bucket (Logs Explorer)
   - The `bot_btc_1hr_kalshi_bet_outcomes.outcomes` BigQuery table (SQL queries)
8. Adds an exclusion to the default `_Default` sink so bet-outcome logs aren't double-stored.
9. Creates Artifact Registry repo `bot-btc-1hr-kalshi` for container images.
10. Creates GCS bucket `bot-btc-1hr-kalshi-tick-archive-$PROJECT` (tick archive) with lifecycle: COLDLINE at 30d, delete at 365d.
11. Creates a **monthly $80 budget** on the project's billing account with email alerts at 50%, 90%, and 100%.

The script prints next steps on success. Rerun is safe.

---

## 2. Build & deploy the container

### 2.1 Build via Cloud Build (no local Docker needed)

```bash
gcloud builds submit \
  --config cloudbuild.yaml \
  --substitutions _REGION=$BOT_BTC_1HR_KALSHI_GCP_REGION \
  --timeout=20m \
  .
```

The `--tag` shortcut does not work here because `deploy/Dockerfile` is not at the build-context root; `cloudbuild.yaml` passes `-f deploy/Dockerfile` explicitly. First build ~5 min; subsequent builds are cached.

### 2.2 Review env vars

Copy and edit the template:

```bash
cp deploy/env.example.yaml deploy/env.yaml
# edit deploy/env.yaml — set BOT_BTC_1HR_KALSHI_MODE=paper to start
```

Review every variable. `BOT_BTC_1HR_KALSHI_MODE=paper` is the default; flipping to `live` requires the promotion gates in RUNBOOK (backtest → paper 48h → shadow 24h → live).

### 2.3 Deploy the Cloud Run service

```bash
gcloud run deploy $BOT_BTC_1HR_KALSHI_SERVICE_NAME \
  --image="$BOT_BTC_1HR_KALSHI_GCP_REGION-docker.pkg.dev/$BOT_BTC_1HR_KALSHI_GCP_PROJECT/bot-btc-1hr-kalshi/bot-btc-1hr-kalshi:latest" \
  --service-account="bot-btc-1hr-kalshi-runtime@$BOT_BTC_1HR_KALSHI_GCP_PROJECT.iam.gserviceaccount.com" \
  --region=$BOT_BTC_1HR_KALSHI_GCP_REGION \
  --platform=managed \
  --min-instances=1 \
  --max-instances=1 \
  --cpu=2 \
  --memory=2Gi \
  --cpu-boost \
  --execution-environment=gen2 \
  --no-cpu-throttling \
  --ingress=all \
  --no-allow-unauthenticated \
  --env-vars-file=deploy/env.yaml \
  --set-env-vars="BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH=/secrets/kalshi/kalshi-private-key" \
  --set-secrets="BOT_BTC_1HR_KALSHI_API_KEY=BOT_BTC_1HR_KALSHI_API_KEY:latest,BOT_BTC_1HR_KALSHI_ADMIN_TOKEN=BOT_BTC_1HR_KALSHI_ADMIN_TOKEN:latest,/secrets/kalshi/kalshi-private-key=BOT_BTC_1HR_KALSHI_PRIVATE_KEY:latest" \
  --timeout=3600
```

**Secret mount — why the PEM is a file, not an env var.** `--set-secrets` accepts two forms:

- `NAME=SECRET:VERSION` → exposes the secret value as an env var named `NAME`.
- `/path/to/file=SECRET:VERSION` → mounts the secret value as a file at that path (read-only, `0400`).

Short values (UUIDs, tokens) stay env vars. The RSA private key is multi-line PEM; exporting it through `set -x` or YAML quoting can silently drop newlines, and although OpenSSL PEM parsers tolerate whitespace-mashed base64, the contract is cleaner when the raw bytes are persisted in a file. Locally we use the same contract: `BOT_BTC_1HR_KALSHI_PRIVATE_KEY_PATH=/path/to/your.pem`. One loader, two environments.

Flags explained:
- `min=max=1`: exactly one instance, always running. Trading state is not horizontally scalable.
- `--no-cpu-throttling`: CPU is always allocated, not just during HTTP requests. This is what keeps the event loop alive for WS consumption.
- `--ingress=all` + `--no-allow-unauthenticated`: zero-trust model — the URL is publicly resolvable but the Google Front End rejects any request without a Google-signed OIDC token from a principal holding `roles/run.invoker` (403 before the request reaches the container). The app then requires `X-Admin-Token` on top for defense-in-depth. Tightening to `internal-*` would only add value behind a VPC/GCLB front-end; without one, it just breaks ops tooling without a real security gain.
- `--timeout=3600`: max HTTP request time; trading loop uses WS, not HTTP, so this only affects admin endpoints.
- `--set-secrets`: mounts Secret Manager values as env vars at container start.

**Tick archive — GCS FUSE mount.** `deploy/cloudrun.yaml` declares a `tick-archive` volume backed by the `gcsfuse.run.googleapis.com` CSI driver, mounting `bot-btc-1hr-kalshi-tick-archive-$PROJECT_ID` at `/app/data/archive`. `BOT_BTC_1HR_KALSHI_ARCHIVE_DIR` is set to that path, and `ArchiveWriter` writes hour-partitioned JSONL files (`events-YYYY-MM-DDTHH.jsonl`) directly into the bucket — no rsync cron, no local scratch dir. Each hour-roll closes the current file, finalizing the underlying GCS object; the writer does **not** flush per line because on a FUSE mount that either no-ops or triggers a full-object rewrite (see `archive/writer.py` module docstring). Two provisioning prerequisites:

1. The runtime service account must have **`roles/storage.objectAdmin`** on the tick-archive bucket — FUSE needs read+write+overwrite (append-within-hour reopens the object). `objectCreator` alone will fail with EACCES after the first roll. `setup_gcp.sh` already provisions this.
2. The FUSE mount options pin `uid=10001,gid=10001` to match the Dockerfile's non-root `bot-btc-1hr-kalshi` user (uid=10001). Without these the inode is root-owned and the app gets EACCES on first write. `implicit-dirs` is also set so `ArchiveWriter`'s `mkdir(parents=True, exist_ok=True)` succeeds — GCS has no real directory inodes.

Because `gcloud run deploy` flags do not include FUSE volume declarations, the first deploy (or any change to volume configuration) must go through `gcloud run services replace` to materialize the CSI volume. `deploy/cloudrun.yaml` uses `${PROJECT_ID}` / `${REGION}` placeholders for portability — resolve them at deploy time:

```bash
# Option A: envsubst (brew install gettext)
PROJECT_ID=$BOT_BTC_1HR_KALSHI_GCP_PROJECT REGION=$BOT_BTC_1HR_KALSHI_GCP_REGION \
  envsubst < deploy/cloudrun.yaml | \
  gcloud run services replace - --region=$BOT_BTC_1HR_KALSHI_GCP_REGION

# Option B: sed (always available)
sed -e "s|\${PROJECT_ID}|$BOT_BTC_1HR_KALSHI_GCP_PROJECT|g" \
    -e "s|\${REGION}|$BOT_BTC_1HR_KALSHI_GCP_REGION|g" \
    deploy/cloudrun.yaml | \
  gcloud run services replace - --region=$BOT_BTC_1HR_KALSHI_GCP_REGION
```

Subsequent env-var-only revisions can use `gcloud run deploy` / Console edits without re-applying the YAML.

**SIGTERM grace — the 10-second contract.** Cloud Run's container-runtime contract states a fixed 10-second SIGTERM→SIGKILL window, and neither `terminationGracePeriodSeconds` nor the `run.googleapis.com/container-shutdown-timeout` annotation is documented as a way to extend it on managed Cloud Run ([YAML reference](https://cloud.google.com/run/docs/reference/yaml/v1), [container contract](https://cloud.google.com/run/docs/container-contract)). `deploy/cloudrun.yaml` sets `terminationGracePeriodSeconds: 600` as a best-effort Knative spec — if Cloud Run ever honors it we get the longer drain; if not, nothing breaks. **The system must be safe to die in 10 seconds.** The OMS `ABANDONED_TO_SETTLEMENT` ledger state covers this: any unfinished IOC exit is recovered on next boot from the broker's authoritative state, and capital stays locked safely in Kalshi's settlement engine.

On successful deploy you'll get a service URL. Save it:

```bash
export BOT_BTC_1HR_KALSHI_SERVICE_URL=$(gcloud run services describe $BOT_BTC_1HR_KALSHI_SERVICE_NAME --region=$BOT_BTC_1HR_KALSHI_GCP_REGION --format='value(status.url)')
```

---

## 3. Grant yourself admin access

```bash
gcloud run services add-iam-policy-binding $BOT_BTC_1HR_KALSHI_SERVICE_NAME \
  --region=$BOT_BTC_1HR_KALSHI_GCP_REGION \
  --member="user:your-email@example.com" \
  --role="roles/run.invoker"
```

Verify:

```bash
./scripts/status.sh
```

Expected output:
```json
{
  "mode": "paper",
  "halt_state": "running",
  "open_positions": 0,
  "session_pnl_usd": 0.00,
  "breakers": {"drawdown": "ok", "clock": "ok", "feeds": "ok"},
  ...
}
```

---

## 4. Updating env vars via the Console (no redeploy)

Non-secret env vars can be edited in the GCP Console without touching code:

1. Cloud Run → services → `bot-btc-1hr-kalshi` → Edit & Deploy New Revision
2. Variables & Secrets tab → Environment Variables
3. Edit in place OR click "Reference Variables" → "Upload YAML" and upload your edited `deploy/env.yaml`
4. Deploy — this creates a new revision and shifts traffic atomically

**Secrets** (Kalshi keys, admin token): rotate via Secret Manager, then either:
- Restart the container (new revision) to pick up new values, or
- Use `gcloud run services update --set-secrets=...` if you're adding a new secret reference.

---

## 5. Start / Stop patterns

There are two independent pause mechanisms. Pick the right one.

### Soft pause — trading logic only (container keeps running)

Use for: end-of-session, high-impact news, temporarily disabling trading without losing WS sessions.

```bash
./scripts/halt.sh      # no new entries; existing positions continue to exit
./scripts/resume.sh    # re-enable entries
```

Cost: unchanged (container still running). Latency to resume: instant.

### Hard stop — container scaled to zero (billing stops)

Use for: overnight shutdowns, extended pauses, project hibernation.

```bash
./scripts/stop.sh       # refuses if any open positions; flatten first via scripts/flatten.sh
./scripts/start.sh      # scale back up; ~20–40s cold start, feeds reconnect, broker reconciled
```

Cost: ~$0 while stopped. Latency to resume: 20–40s cold start.

### Emergency flatten (Tier 1 human kill-switch)

```bash
./scripts/tier1_override.sh "exchange X halted trading"
```

Immediately fires the IOC escalation ladder on all open positions. Logs the reason. Enforces 2h stabilization window (no new entries).

---

## 6. Monitoring

### Quick status
```bash
./scripts/status.sh
```

### Dashboards
In GCP Console:
- **Cloud Run → bot-btc-1hr-kalshi → Metrics**: instance count, CPU, memory, request latency.
- **Cloud Logging → Logs Explorer**: filter `resource.type="cloud_run_revision" resource.labels.service_name="bot-btc-1hr-kalshi"` for live logs.
- **Cloud Logging → Log Analytics**: for SQL-style queries on operational logs.
- **BigQuery → bot_btc_1hr_kalshi_bet_outcomes.outcomes**: bet-outcome tuning queries (see RUNBOOK.md).

### Alerting (recommended)

Create Cloud Monitoring alert policies for:
- Cloud Run instance count < 1 (should never happen with min=1)
- Cloud Run 5xx rate > 1/min
- No `bet_closed` events for > 90 minutes during market hours (bot silently broken)
- `bot_btc_1hr_kalshi_circuit_breaker_state != 0` for any breaker

Alert channels: email or SMS. Pager duties are operator-defined.

---

## 7. Log retention details

| Log name                         | Destination                         | Retention | Cleanup             |
| -------------------------------- | ----------------------------------- | --------- | ------------------- |
| `bot_btc_1hr_kalshi.bet_outcomes`             | `bot-btc-1hr-kalshi-bets-7d` log bucket          | 7 days    | Automatic (bucket)  |
| `bot_btc_1hr_kalshi.bet_outcomes`             | BigQuery `bot_btc_1hr_kalshi_bet_outcomes.outcomes` | 7 days (partition expiry) | Automatic (BQ) |
| `bot_btc_1hr_kalshi.admin_audit`              | `_Default` bucket                   | 7 days    | Automatic (bucket)  |
| `bot_btc_1hr_kalshi.market_data`, `bot_btc_1hr_kalshi.risk`, `bot_btc_1hr_kalshi.execution`, others | `_Default` bucket | 7 days | Automatic |
| Tick archive (JSONL, hour-partitioned) | GCS `bot-btc-1hr-kalshi-tick-archive-$PROJECT` — mounted at `/app/data/archive` via FUSE CSI | 365 days, COLDLINE at 30d | Lifecycle rule |

**Manual cleanup is never required** — GCP enforces retention at both the log bucket and BigQuery partition level. The `_Default` bucket is explicitly capped at 7 days by `setup_gcp.sh` (default would otherwise be 30 days).

If you need longer-than-7-days analysis, route a second sink to GCS as Parquet. Document rationale in a PR — the 7-day hot window is deliberate (forces the tuning loop to care about recent performance and keeps monthly cost inside the $80 budget alert).

---

## 8. Cost estimate (rough)

At `min=max=1, cpu=2, 2Gi, no CPU throttling`:
- Cloud Run: ~$55–75/month **continuous** (24×7, 2-vCPU allocation); **~$23–32/month for 10 hrs/day** — scaling to zero outside trading hours via `scripts/stop.sh` is the big lever. CPU is the dominant line item.
- Cloud Logging: free tier covers <50 GiB ingestion/month. Even with every `decision` record emitted (~1-10/tick × 5 Hz), ingestion stays under the free tier at 10 hrs/day. Beyond the free tier, $0.50/GiB ingested.
- BigQuery: 14-day partitioned storage of outcomes is < 10 MiB total. Storage and slot-hours both well under $1/month.
- Secret Manager: 3 secrets × $0.06 = $0.18/month.
- Artifact Registry: first 0.5 GB free; container image ≈ 300 MB after first push.
- GCS tick archive: BTC L2 + spot at ~3 hops/sec compresses to ~50-150 MiB/day; 10 hrs/day ≈ 2 GiB/month STANDARD + free COLDLINE transition → < $0.50/month.
- Cloud Scheduler (for external watchdog): 1 job at 1-min cadence = free tier.
- Egress (WS + REST to Kalshi/Coinbase/Binance): ~1 GiB/month → negligible.

**10-hours-a-day paper mode total: ~$28–35/month.** 24×7 live mode: ~$60–80/month.

Cut by another ~40% if you set `cpu=1, memory=1Gi` — the bot's hot path fits comfortably. Keep the headroom while in paper mode; revisit once you have 2 weeks of steady-state metrics.

---

## 9. Security notes

- Admin endpoints gated by **two layers**: IAM `roles/run.invoker` + bearer token. Either alone is insufficient.
- `BOT_BTC_1HR_KALSHI_ADMIN_TOKEN` is a random 32-byte hex string; rotate quarterly or after any suspected exposure.
- Secrets never appear in logs, env YAML, or container images.
- Cloud Run service has `--ingress=all --no-allow-unauthenticated`. The URL is publicly resolvable but un-authenticated requests are dropped at the Google Front End with 403 — they never reach the container, never consume CPU. An attacker would need (a) a forged Google-signed OIDC token from an authorized principal AND (b) the `X-Admin-Token` secret. The GFE+IAM layer is cryptographically sufficient against an unauthenticated attacker; the in-app token is defense-in-depth.
- Service account follows least privilege — it cannot read other GCP resources in the project, cannot impersonate users, cannot modify IAM.

---

## 10. Disaster recovery

- **Container crash:** Cloud Run auto-restarts. On boot, the bot reads broker state first (§DESIGN 10), reconstructs local OMS, reconciles, then resumes. No data loss.
- **Region outage:** not handled in v1 (single-region deployment). For multi-region active-passive, would require Kalshi key rotation + state sync to a second region. Out of scope until we hit AUM that justifies the complexity.
- **Accidental `stop.sh` with open positions:** `stop.sh` refuses. If the refusal is bypassed via direct `gcloud` call and positions orphan, the hourly resolution still settles cash — but margin is locked and the bot can't adapt mid-hour. Recovery: `start.sh`, the bot reconciles from broker on startup and picks up where it left off.

---

## 11. What this does NOT set up (intentionally)

- **No Cloud SQL / Firestore.** State lives in memory + Cloud Logging + BigQuery. Persistence beyond a container restart is recovered from Kalshi broker state, not a database.
- **No load balancer.** Single-instance; `ingress=all` with IAM-only auth (see §9). A GCLB+IAP front-end would be required to restrict ingress further; we don't need that for a solo-operator deployment.
- **No CI/CD pipeline.** Deploy is a manual `gcloud run deploy` — this is intentional gating for a trading system. Automate only after promotion gates are scripted and tested.
- **No PubSub/Kafka.** `asyncio.Queue` is sufficient for single-process event flow.
