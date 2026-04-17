#!/usr/bin/env bash
# ============================================================================
# bot_btc_1hr_kalshi — One-time GCP infrastructure bootstrap.
#
# Idempotent: rerunning skips resources that already exist.
#
# Requires: gcloud CLI authenticated, billing enabled on the target project,
# and the following env vars:
#   BOT_BTC_1HR_KALSHI_GCP_PROJECT    - GCP project ID
#   BOT_BTC_1HR_KALSHI_GCP_REGION     - GCP region (default: us-central1)
#   BOT_BTC_1HR_KALSHI_SERVICE_NAME   - Cloud Run service name (default: bot-btc-1hr-kalshi)
# ============================================================================
set -euo pipefail

: "${BOT_BTC_1HR_KALSHI_GCP_PROJECT:?set BOT_BTC_1HR_KALSHI_GCP_PROJECT}"
: "${BOT_BTC_1HR_KALSHI_GCP_REGION:=us-central1}"
: "${BOT_BTC_1HR_KALSHI_SERVICE_NAME:=bot-btc-1hr-kalshi}"

PROJECT="$BOT_BTC_1HR_KALSHI_GCP_PROJECT"
REGION="$BOT_BTC_1HR_KALSHI_GCP_REGION"
SA_NAME="bot-btc-1hr-kalshi-runtime"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
LOG_BUCKET="bot-btc-1hr-kalshi-bets-5d"
BQ_DATASET="bot_btc_1hr_kalshi_bet_outcomes"
BQ_TABLE="outcomes"
TICK_BUCKET="bot-btc-1hr-kalshi-tick-archive-${PROJECT}"
AR_REPO="bot-btc-1hr-kalshi"
BET_OUTCOMES_LOG="bot_btc_1hr_kalshi.bet_outcomes"
BET_SINK_NAME="bot-btc-1hr-kalshi-bet-outcomes-sink"

log() { printf "\033[1;34m[setup]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*"; }

gcloud config set project "$PROJECT" >/dev/null

# ----------------------------------------------------------------------------
# 1. Enable APIs
# ----------------------------------------------------------------------------
log "Enabling required APIs (this can take a few minutes on first run)..."
gcloud services enable \
  run.googleapis.com \
  secretmanager.googleapis.com \
  logging.googleapis.com \
  bigquery.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  monitoring.googleapis.com \
  storage-api.googleapis.com \
  --project="$PROJECT"

# ----------------------------------------------------------------------------
# 2. Service account
# ----------------------------------------------------------------------------
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" &>/dev/null; then
  log "Service account $SA_EMAIL already exists — skipping create."
else
  log "Creating service account $SA_EMAIL ..."
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="bot_btc_1hr_kalshi runtime service account" \
    --project="$PROJECT"
fi

log "Binding minimum-scope roles to $SA_EMAIL ..."
for ROLE in \
    roles/secretmanager.secretAccessor \
    roles/logging.logWriter \
    roles/bigquery.dataEditor \
    roles/bigquery.jobUser \
    roles/monitoring.metricWriter; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --condition=None \
    --quiet >/dev/null
done

# ----------------------------------------------------------------------------
# 3. Secrets (interactive; paste values, Ctrl-D to finish, or enter to generate)
# ----------------------------------------------------------------------------
create_secret_interactive() {
  local name="$1"
  local prompt="$2"
  local autogen="${3:-false}"
  if gcloud secrets describe "$name" --project="$PROJECT" &>/dev/null; then
    log "Secret $name already exists — skipping create. (Use 'gcloud secrets versions add' to rotate.)"
    return
  fi
  log "Creating secret $name ..."
  gcloud secrets create "$name" --replication-policy="automatic" --project="$PROJECT" >/dev/null

  if [ "$autogen" = "true" ]; then
    read -rp "[Press Enter to auto-generate $name, or type value and Enter]: " v
    if [ -z "$v" ]; then
      v="$(openssl rand -hex 32)"
      log "Generated $name (64-char hex)."
    fi
    printf "%s" "$v" | gcloud secrets versions add "$name" --data-file=- --project="$PROJECT" >/dev/null
  else
    echo ""
    echo "  $prompt"
    echo "  (Paste value, then press Ctrl-D on a new line to finish)"
    v="$(cat)"
    printf "%s" "$v" | gcloud secrets versions add "$name" --data-file=- --project="$PROJECT" >/dev/null
  fi

  # Grant runtime SA access to this specific secret.
  gcloud secrets add-iam-policy-binding "$name" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --project="$PROJECT" \
    --quiet >/dev/null
}

create_secret_interactive "BOT_BTC_1HR_KALSHI_API_KEY"    "Paste your Kalshi API KEY:"
create_secret_interactive "BOT_BTC_1HR_KALSHI_API_SECRET" "Paste your Kalshi API SECRET:"
create_secret_interactive "BOT_BTC_1HR_KALSHI_ADMIN_TOKEN"       "Admin bearer token (leave blank to auto-generate):" true

# ----------------------------------------------------------------------------
# 4. Cloud Logging: 5-day log bucket for bet outcomes
# ----------------------------------------------------------------------------
if gcloud logging buckets describe "$LOG_BUCKET" --location="$REGION" --project="$PROJECT" &>/dev/null; then
  log "Log bucket $LOG_BUCKET already exists — skipping create."
else
  log "Creating 5-day log bucket $LOG_BUCKET in $REGION ..."
  gcloud logging buckets create "$LOG_BUCKET" \
    --location="$REGION" \
    --retention-days=5 \
    --project="$PROJECT" \
    --description="bot_btc_1hr_kalshi bet-outcome logs (auto-expire at 5 days)"
fi

# ----------------------------------------------------------------------------
# 5. BigQuery dataset with 5-day partition expiration
# ----------------------------------------------------------------------------
if bq --project_id="$PROJECT" show --format=none "${PROJECT}:${BQ_DATASET}" &>/dev/null; then
  log "BigQuery dataset $BQ_DATASET already exists — skipping create."
else
  log "Creating BigQuery dataset $BQ_DATASET ..."
  # 432000s = 5 days.
  bq --project_id="$PROJECT" mk \
    --location="$REGION" \
    --default_partition_expiration=432000 \
    --description="bot_btc_1hr_kalshi bet outcomes. 5-day partition expiration." \
    "${BQ_DATASET}"
fi

# ----------------------------------------------------------------------------
# 6. Log sink: bot_btc_1hr_kalshi.bet_outcomes → log bucket + BigQuery
# ----------------------------------------------------------------------------
SINK_FILTER="logName=\"projects/${PROJECT}/logs/${BET_OUTCOMES_LOG}\""

if gcloud logging sinks describe "$BET_SINK_NAME" --project="$PROJECT" &>/dev/null; then
  log "Log sink $BET_SINK_NAME already exists — updating filter."
  gcloud logging sinks update "$BET_SINK_NAME" \
    "bigquery.googleapis.com/projects/${PROJECT}/datasets/${BQ_DATASET}" \
    --log-filter="$SINK_FILTER" \
    --project="$PROJECT" \
    --quiet
else
  log "Creating log sink $BET_SINK_NAME → BigQuery ..."
  gcloud logging sinks create "$BET_SINK_NAME" \
    "bigquery.googleapis.com/projects/${PROJECT}/datasets/${BQ_DATASET}" \
    --log-filter="$SINK_FILTER" \
    --project="$PROJECT" \
    --use-partitioned-tables

  # Grant sink writer identity permission to insert into the dataset.
  SINK_WRITER="$(gcloud logging sinks describe "$BET_SINK_NAME" --project="$PROJECT" --format='value(writerIdentity)')"
  log "Granting sink writer $SINK_WRITER BigQuery Data Editor on dataset."
  bq --project_id="$PROJECT" update \
    --source <(bq --project_id="$PROJECT" show --format=prettyjson "${BQ_DATASET}" | \
               python3 -c "import json,sys; d=json.load(sys.stdin); d.setdefault('access', []); d['access'].append({'role':'WRITER','userByEmail':'${SINK_WRITER#serviceAccount:}'}); json.dump(d, sys.stdout)") \
    "${BQ_DATASET}" >/dev/null 2>&1 || \
  warn "Could not automatically grant BQ writer; run manually: bq add-iam-policy-binding ${BQ_DATASET}"
fi

# Exclusion on default sink so bet-outcome logs are NOT double-stored.
if gcloud logging sinks describe "_Default" --project="$PROJECT" --format='value(exclusions.name)' 2>/dev/null | grep -q "exclude-bet-outcomes"; then
  log "Default sink already excludes bet outcomes — skipping."
else
  log "Adding exclusion on _Default sink to skip bet-outcome logs ..."
  gcloud logging sinks update "_Default" \
    --add-exclusion="name=exclude-bet-outcomes,filter=${SINK_FILTER},description=Excluded; routed to bot_btc_1hr_kalshi_bet_outcomes dataset." \
    --project="$PROJECT" \
    --quiet || warn "Could not update _Default exclusion (may already exist)."
fi

# ----------------------------------------------------------------------------
# 7. Artifact Registry for container images
# ----------------------------------------------------------------------------
if gcloud artifacts repositories describe "$AR_REPO" --location="$REGION" --project="$PROJECT" &>/dev/null; then
  log "Artifact Registry repo $AR_REPO already exists — skipping."
else
  log "Creating Artifact Registry repo $AR_REPO ..."
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="bot_btc_1hr_kalshi container images" \
    --project="$PROJECT"
fi

# ----------------------------------------------------------------------------
# 8. GCS bucket for tick archive (lifecycle: COLDLINE @ 30d, delete @ 365d)
# ----------------------------------------------------------------------------
if gcloud storage buckets describe "gs://${TICK_BUCKET}" --project="$PROJECT" &>/dev/null; then
  log "GCS bucket gs://$TICK_BUCKET already exists — skipping create."
else
  log "Creating tick archive bucket gs://$TICK_BUCKET ..."
  gcloud storage buckets create "gs://${TICK_BUCKET}" \
    --location="$REGION" \
    --uniform-bucket-level-access \
    --project="$PROJECT"

  # Lifecycle policy
  LIFECYCLE_JSON="$(mktemp)"
  cat > "$LIFECYCLE_JSON" <<'EOF'
{
  "rule": [
    {"action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
     "condition": {"age": 30, "matchesStorageClass": ["STANDARD"]}},
    {"action": {"type": "Delete"},
     "condition": {"age": 365}}
  ]
}
EOF
  gcloud storage buckets update "gs://${TICK_BUCKET}" --lifecycle-file="$LIFECYCLE_JSON"
  rm -f "$LIFECYCLE_JSON"

  gcloud storage buckets add-iam-policy-binding "gs://${TICK_BUCKET}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --project="$PROJECT" >/dev/null
fi

# ----------------------------------------------------------------------------
# Done
# ----------------------------------------------------------------------------
cat <<EOF

============================================================================
✓ GCP bootstrap complete for project: $PROJECT

Next steps:
  1. Edit deploy/env.example.yaml → save as deploy/env.yaml
     - BOT_BTC_1HR_KALSHI_TICK_ARCHIVE_BUCKET: "gs://${TICK_BUCKET}"
     - BOT_BTC_1HR_KALSHI_GCP_PROJECT/REGION already resolved at runtime

  2. Build the container:
     gcloud builds submit \\
       --tag "${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/bot-btc-1hr-kalshi:latest" .

  3. Deploy (see docs/DEPLOYMENT.md §2.3 for full flags):
     gcloud run deploy ${BOT_BTC_1HR_KALSHI_SERVICE_NAME} \\
       --image=${REGION}-docker.pkg.dev/${PROJECT}/${AR_REPO}/bot-btc-1hr-kalshi:latest \\
       --service-account=${SA_EMAIL} \\
       --region=${REGION} --min-instances=1 --max-instances=1 \\
       --no-cpu-throttling --cpu=2 --memory=2Gi \\
       --ingress=internal-and-cloud-load-balancing --no-allow-unauthenticated \\
       --env-vars-file=deploy/env.yaml \\
       --set-secrets="BOT_BTC_1HR_KALSHI_API_KEY=BOT_BTC_1HR_KALSHI_API_KEY:latest,BOT_BTC_1HR_KALSHI_API_SECRET=BOT_BTC_1HR_KALSHI_API_SECRET:latest,BOT_BTC_1HR_KALSHI_ADMIN_TOKEN=BOT_BTC_1HR_KALSHI_ADMIN_TOKEN:latest"

  4. Grant yourself invoker access:
     gcloud run services add-iam-policy-binding ${BOT_BTC_1HR_KALSHI_SERVICE_NAME} \\
       --region=${REGION} --member=user:YOUR@EMAIL --role=roles/run.invoker

  5. Verify: ./scripts/status.sh
============================================================================

EOF
