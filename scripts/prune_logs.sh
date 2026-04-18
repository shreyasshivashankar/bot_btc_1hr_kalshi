#!/usr/bin/env bash
# Prune Cloud Run logs older than N days (default: 14) for bot_btc_1hr_kalshi.
#
# Usage:
#   ./scripts/prune_logs.sh               # prune entries older than 14d
#   ./scripts/prune_logs.sh --days 30     # override retention window
#   ./scripts/prune_logs.sh --dry-run     # show filter, don't delete
#
# Retention policy: we keep 14 days of Cloud Run logs in Logging. The
# BigQuery sink (bot_btc_1hr_kalshi.bet_outcomes) is the long-term store
# for closed-trade records — we never prune those from here.
#
# Implementation: `gcloud logging delete` deletes matching *log entries*,
# not the log itself. Safe to re-run.
#
# Requires: gcloud Logging Admin role.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
  fi
done

source "${DIR}/_common.sh"

DAYS=14
DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --days) DAYS="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help)
      sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) _fatal "Unknown flag: $1" ;;
  esac
done

SERVICE="${BOT_BTC_1HR_KALSHI_SERVICE_NAME}"
PROJECT="${BOT_BTC_1HR_KALSHI_GCP_PROJECT}"
CUTOFF=$(date -u -v-"${DAYS}"d "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
         || date -u -d "-${DAYS} days" "+%Y-%m-%dT%H:%M:%SZ")

FILTER="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${SERVICE}\" AND timestamp < \"${CUTOFF}\""

_info "Deleting log entries older than ${DAYS} days (before ${CUTOFF})"
_info "Filter: ${FILTER}"

if [[ "$DRY_RUN" -eq 1 ]]; then
  _info "[dry-run] preview first 5 matching entries:"
  gcloud logging read "$FILTER" \
    --project="$PROJECT" \
    --limit=5 \
    --order=asc \
    --format='value(timestamp,jsonPayload.event,textPayload)'
  exit 0
fi

# gcloud prompts for confirmation; --quiet auto-accepts.
gcloud logging delete "$FILTER" \
  --project="$PROJECT" \
  --quiet

_info "Prune complete."
