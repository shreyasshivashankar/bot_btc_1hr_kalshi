#!/usr/bin/env bash
# View Cloud Run logs for bot_btc_1hr_kalshi.
#
# Usage:
#   ./scripts/view_logs.sh [tail|stream|bets|decisions|errors] [--minutes N]
#
# Modes:
#   tail       Last 100 entries across all severities (default).
#   stream     Follow live logs (equivalent to `gcloud beta logging tail`).
#   bets       Filter to bet_outcome records only (closed positions).
#   decisions  Filter to decision records (emitted per trap evaluation).
#   errors     Filter to severity>=ERROR.
#
# --minutes N (default 60) bounds the historical window for non-stream modes.
#
# Requires: gcloud auth, Logging Viewer role. The Cloud Run service name is
# read from BOT_BTC_1HR_KALSHI_SERVICE_NAME (default: bot-btc-1hr-kalshi).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Handle --help before sourcing _common.sh so it works without GCP env vars.
for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
  fi
done

source "${DIR}/_common.sh"

MODE="${1:-tail}"; shift || true
MINUTES=60
while [[ $# -gt 0 ]]; do
  case "$1" in
    --minutes) MINUTES="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) _fatal "Unknown flag: $1" ;;
  esac
done

SERVICE="${BOT_BTC_1HR_KALSHI_SERVICE_NAME}"
PROJECT="${BOT_BTC_1HR_KALSHI_GCP_PROJECT}"
BASE_FILTER="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${SERVICE}\""
SINCE=$(date -u -v-"${MINUTES}"M "+%Y-%m-%dT%H:%M:%SZ" 2>/dev/null \
        || date -u -d "-${MINUTES} minutes" "+%Y-%m-%dT%H:%M:%SZ")

case "$MODE" in
  tail)
    _info "Last 100 entries (${MINUTES}m window)"
    FILTER="${BASE_FILTER} AND timestamp >= \"${SINCE}\""
    ;;
  stream)
    _info "Streaming logs (Ctrl-C to stop) — service=${SERVICE}"
    exec gcloud beta logging tail "${BASE_FILTER}" \
      --project="${PROJECT}" \
      --format='value(timestamp,severity,jsonPayload.event,textPayload)'
    ;;
  bets)
    _info "bet_outcome records (${MINUTES}m window)"
    FILTER="${BASE_FILTER} AND timestamp >= \"${SINCE}\" AND jsonPayload.event=\"bet_outcome\""
    ;;
  decisions)
    _info "decision records (${MINUTES}m window)"
    FILTER="${BASE_FILTER} AND timestamp >= \"${SINCE}\" AND jsonPayload.event=\"decision\""
    ;;
  errors)
    _info "severity>=ERROR (${MINUTES}m window)"
    FILTER="${BASE_FILTER} AND timestamp >= \"${SINCE}\" AND severity>=ERROR"
    ;;
  *)
    _fatal "Unknown mode '$MODE'. See --help." ;;
esac

gcloud logging read "$FILTER" \
  --project="$PROJECT" \
  --limit=100 \
  --order=desc \
  --format='value(timestamp,severity,jsonPayload.event,jsonPayload.market_id,jsonPayload.trap,jsonPayload.approved,jsonPayload.net_pnl_usd,textPayload)'
