#!/usr/bin/env bash
# Print bot_btc_1hr_kalshi runtime status. Read-only; safe to run any time.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

# Container state (scaled up or down?)
INSTANCES=$(gcloud run services describe "$BOT_BTC_1HR_KALSHI_SERVICE_NAME" \
  --region="$BOT_BTC_1HR_KALSHI_GCP_REGION" --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" \
  --format='value(spec.template.metadata.annotations."autoscaling.knative.dev/minScale")' 2>/dev/null || echo "unknown")

_info "Cloud Run minScale: ${INSTANCES}"

if [ "$INSTANCES" = "0" ]; then
  _warn "Container is scaled to zero. Use ./scripts/start.sh to resume."
  exit 0
fi

# Live status via admin endpoint
_admin_call GET /admin/status | python3 -m json.tool
