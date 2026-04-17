#!/usr/bin/env bash
# Scale bot_btc_1hr_kalshi to zero. Billing stops. Refuses if any open positions.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

# Safety check — no scaling down with live positions.
STATUS_JSON="$(_admin_call GET /admin/status || echo '{"open_positions": -1}')"
OPEN=$(echo "$STATUS_JSON" | python3 -c "import sys, json; print(json.load(sys.stdin).get('open_positions', -1))")

if [ "$OPEN" = "-1" ]; then
  _fatal "Could not reach /admin/status (service may already be down). If you are sure, run the gcloud command manually."
fi

if [ "$OPEN" != "0" ]; then
  _fatal "${OPEN} open positions. Flatten first: ./scripts/flatten.sh — then retry stop."
fi

_info "Scaling ${BOT_BTC_1HR_KALSHI_SERVICE_NAME} to 0 instances..."
gcloud run services update "$BOT_BTC_1HR_KALSHI_SERVICE_NAME" \
  --region="$BOT_BTC_1HR_KALSHI_GCP_REGION" \
  --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" \
  --min-instances=0 \
  --max-instances=0 \
  --quiet

_info "Service is stopped. Billing for compute should drop to zero."
_info "Restart with: ./scripts/start.sh"
