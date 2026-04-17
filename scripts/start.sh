#!/usr/bin/env bash
# Scale bot_btc_1hr_kalshi back up (min=max=1). Container cold-starts; feeds reconnect.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

_info "Scaling ${BOT_BTC_1HR_KALSHI_SERVICE_NAME} to 1 instance..."
gcloud run services update "$BOT_BTC_1HR_KALSHI_SERVICE_NAME" \
  --region="$BOT_BTC_1HR_KALSHI_GCP_REGION" \
  --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" \
  --min-instances=1 \
  --max-instances=1 \
  --quiet

_info "Waiting for service to become ready..."
URL="$(_service_url)"
for i in {1..30}; do
  if curl -sS -o /dev/null -w "%{http_code}" \
      -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
      "${URL}/readyz" | grep -q "^200$"; then
    _info "Service is ready."
    _admin_call GET /admin/status | python3 -m json.tool || true
    exit 0
  fi
  sleep 2
done
_warn "Service did not become ready within 60s; check logs:"
_warn "  gcloud logging read 'resource.type=cloud_run_revision' --project=${BOT_BTC_1HR_KALSHI_GCP_PROJECT} --limit=50"
exit 1
