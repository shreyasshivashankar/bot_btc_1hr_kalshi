#!/usr/bin/env bash
# Shared helpers for bot_btc_1hr_kalshi ops scripts. Sourced, not executed.
set -euo pipefail

: "${BOT_BTC_1HR_KALSHI_GCP_PROJECT:?Set BOT_BTC_1HR_KALSHI_GCP_PROJECT in your shell (or .env and source it)}"
: "${BOT_BTC_1HR_KALSHI_GCP_REGION:=us-central1}"
: "${BOT_BTC_1HR_KALSHI_SERVICE_NAME:=bot-btc-1hr-kalshi}"

# Resolve the Cloud Run service URL once per invocation.
_service_url() {
  gcloud run services describe "$BOT_BTC_1HR_KALSHI_SERVICE_NAME" \
    --region="$BOT_BTC_1HR_KALSHI_GCP_REGION" \
    --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" \
    --format='value(status.url)'
}

# Fetch the admin bearer token from Secret Manager.
_admin_token() {
  gcloud secrets versions access latest \
    --secret="BOT_BTC_1HR_KALSHI_ADMIN_TOKEN" \
    --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT"
}

# Make an authenticated admin HTTP call. Usage: _admin_call METHOD PATH [curl-extra...]
_admin_call() {
  local method="$1" path="$2"; shift 2
  local url="$(_service_url)${path}"
  local id_token="$(gcloud auth print-identity-token)"
  local adm_token="$(_admin_token)"
  curl -sS --fail-with-body \
    -X "$method" \
    -H "Authorization: Bearer ${id_token}" \
    -H "X-Admin-Token: ${adm_token}" \
    -H "Content-Type: application/json" \
    "$url" "$@"
}

_info()  { printf "\033[1;34m[bot_btc_1hr_kalshi]\033[0m %s\n" "$*" >&2; }
_warn()  { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
_fatal() { printf "\033[1;31m[fatal]\033[0m %s\n" "$*" >&2; exit 1; }
