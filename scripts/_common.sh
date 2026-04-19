#!/usr/bin/env bash
# Shared helpers for bot_btc_1hr_kalshi ops scripts. Sourced, not executed.
set -euo pipefail

: "${BOT_BTC_1HR_KALSHI_GCP_PROJECT:?Set BOT_BTC_1HR_KALSHI_GCP_PROJECT in your shell (or .env and source it)}"
: "${BOT_BTC_1HR_KALSHI_GCP_REGION:=us-central1}"
: "${BOT_BTC_1HR_KALSHI_SERVICE_NAME:=bot-btc-1hr-kalshi}"

# Local port used by `gcloud run services proxy`. Override via env if 8765
# collides with something else on the operator's machine.
: "${BOT_BTC_1HR_KALSHI_ADMIN_PROXY_PORT:=8765}"

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
#
# The service is deployed with ingress=internal-and-cloud-load-balancing
# (see deploy/cloudrun.yaml), so direct curls from an operator laptop to the
# *.run.app URL get a 404 from Cloud Run's edge — they never reach the
# container. `gcloud run services proxy` opens a short-lived local TCP tunnel
# that carries the request over the IAP path with the operator's IAM identity,
# so it bypasses the public-internet ingress block correctly. The admin bearer
# token is still required in-band (`X-Admin-Token`) and the IAP identity is
# still checked by Cloud Run — we get both layers.
_admin_call() {
  local method="$1" path="$2"; shift 2
  local port="$BOT_BTC_1HR_KALSHI_ADMIN_PROXY_PORT"
  local proxy_pid=""
  local rc=0

  gcloud run services proxy "$BOT_BTC_1HR_KALSHI_SERVICE_NAME" \
    --region="$BOT_BTC_1HR_KALSHI_GCP_REGION" \
    --project="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" \
    --port="$port" >/dev/null 2>&1 &
  proxy_pid=$!

  # Wait up to 10s for the local proxy to accept connections. Any HTTP
  # response (including 404/503) proves the tunnel is up; we don't care
  # what /healthz returns here, only that the TCP listener is answering.
  local tries=0
  while ! curl -sS --max-time 0.5 -o /dev/null "http://localhost:${port}/healthz" 2>/dev/null; do
    tries=$((tries + 1))
    if [ "$tries" -gt 50 ]; then
      kill "$proxy_pid" 2>/dev/null || true
      wait "$proxy_pid" 2>/dev/null || true
      _fatal "gcloud run services proxy failed to start on port ${port} within 10s"
    fi
    sleep 0.2
  done

  local adm_token
  adm_token="$(_admin_token)"
  curl -sS --fail-with-body \
    -X "$method" \
    -H "X-Admin-Token: ${adm_token}" \
    -H "Content-Type: application/json" \
    "http://localhost:${port}${path}" "$@" || rc=$?

  kill "$proxy_pid" 2>/dev/null || true
  wait "$proxy_pid" 2>/dev/null || true
  return $rc
}

_info()  { printf "\033[1;34m[bot_btc_1hr_kalshi]\033[0m %s\n" "$*" >&2; }
_warn()  { printf "\033[1;33m[warn]\033[0m %s\n" "$*" >&2; }
_fatal() { printf "\033[1;31m[fatal]\033[0m %s\n" "$*" >&2; exit 1; }
