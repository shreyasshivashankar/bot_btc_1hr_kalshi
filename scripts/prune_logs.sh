#!/usr/bin/env bash
# Clear Cloud Run log entries for bot_btc_1hr_kalshi.
#
# Usage:
#   ./scripts/prune_logs.sh              # delete all Cloud Run log entries
#   ./scripts/prune_logs.sh --dry-run    # show what would be deleted
#
# Retention (14 days by default) is enforced at the log-bucket level by
# `deploy/setup_gcp.sh` via `gcloud logging buckets update _Default
# --retention-days=N`. That is the source of truth for steady-state retention;
# this script is for on-demand clean cuts (e.g. before a fresh soak).
#
# The BigQuery sink (bot_btc_1hr_kalshi.bet_outcomes) is the long-term store
# for closed-trade records — we never touch it from here. Only Cloud Run
# log streams (run.googleapis.com/*) are deleted; audit, ops-agent, and
# cloudbuild logs are left alone.
#
# Implementation note: `gcloud logging logs delete LOG_NAME` deletes all
# entries for a named log. gcloud has no filter-based entry delete
# (contrary to older docs — `gcloud logging delete` with a filter doesn't
# exist as a subcommand). Deletion is accepted synchronously but entries
# may take up to ~1h to fully drain from read paths.
#
# Requires: Logging Admin role.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
  fi
done

source "${DIR}/_common.sh"

DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help)
      sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) _fatal "Unknown flag: $1" ;;
  esac
done

PROJECT="${BOT_BTC_1HR_KALSHI_GCP_PROJECT}"

# Enumerate Cloud Run log streams. `gcloud logging logs list` returns
# fully-qualified names like `projects/PROJECT/logs/run.googleapis.com%2Fstdout`.
# Strip the `projects/.../logs/` prefix; gcloud accepts either the encoded
# (`run.googleapis.com%2Fstdout`) or decoded (`run.googleapis.com/stdout`) form
# when passed to `logs delete`.
# macOS ships bash 3.2 which lacks `mapfile`; use a portable read loop.
LOGS=()
while IFS= read -r line; do
  [ -n "$line" ] && LOGS+=("$line")
done < <(
  gcloud logging logs list --project="$PROJECT" --format='value(NAME)' \
    | sed -n 's|^projects/[^/]*/logs/||p' \
    | grep -E '^run\.googleapis\.com' \
    || true
)

if [ "${#LOGS[@]}" -eq 0 ]; then
  _info "No Cloud Run log streams present — nothing to prune."
  exit 0
fi

_info "Cloud Run log streams queued for deletion (${#LOGS[@]}):"
for LOG in "${LOGS[@]}"; do
  printf "  - %s\n" "$(printf '%s' "$LOG" | sed 's|%2F|/|g')"
done

if [[ "$DRY_RUN" -eq 1 ]]; then
  _info "[dry-run] nothing deleted."
  exit 0
fi

for LOG in "${LOGS[@]}"; do
  _info "Deleting: ${LOG//%2F/\/}"
  gcloud logging logs delete "$LOG" --project="$PROJECT" --quiet
done

_info "Prune accepted. Entries drain from read paths within ~1h."
