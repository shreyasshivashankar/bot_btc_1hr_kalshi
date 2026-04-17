#!/usr/bin/env bash
# Download captured tick sessions from the GCS tick-archive bucket to ./data/ticks/.
# Used by `make replay` / `make backtest` to pull Parquet files for local replay.
#
# Usage:
#   ./scripts/fetch_ticks.sh [YYYY-MM-DD]              # single day
#   ./scripts/fetch_ticks.sh [YYYY-MM-DD] [YYYY-MM-DD] # inclusive range
#   ./scripts/fetch_ticks.sh --latest                  # most recent day only
#   ./scripts/fetch_ticks.sh --dry-run ...             # list what would be copied
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

BUCKET="gs://bot-btc-1hr-kalshi-tick-archive-${BOT_BTC_1HR_KALSHI_GCP_PROJECT}"
LOCAL_DIR="${DIR}/../data/ticks"

DRY=""
if [ "${1:-}" = "--dry-run" ]; then DRY="--dry-run"; shift; fi

mkdir -p "$LOCAL_DIR"

case "${1:-}" in
  "")
    _fatal "Usage: $0 [YYYY-MM-DD] | $0 FROM TO | $0 --latest  (add --dry-run first to preview)"
    ;;
  --latest)
    LATEST="$(gsutil ls -d "${BUCKET}/ticks/*/" | sort | tail -1)"
    [ -n "$LATEST" ] || _fatal "No tick data found under ${BUCKET}/ticks/"
    _info "Latest partition: $LATEST"
    if [ -n "$DRY" ]; then
      gsutil ls -r "$LATEST"
    else
      gsutil -m cp -r "$LATEST" "$LOCAL_DIR/"
    fi
    ;;
  *)
    FROM="$1"
    TO="${2:-$1}"
    _info "Fetching ticks from ${FROM} to ${TO} into ${LOCAL_DIR}"
    # Expand date range — stop on any missing partition so the user notices gaps.
    d="$FROM"
    while [[ "$d" < "$TO" || "$d" = "$TO" ]]; do
      SRC="${BUCKET}/ticks/${d}/"
      if [ -n "$DRY" ]; then
        gsutil ls -r "$SRC" || _warn "missing partition: $SRC"
      else
        gsutil -m cp -r "$SRC" "$LOCAL_DIR/" || _warn "missing partition: $SRC"
      fi
      d="$(date -j -v+1d -f '%Y-%m-%d' "$d" '+%Y-%m-%d')"
    done
    ;;
esac

_info "Done. Local archive at: ${LOCAL_DIR}"
