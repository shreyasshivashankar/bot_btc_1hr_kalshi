#!/usr/bin/env bash
# Human kill-switch for unscheduled macro shocks. Flattens book + 2h stabilization.
# Usage: ./scripts/tier1_override.sh "reason string"
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

REASON="${1:-}"
[ -n "$REASON" ] || _fatal "Usage: $0 \"reason string for audit log\""

_info "TIER 1 OVERRIDE — reason: $REASON"
_info "This flattens ALL positions and blocks new entries for 2 hours."
_info "Confirm [type YES]:"
read -r CONFIRM
[ "$CONFIRM" = "YES" ] || { _info "Aborted."; exit 1; }

_admin_call POST /admin/tier1_override \
  --data-raw "$(printf '{"reason":%s}' "$(python3 -c "import json,sys;print(json.dumps(sys.argv[1]))" "$REASON")")" \
  | python3 -m json.tool
