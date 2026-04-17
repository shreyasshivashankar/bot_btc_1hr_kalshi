#!/usr/bin/env bash
# Immediately close all open positions via IOC escalation ladder.
# Same mechanism as /admin/tier1_override but without the 2h stabilization window.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

_info "Confirm flatten of ALL open positions [y/N]?"
read -r CONFIRM
[ "$CONFIRM" = "y" ] || { _info "Aborted."; exit 1; }

_admin_call POST /admin/flatten | python3 -m json.tool
