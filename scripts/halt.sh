#!/usr/bin/env bash
# Soft pause — no new entries. Container keeps running; existing positions exit normally.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

_info "Sending /admin/halt..."
_admin_call POST /admin/halt | python3 -m json.tool
_info "Trading is halted (no new entries). Use ./scripts/resume.sh to re-enable."
