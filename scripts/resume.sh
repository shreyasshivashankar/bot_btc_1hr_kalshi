#!/usr/bin/env bash
# Clear soft halt. New entries re-enabled subject to risk checks.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

_info "Sending /admin/resume..."
_admin_call POST /admin/resume | python3 -m json.tool
