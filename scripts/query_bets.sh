#!/usr/bin/env bash
# Run canned BigQuery queries from docs/RUNBOOK.md against the 5-day bet-outcome dataset.
# Usage: ./scripts/query_bets.sh [q1|q2|q3|q4|q5]  (default: q1)
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${DIR}/_common.sh"

BQ_DATASET="${BOT_BTC_1HR_KALSHI_BET_OUTCOMES_BQ_DATASET:-bot_btc_1hr_kalshi_bet_outcomes}"
BQ_TABLE="${BOT_BTC_1HR_KALSHI_BET_OUTCOMES_BQ_TABLE:-outcomes}"
FQTN="${BOT_BTC_1HR_KALSHI_GCP_PROJECT}.${BQ_DATASET}.${BQ_TABLE}"

QUERY="${1:-q1}"

case "$QUERY" in
  q1)  # Hit rate & PnL by trap
    SQL=$(cat <<EOF
SELECT trap, COUNT(*) n,
       ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) hit_rate,
       ROUND(SUM(net_pnl_usd), 2) total_pnl_usd,
       ROUND(AVG(net_pnl_usd), 3) avg_pnl_per_trade,
       ROUND(AVG(hold_duration_sec), 0) avg_hold_sec
FROM \`${FQTN}\`
WHERE exit_reason != 'abandoned_to_settlement'
GROUP BY trap
ORDER BY total_pnl_usd DESC
EOF
)
    ;;
  q2)  # Exit reason distribution
    SQL=$(cat <<EOF
SELECT exit_reason, COUNT(*) n,
       ROUND(SUM(net_pnl_usd), 2) total_pnl_usd,
       ROUND(AVG(net_pnl_usd), 3) avg_pnl
FROM \`${FQTN}\`
GROUP BY exit_reason
ORDER BY n DESC
EOF
)
    ;;
  q3)  # Regime-conditioned PnL
    SQL=$(cat <<EOF
SELECT features_at_entry.regime_trend trend,
       features_at_entry.regime_vol vol,
       COUNT(*) n,
       ROUND(SUM(net_pnl_usd), 2) total_pnl_usd,
       ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) hit_rate
FROM \`${FQTN}\`
GROUP BY trend, vol
HAVING n >= 10
ORDER BY total_pnl_usd ASC
EOF
)
    ;;
  q4)  # Early cash-out EV
    SQL=$(cat <<EOF
SELECT COUNT(*) n,
       ROUND(SUM(net_pnl_usd), 2) realized_total,
       ROUND(SUM(counterfactual_held_pnl_usd), 2) counterfactual_total,
       ROUND(SUM(counterfactual_held_pnl_usd) - SUM(net_pnl_usd), 2) missed_upside
FROM \`${FQTN}\`
WHERE exit_reason = 'early_cashout_99'
EOF
)
    ;;
  q5)  # Confidence calibration
    SQL=$(cat <<EOF
SELECT ROUND(features_at_entry.signal_confidence, 1) conf_bucket,
       COUNT(*) n,
       ROUND(AVG(CAST(net_pnl_usd > 0 AS INT64)), 3) realized_hit_rate,
       ROUND(AVG(net_pnl_usd), 3) avg_pnl
FROM \`${FQTN}\`
GROUP BY conf_bucket
ORDER BY conf_bucket
EOF
)
    ;;
  *)
    _fatal "Unknown query '$QUERY'. Options: q1 q2 q3 q4 q5. See docs/RUNBOOK.md for descriptions."
    ;;
esac

_info "Running ${QUERY} against ${FQTN}"
bq --project_id="$BOT_BTC_1HR_KALSHI_GCP_PROJECT" query \
  --use_legacy_sql=false \
  --format=pretty \
  "$SQL"
