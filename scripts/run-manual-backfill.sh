#!/usr/bin/env bash
# Trigger an end-to-end FootballIQ pipeline run on demand.
#
# Useful for:
#   - one-off backfills of a historical season
#   - testing a full run on a fresh stack without waiting for cron
#
# Usage:
#   ./scripts/run-manual-backfill.sh <competition_id> <season_id> [max_matches]
#
# Example:
#   ./scripts/run-manual-backfill.sh 9 281 10

set -euo pipefail

COMPETITION_ID="${1:-9}"
SEASON_ID="${2:-281}"
MAX_MATCHES="${3:-10}"

AIRFLOW_API_BASE="${AIRFLOW_API_BASE:-http://localhost:8085/api/v1}"
AIRFLOW_USER="${AIRFLOW_USER:-admin}"
AIRFLOW_PASS="${AIRFLOW_PASS:-admin}"

trigger_dag() {
  local dag_id="$1"
  local conf_json="$2"
  local payload
  payload='{"conf":'"${conf_json}"'}'

  echo "Triggering ${dag_id} with conf=${conf_json}"
  curl --fail --silent --show-error \
    --user "${AIRFLOW_USER}:${AIRFLOW_PASS}" \
    --header "Content-Type: application/json" \
    --request POST \
    "${AIRFLOW_API_BASE}/dags/${dag_id}/dagRuns" \
    --data "${payload}" >/dev/null
}

trigger_dag "bronze_silver_weekly" "{\"competition_id\": ${COMPETITION_ID}, \"season_id\": ${SEASON_ID}, \"max_matches\": ${MAX_MATCHES}}"

echo "After Bronze+Silver succeeds, trigger Gold runs with:"
echo "  ${0} ${COMPETITION_ID} ${SEASON_ID} ${MAX_MATCHES}  # then call trigger_dag manually below if needed"
echo "  curl -u ${AIRFLOW_USER}:${AIRFLOW_PASS} -H 'Content-Type: application/json' -X POST ${AIRFLOW_API_BASE}/dags/gold_match_kpi_weekly/dagRuns -d '{\"conf\":{\"season_id\":${SEASON_ID}}}'"
echo "  curl -u ${AIRFLOW_USER}:${AIRFLOW_PASS} -H 'Content-Type: application/json' -X POST ${AIRFLOW_API_BASE}/dags/gold_season_kpi_monthly/dagRuns -d '{\"conf\":{\"season_id\":${SEASON_ID}}}'"
echo "  curl -u ${AIRFLOW_USER}:${AIRFLOW_PASS} -H 'Content-Type: application/json' -X POST ${AIRFLOW_API_BASE}/dags/gold_form_last5_5weeks/dagRuns -d '{\"conf\":{\"season_id\":${SEASON_ID}}}'"
