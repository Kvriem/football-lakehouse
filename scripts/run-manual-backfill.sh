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

CONF='{"competition_id": '"${COMPETITION_ID}"', "season_id": '"${SEASON_ID}"', "max_matches": '"${MAX_MATCHES}"'}'

echo "Triggering bronze_silver_weekly with conf=${CONF}"
docker exec airflow-scheduler airflow dags trigger \
  bronze_silver_weekly \
  --conf "${CONF}"

echo "After Bronze+Silver succeeds, trigger Gold runs:"
echo "  docker exec airflow-scheduler airflow dags trigger gold_match_kpi_weekly --conf '{\"season_id\": ${SEASON_ID}}'"
echo "  docker exec airflow-scheduler airflow dags trigger gold_season_kpi_monthly --conf '{\"season_id\": ${SEASON_ID}}'"
echo "  docker exec airflow-scheduler airflow dags trigger gold_form_last5_5weeks --conf '{\"season_id\": ${SEASON_ID}}'"
