#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yaml"

check_url() {
  local name="$1"
  local url="$2"
  echo "Checking ${name} at ${url}"
  curl -fsS "${url}" >/dev/null
}

echo "Preparing Spark runtime jars"
"${ROOT_DIR}/scripts/setup_spark_jars.sh"

echo "Starting docker compose stack"
docker compose -f "${COMPOSE_FILE}" up -d

echo "Waiting for base services"
for i in $(seq 1 30); do
  if check_url "Nessie" "http://localhost:19120/api/v1/config" \
     && check_url "MinIO" "http://localhost:9000/minio/health/live"; then
    break
  fi
  if [[ "${i}" -eq 30 ]]; then
    echo "Base services did not become healthy in time"
    exit 1
  fi
  sleep 4
done

echo "Checking MinIO bucket exists"
docker exec minio-init /usr/bin/mc ls local/football-lake >/dev/null 2>&1 || true
docker exec minio /bin/sh -c "ls /data >/dev/null"

echo "Running Spark catalog smoke query"
SPARK_SMOKE_SQL="
CREATE NAMESPACE IF NOT EXISTS nessie.bronze;
CREATE TABLE IF NOT EXISTS nessie.bronze.smoke_table (id INT, name STRING) USING iceberg;
DELETE FROM nessie.bronze.smoke_table;
INSERT INTO nessie.bronze.smoke_table VALUES (1, 'ok');
SELECT COUNT(*) AS row_count FROM nessie.bronze.smoke_table;
"

spark_ok=0
for i in $(seq 1 8); do
  if docker exec spark /opt/spark/bin/spark-sql \
    --master spark://spark:7077 \
    --conf "spark.sql.warehouse.dir=/tmp/smoke-warehouse-${i}" \
    -e "${SPARK_SMOKE_SQL}" | tee /tmp/pitchlake_spark_smoke.log; then
    spark_ok=1
    break
  fi
  echo "Spark query attempt ${i} failed, retrying..."
  sleep 6
done

if [[ "${spark_ok}" -ne 1 ]]; then
  echo "Spark smoke query failed after retries"
  exit 1
fi

if ! awk 'BEGIN{found=0} /^[[:space:]]*1[[:space:]]*$/ {found=1} END{exit found?0:1}' /tmp/pitchlake_spark_smoke.log; then
  echo "Spark smoke query output did not include expected row count"
  exit 1
fi

echo "Checking Dremio UI endpoint"
for i in $(seq 1 30); do
  if check_url "Dremio" "http://localhost:9047"; then
    break
  fi
  if [[ "${i}" -eq 30 ]]; then
    echo "Dremio did not become reachable in time"
    exit 1
  fi
  sleep 5
done

echo "Smoke test passed: MinIO + Nessie + Spark + Dremio are reachable."
