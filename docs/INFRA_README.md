# Infrastructure README

This document covers the local stack used by this project and how each service participates in Iceberg table operations.

## Architecture

```text
StatsBomb files -> Python/PySpark jobs -> Spark Catalog (Nessie) -> MinIO (Iceberg data files)
                                                        |
                                                        v
                                                  Dremio SQL
```

Control and storage boundaries:

- Spark handles compute and Iceberg write/read operations.
- Nessie stores Iceberg catalog metadata and branch history.
- MinIO stores table data files and manifests under `s3a://football-lake/`.
- Dremio queries Iceberg tables via Nessie metadata.

## Service Inventory

| Service | Role | Endpoint |
|---|---|---|
| `minio` | S3-compatible object store | API `http://localhost:9000`, UI `http://localhost:9001` |
| `minio-init` | Creates bucket and layer prefixes | Runs once during startup |
| `nessie-postgres` | Persistent backing store for Nessie | Internal service |
| `nessie` | Catalog + branching API | `http://localhost:19120/api/v1` |
| `spark` | Spark master process | `spark://localhost:7077`, UI `http://localhost:8080` |
| `spark-worker` | Spark worker process | UI `http://localhost:8081` |
| `jupyter` | Notebook runtime for jobs | `http://localhost:8888` |
| `dremio` | SQL and BI query layer | `http://localhost:9047` |
| `airflow-postgres` | Airflow metadata DB | Internal service |
| `airflow-init` | One-shot DB migrate + admin user + pool seed | Runs once on `up` |
| `airflow-scheduler` | Schedules DAG runs and dispatches tasks | Internal service |
| `airflow-webserver` | Airflow UI and REST API | `http://localhost:8085` |

## Storage Layout

Bucket created at startup:

- `football-lake`

Prefix layout:

- `football-lake/bronze/`
- `football-lake/silver/`
- `football-lake/gold/`

## Prerequisites

- Docker Engine
- Docker Compose v2
- `curl`

## Bootstrap

1) Download Spark runtime jars (Iceberg, Nessie, Hadoop AWS, AWS SDK):

```bash
./scripts/setup_spark_jars.sh
```

2) Start all services:

```bash
docker compose up -d
```

3) Run end-to-end infrastructure verification:

```bash
./scripts/smoke-test.sh
```

## What the Smoke Test Validates

`scripts/smoke-test.sh` verifies that:

- Nessie API is reachable.
- MinIO health endpoint is reachable.
- `football-lake` bucket is available.
- Spark can create, insert, and query an Iceberg table through Nessie.
- Dremio endpoint is reachable.

## Spark Catalog Wiring

Configured in `spark/conf/spark-defaults.conf`:

- `spark.sql.extensions` enables Iceberg + Nessie Spark extensions.
- `spark.sql.catalog.nessie` uses `SparkCatalog` with `NessieCatalog`.
- `spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1`
- `spark.sql.catalog.nessie.ref=main`
- `spark.sql.catalog.nessie.warehouse=s3a://football-lake/`
- `spark.sql.catalog.nessie.s3.endpoint=http://minio:9000`

## Access and Credentials

| Service | Username | Password |
|---|---|---|
| MinIO | `admin` | `password` |
| Jupyter token | N/A | `footballiq` |
| Dremio | Set on first login | Set on first login |
| Airflow | `admin` | `admin` |

## Orchestration Layer

Airflow orchestrates the medallion pipeline on independent schedules:

- `bronze_silver_weekly` -> Mon 02:00 UTC
- `gold_match_kpi_weekly` -> Mon 03:00 UTC
- `gold_season_kpi_monthly` -> 1st of each month 04:00 UTC
- `gold_form_last5_5weeks` -> every 5 weeks

See [`docs/AIRFLOW_README.md`](AIRFLOW_README.md) for the full DAG topology, branch promotion strategy, and runbook.

Build and start the Airflow services:

```bash
docker compose build airflow-init airflow-scheduler airflow-webserver
docker compose up -d airflow-postgres airflow-init airflow-scheduler airflow-webserver
```

## Operations

Start:

```bash
docker compose up -d
```

Stop:

```bash
docker compose down
```

Stop and reset volumes:

```bash
docker compose down -v
```

Follow logs:

```bash
docker compose logs -f nessie minio spark spark-worker jupyter dremio airflow-scheduler airflow-webserver
```

## Troubleshooting

- Spark has no resources:
  - Confirm `spark-worker` is healthy: `docker compose ps`.
- Spark cannot access MinIO:
  - Re-check endpoint and credentials in `spark/conf/spark-defaults.conf`.
- Catalog operations fail:
  - Verify `nessie` and `nessie-postgres` are both up.
- Dremio cannot show tables:
  - Ensure Iceberg tables were already created in Spark on the same Nessie branch.
