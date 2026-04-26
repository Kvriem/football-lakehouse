# Infrastructure README

This document explains the local infrastructure for the Football Lakehouse stack and how to set it up from scratch.

## Architecture

```text
                           +---------------------------+
                           |        Dremio UI          |
                           |      http://:9047         |
                           +------------+--------------+
                                        |
                                        | SQL over Iceberg catalog
                                        v
+------------------+          +---------+-----------------------------+
|   Spark Worker   |<---------+          Spark Master                 |
|  (executes jobs) | spark:// |  - Iceberg + Nessie catalog config   |
+--------+---------+          |  - Writes table metadata via Nessie   |
         |                    |  - Writes data files to MinIO         |
         |                    +-------------------+--------------------+
         |                                        |
         |                                        | Catalog operations
         |                                        v
         |                            +-----------+-----------+
         |                            |       Nessie          |
         |                            |  http://:19120/api/v1 |
         |                            +-----------+-----------+
         |                                        |
         |                                        | S3 API
         v                                        v
+--------+----------------------------------------+--------+
|                        MinIO                              |
|                     http://:9001                          |
|    Bucket: football-lake                                  |
|    Prefixes: bronze/, silver/, gold/                      |
+-----------------------------------------------------------+
```

## Services

| Service | Purpose | URL/Port |
|---|---|---|
| MinIO | Object storage for Iceberg data files | API: `http://localhost:9000`, UI: `http://localhost:9001` |
| Nessie | Catalog and branching for Iceberg metadata | `http://localhost:19120/api/v1` |
| Spark master | Spark control plane + SQL entry point | Master: `spark://localhost:7077`, UI: `http://localhost:8080` |
| Spark worker | Spark execution resources | UI: `http://localhost:8081` |
| Dremio | SQL analytics/query engine | `http://localhost:9047` |

## Prerequisites

- Docker Engine
- Docker Compose (v2)
- `curl`

## Setup Steps

### 1) Start infrastructure

```bash
docker compose up -d
```

### 2) Download Spark runtime dependencies (Iceberg/Nessie/S3)

```bash
./scripts/setup_spark_jars.sh
```

If jars are missing when Spark starts, rerun:

```bash
docker compose restart spark spark-worker
```

### 3) Verify MinIO bucket and prefixes

The `minio-init` service bootstraps:

- bucket: `football-lake`
- prefixes: `bronze/`, `silver/`, `gold/`

Manual check:

```bash
docker run --rm --entrypoint /bin/sh --network footballiq_iceberg minio/mc -c \
"mc alias set local http://minio:9000 admin password >/dev/null && mc ls local/football-lake"
```

### 4) Verify Nessie API and `main` branch

```bash
curl -fsS http://localhost:19120/api/v1/config
curl -fsS http://localhost:19120/api/v1/trees
```

Expected:

- `defaultBranch` is `main`
- branch list includes `main`

### 5) Verify Spark catalog wiring

Spark uses Nessie as the Iceberg catalog through `spark/conf/spark-defaults.conf`:

- `spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog`
- `spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog`
- `spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1`
- `spark.sql.catalog.nessie.ref=main`
- `spark.sql.catalog.nessie.warehouse=s3a://football-lake/`

### 6) Run smoke test

```bash
./scripts/smoke-test.sh
```

This verifies:

- MinIO/Nessie reachability
- bucket availability
- Spark can create/query an Iceberg table via Nessie
- Dremio endpoint is reachable

## Access and Default Credentials

| Service | Username | Password |
|---|---|---|
| MinIO | `admin` | `password` |
| Dremio | Set on first login | Set on first login |

## Common Operations

### Start

```bash
docker compose up -d
```

### Stop

```bash
docker compose down
```

### Stop + remove volumes (full reset)

```bash
docker compose down -v
```

### View logs

```bash
docker compose logs -f minio nessie spark spark-worker dremio
```

## Troubleshooting

- Spark jobs stuck with "Initial job has not accepted any resources"
  - Ensure `spark-worker` is running: `docker compose ps`
- Iceberg writes fail with AWS region errors
  - Confirm `spark-defaults.conf` contains MinIO/Nessie S3 region settings (`us-east-1`)
- MinIO prefixes visible in MinIO but not Dremio
  - This is expected until actual Iceberg tables are created and registered in Nessie
