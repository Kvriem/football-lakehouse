# ⚽ PitchLake — Football Analytics Lakehouse

> A production-grade open lakehouse built with Apache Iceberg, Apache Spark, Project Nessie, and Dremio — end-to-end player performance analytics on football match data.

![Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5-3B6ECC?logo=apache&logoColor=white)
![Nessie](https://img.shields.io/badge/Project%20Nessie-0.76-6A4C93)
![Dremio](https://img.shields.io/badge/Dremio-25.x-1B7FC4?logo=dremio&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-latest-C72E49?logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-ready-2496ED?logo=docker&logoColor=white)

---

## Table of contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Project structure](#project-structure)
- [Quick start](#quick-start)
- [Data pipeline](#data-pipeline)
  - [Phase 1 — Infrastructure](#phase-1--infrastructure)
  - [Phase 2 — Bronze layer](#phase-2--bronze-layer)
  - [Phase 3 — Silver layer](#phase-3--silver-layer)
  - [Phase 4 — Gold layer](#phase-4--gold-layer)
- [Deep knowledge showcases](#deep-knowledge-showcases)
  - [Iceberg schema evolution](#iceberg-schema-evolution)
  - [Iceberg time travel](#iceberg-time-travel)
  - [Small file compaction](#small-file-compaction)
  - [Nessie branching workflow](#nessie-branching-workflow)
  - [Dremio reflections](#dremio-reflections)
- [Analytical queries](#analytical-queries)
- [Configuration reference](#configuration-reference)
- [Design decisions](#design-decisions)
- [Roadmap](#roadmap)

---

## Overview

<!-- 3–5 sentences. What is this project? What problem does it solve? Why this stack? -->

---

## Architecture

<!-- Insert architecture diagram here (PNG or SVG) -->

```
Data Source (StatsBomb/FBref)
        │  weekly batch
        ▼
Python Ingestion Scripts
        │
        ▼
MinIO (s3://pitchlake/)
 ├── bronze/    ← raw, schema-on-read Iceberg tables
 ├── silver/    ← typed, cleaned, enriched
 └── gold/      ← aggregated player performance metrics
        │
        ▼
Project Nessie  ← Git-like catalog versioning for all Iceberg tables
        │
        ▼
Apache Spark    ← batch transformation engine (Bronze → Silver → Gold)
        │
        ▼
Dremio          ← SQL analytics layer with reflections
```

---

## Tech stack

| Layer | Technology | Role |
|---|---|---|
| Object storage | MinIO | S3-compatible local storage |
| Table format | Apache Iceberg | ACID tables, schema evolution, time travel |
| Catalog | Project Nessie | Git-like versioning for Iceberg metadata |
| Processing | Apache Spark 3.5 | Batch ETL and aggregation jobs |
| Query engine | Dremio | Interactive SQL + reflections |
| Orchestration | <!-- Airflow / shell scripts --> | Weekly job scheduling |
| Language | Python 3.11 | Ingestion scripts and Spark jobs |
| Infrastructure | Docker Compose | Local full-stack deployment |

---

## Project structure

```
pitchlake/
├── docker/
│   ├── docker-compose.yml
│   └── spark/
│       └── spark-defaults.conf
├── ingestion/
│   ├── fetch_statsbomb.py       # pull raw match data
│   └── utils.py
├── jobs/
│   ├── bronze_to_silver.py      # Spark: Bronze → Silver
│   └── silver_to_gold.py        # Spark: Silver → Gold
├── sql/
│   └── analytics/               # Dremio analytical queries
├── notebooks/
│   └── exploration.ipynb
├── docs/
│   └── DEEP_DIVE.md             # architectural decisions explained
├── tests/
│   └── quality_checks.py
└── README.md
```

---

## Quick start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- 8 GB RAM minimum

### 1. Clone and start the stack

```bash
git clone https://github.com/yourusername/pitchlake.git
cd pitchlake
docker compose up -d
```

### 2. Verify services

| Service | URL | Credentials |
|---|---|---|
| MinIO console | http://localhost:9001 | <!-- add --> |
| Nessie API | http://localhost:19120/api/v1 | — |
| Dremio UI | http://localhost:9047 | <!-- add --> |
| Spark UI | http://localhost:4040 | — |

### 3. Run the pipeline

```bash
# Ingest raw data → Bronze
python ingestion/fetch_statsbomb.py --season 2023 --week 1

# Bronze → Silver (on a Nessie dev branch)
spark-submit jobs/bronze_to_silver.py

# Silver → Gold
spark-submit jobs/silver_to_gold.py
```

---

## Data pipeline

### Phase 1 — Infrastructure

<!-- Describe the Docker Compose setup, how services are networked, Nessie catalog config in Spark, MinIO bucket layout -->

### Phase 2 — Bronze layer

<!-- Describe ingestion script, idempotency strategy, Bronze Iceberg schema (all string/raw), partition by season + match_week -->

**Bronze schema:**

```
<!-- paste your Iceberg DDL or schema dict here -->
```

### Phase 3 — Silver layer

<!-- Describe the Bronze → Silver Spark job, type casting, null handling, enrichment UDFs, Nessie branching used here -->

**Nessie branch flow:**

```
main
 └── dev  ← Silver writes land here first
      │    validate in Dremio
      └──► merge to main
```

### Phase 4 — Gold layer

<!-- Describe Silver → Gold Spark job, window functions used, final Gold table schemas -->

**Gold tables:**

| Table | Description | Key metrics |
|---|---|---|
| `player_per90` | Per-90 normalised stats | goals, xG, assists, key_passes |
| `player_season_agg` | Full season aggregates | total_minutes, progressive_carries, rank_in_league |
| `form_rolling` | Rolling 5-match form | avg_xG_last5, shots_on_target_pct |

---

## Deep knowledge showcases

### Iceberg schema evolution

<!-- Show adding dribble_success_rate column mid-season without rewriting existing data. Include before/after metadata diff. -->

```python
# ADD COLUMN — no data rewrite needed
spark.sql("""
    ALTER TABLE nessie.silver.players
    ADD COLUMN dribble_success_rate DOUBLE
""")
```

### Iceberg time travel

<!-- Show querying Silver AS OF a specific snapshot before gameweek 15. Include snapshot IDs and how to find them. -->

```sql
-- Query state of the table before gameweek 15 was loaded
SELECT * FROM nessie.silver.players
TIMESTAMP AS OF '2024-02-01 00:00:00'
WHERE season = '2023-24';
```

### Small file compaction

<!-- Show the small file problem (many small Parquet files from weekly loads), then rewrite_data_files procedure, then before/after file count comparison -->

```python
# Before: X files, avg size Y MB
# After rewrite_data_files: X files, avg size Y MB
```

### Nessie branching workflow

<!-- Full walkthrough: create dev branch → write → validate → merge → rollback demo -->

```bash
# Create dev branch
curl -X POST http://localhost:19120/api/v1/trees/branch \
  -d '{"name": "dev", "hash": "..."}'

# Point Spark at dev branch
spark.conf.set("spark.sql.catalog.nessie.ref", "dev")

# After validation, merge to main
curl -X POST http://localhost:19120/api/v1/trees/branch/main/merge ...
```

### Dremio reflections

<!-- Show a Gold table with no reflection (query time), then after enabling reflection (query time). Include screenshot of Dremio query profile. -->

---

## Analytical queries

<!-- 5 example SQL queries runnable in Dremio -->

### Top 10 players by xG per 90

```sql
-- TODO
```

### Progressive carries ranking by position

```sql
-- TODO
```

### xG vs goals conversion rate (over/underperformers)

```sql
-- TODO
```

### Player form trend (last 5 matches)

```sql
-- TODO
```

### Positional benchmark comparison

```sql
-- TODO
```

---

## Configuration reference

### Spark catalog config (`spark-defaults.conf`)

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1
spark.sql.catalog.nessie.ref=main
spark.sql.catalog.nessie.warehouse=s3://pitchlake/
```

### MinIO environment variables

```env
MINIO_ROOT_USER=<!-- -->
MINIO_ROOT_PASSWORD=<!-- -->
```

---

## Design decisions

> Full reasoning in [`docs/DEEP_DIVE.md`](docs/DEEP_DIVE.md)

| Decision | Choice | Why |
|---|---|---|
| Table format | Iceberg over Delta/Hudi | <!-- --> |
| Catalog | Nessie over Hive/Glue | <!-- --> |
| Partitioning strategy | season / league / match_week | <!-- --> |
| Bronze schema philosophy | All strings, schema-on-read | <!-- --> |
| Why no streaming | Weekly batch fits the use case | <!-- --> |

---

## Roadmap

- [ ] Add Airflow DAG for weekly scheduling
- [ ] Add Great Expectations data quality layer
- [ ] Add dbt models on top of Gold for semantic layer
- [ ] Streamlit dashboard on top of Dremio Gold queries
- [ ] Migrate to AWS (S3 + Glue catalog) for cloud variant

---

*Built by Kariem Abdelmoniem — Data Engineering Portfolio Project*
# football-lakehouse
