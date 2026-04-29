# Airflow Orchestration

This document describes how Airflow orchestrates the FootballIQ medallion pipeline.

## Why Airflow Sits in This Stack

The notebooks and standalone scripts already produce correct Bronze/Silver/Gold tables. Airflow adds:

- Independent schedules for each KPI mart (weekly / seasonal / 5-weekly).
- Single-writer guarantees per Iceberg table via a shared pool.
- Idempotent reruns with partition overwrite.
- Branch-based promotion in Nessie after validations pass.

## DAG Topology

```text
bronze_silver_weekly        weekly  Mon 02:00 UTC
   ingest -> bronze_upsert -> silver_transform -> silver_validate -> dev_to_main

gold_match_kpi_weekly       weekly  Mon 03:00 UTC
   gold_match_kpi -> gold_match_validate -> gold_dev_to_dev

gold_season_kpi_monthly     monthly 1st @ 04:00 UTC
   gold_season_kpi -> gold_season_validate -> gold_dev_to_dev

gold_form_last5_5weeks      every 5 weeks
   gold_form_last5 -> gold_form_validate -> gold_dev_to_dev
```

All Spark tasks share the `spark_jobs` Airflow pool with `slots=1` so only one writer touches the catalog at a time.

## Branch Strategy

| Stage | Branch written | Promotion target |
|---|---|---|
| Bronze upsert | `dev` (via Spark catalog config) | merged into `main` after Silver passes |
| Silver | `dev` | merged into `main` after Silver passes |
| Gold (any of 3) | `gold_dev` | merged into `dev` after Gold passes |

This keeps every scheduled write isolated until validation succeeds, and only then promotes the change.

## How Jobs Are Executed

Airflow does not run Spark itself. It uses BashOperator to:

- Run lightweight Python (ingestion, branch promotion) inside the Airflow container.
- Run Spark jobs via `docker exec spark spark-submit ...` against the existing Spark master.

This avoids bundling a Spark client in the Airflow image and reuses the existing jar mounts.

## Files Added by This Phase

- `dags/bronze_silver_weekly_dag.py`
- `dags/gold_match_weekly_dag.py`
- `dags/gold_season_monthly_dag.py`
- `dags/gold_form_5weeks_dag.py`
- `dags/footballiq_common.py`
- `jobs/silver_job.py`
- `jobs/gold_match_kpi_job.py`
- `jobs/gold_season_kpi_job.py`
- `jobs/gold_form_last5_job.py`
- `jobs/post_write_validate.py`
- `jobs/promote_branch.py`
- `jobs/nessie_utils.py`
- `jobs/spark_session.py`
- `docker/airflow/Dockerfile`
- `scripts/run-manual-backfill.sh`

The original notebook stages remain unchanged and continue to work for ad-hoc development.

## Running Airflow Locally

Build the Airflow image and start the new services:

```bash
docker compose build airflow-init airflow-scheduler airflow-webserver
docker compose up -d airflow-postgres airflow-init airflow-scheduler airflow-webserver
```

Then open `http://localhost:8085` and log in with `admin` / `admin`.

## Triggering A Manual Backfill

```bash
./scripts/run-manual-backfill.sh 9 281 10
```

That triggers Bronze + Silver. Once it completes, trigger Gold runs as listed in the script output.

## Concurrency and Conflict Avoidance

- One pool: `spark_jobs` (slots = 1) prevents concurrent Spark writes.
- `max_active_runs=1` per DAG prevents overlapping schedule fires.
- All Spark writes use `overwritePartitions()` which is deterministic and idempotent.
- All scheduled writes go through Nessie `dev` / `gold_dev` first; promotion only happens after validation.

## What Was Intentionally Avoided

- Running notebooks in scheduled jobs (notebooks remain for development only).
- Auto-merge into `main` for every Gold run (Gold promotes only into `dev`).
- Bundling Spark client into Airflow (use Docker exec instead).

## Adjusting Schedules

| DAG | Where to change |
|---|---|
| Match weekly | `dags/gold_match_weekly_dag.py` -> `schedule="0 3 * * MON"` |
| Season cadence | `dags/gold_season_monthly_dag.py` -> `schedule="0 4 1 * *"` |
| Form every 5 weeks | `dags/gold_form_5weeks_dag.py` -> `schedule=timedelta(weeks=5)` |
| Bronze + Silver | `dags/bronze_silver_weekly_dag.py` -> `schedule="0 2 * * MON"` |
