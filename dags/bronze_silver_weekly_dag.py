"""Weekly Bronze + Silver pipeline.

Schedule: every Monday at 02:00 UTC.

Flow:
    ingest_statsbomb -> bronze_upsert -> silver_transform -> promote_dev_to_main

Writes land first on Nessie `dev`. After successful Silver validation,
`dev` is merged into `main` so downstream Gold DAGs (which read from
Silver) see a consistent snapshot.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAGS_DIR = str(Path(__file__).resolve().parent)
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

from footballiq_common import (  # noqa: E402
    AIRFLOW_WORKDIR,
    DEFAULT_COMPETITION_ID,
    DEFAULT_DAG_ARGS,
    DEFAULT_SEASON_ID,
    SPARK_POOL,
    SPARK_WORKDIR,
    docker_exec,
    python_in_airflow,
    spark_submit,
)


with DAG(
    dag_id="bronze_silver_weekly",
    description="Weekly StatsBomb ingestion -> Bronze Iceberg -> Silver Iceberg",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="0 2 * * MON",
    catchup=False,
    max_active_runs=1,
    tags=["footballiq", "bronze", "silver", "weekly"],
    params={
        "competition_id": DEFAULT_COMPETITION_ID,
        "season_id": DEFAULT_SEASON_ID,
        "max_matches": 10,
    },
) as dag:

    start = EmptyOperator(task_id="start")

    ingest_statsbomb = BashOperator(
        task_id="ingest_statsbomb",
        bash_command=python_in_airflow(
            f"{AIRFLOW_WORKDIR}/ingestion/fetch_statsbomb.py",
            [
                "--competition-id {{ params.competition_id }}",
                "--season-id {{ params.season_id }}",
                "--max-matches {{ params.max_matches }}",
                f"--output-dir {AIRFLOW_WORKDIR}/data/bronze/statsbomb",
            ],
        ),
        cwd=AIRFLOW_WORKDIR,
    )

    bronze_upsert = BashOperator(
        task_id="bronze_upsert",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/bronze_upsert_iceberg.py",
            [
                f"--input-dir {SPARK_WORKDIR}/data/bronze/statsbomb",
                "--competition-id {{ params.competition_id }}",
                "--season-id {{ params.season_id }}",
            ],
        ),
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/silver_job.py",
            [
                "--season-id {{ params.season_id }}",
                "--target-branch dev",
                "--base-branch main",
            ],
        ),
    )

    silver_validate = BashOperator(
        task_id="silver_validate",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/post_write_validate.py",
            [
                "--table nessie.silver.player_team_match_stats",
                "--branch dev",
                "--season-id {{ params.season_id }}",
                "--grain-cols season_id match_week match_id player_id team_id",
                "--min-rows 1",
            ],
        ),
    )

    promote_dev_to_main = BashOperator(
        task_id="promote_dev_to_main",
        bash_command=python_in_airflow(
            f"{AIRFLOW_WORKDIR}/jobs/promote_branch.py",
            ["--from-ref dev", "--into-ref main"],
        ),
        cwd=AIRFLOW_WORKDIR,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> ingest_statsbomb
        >> bronze_upsert
        >> silver_transform
        >> silver_validate
        >> promote_dev_to_main
        >> end
    )
