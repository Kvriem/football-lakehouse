"""Weekly Gold player_match_kpis pipeline.

Schedule: every Monday at 03:00 UTC (one hour after Bronze+Silver).

Flow:
    gold_match_kpi -> gold_match_validate -> promote_gold_dev_to_dev

Reads from Nessie `dev` (Silver), writes to `gold_dev`, then promotes
`gold_dev` into `dev` so downstream consumers see the new mart.
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
    DEFAULT_DAG_ARGS,
    DEFAULT_SEASON_ID,
    SPARK_POOL,
    SPARK_WORKDIR,
    python_in_airflow,
    spark_submit,
)


with DAG(
    dag_id="gold_match_kpi_weekly",
    description="Weekly Silver -> Gold player_match_kpis",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="0 3 * * MON",
    catchup=False,
    max_active_runs=1,
    tags=["footballiq", "gold", "match-kpi", "weekly"],
    params={"season_id": DEFAULT_SEASON_ID},
) as dag:

    start = EmptyOperator(task_id="start")

    gold_match_kpi = BashOperator(
        task_id="gold_match_kpi",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/gold_match_kpi_job.py",
            [
                "--season-id",
                "{{ params.season_id }}",
                "--target-branch",
                "gold_dev",
                "--base-branch",
                "dev",
            ],
        ),
    )

    gold_match_validate = BashOperator(
        task_id="gold_match_validate",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/post_write_validate.py",
            [
                "--table",
                "nessie.gold.player_match_kpis",
                "--branch",
                "gold_dev",
                "--season-id",
                "{{ params.season_id }}",
                "--grain-cols",
                "season_id",
                "match_week",
                "match_id",
                "player_id",
                "--min-rows",
                "1",
            ],
        ),
    )

    promote_gold_to_dev = BashOperator(
        task_id="promote_gold_to_dev",
        pool=SPARK_POOL,
        bash_command=python_in_airflow(
            f"{AIRFLOW_WORKDIR}/jobs/promote_branch.py",
            ["--from-ref", "gold_dev", "--into-ref", "dev"],
        ),
        cwd=AIRFLOW_WORKDIR,
    )

    end = EmptyOperator(task_id="end")

    start >> gold_match_kpi >> gold_match_validate >> promote_gold_to_dev >> end
