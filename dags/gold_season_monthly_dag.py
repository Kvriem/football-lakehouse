"""Seasonal Gold player_season_kpis pipeline.

Schedule: 1st day of each month at 04:00 UTC.

Note: real football "seasonal" cadence is once per season. Monthly is
chosen as a conservative default so the DAG actually runs in dev. Adjust
the cron expression for your real season boundaries (for example
`0 4 1 1,4,7,10 *` to run quarterly).

Flow:
    gold_season_kpi -> gold_season_validate -> promote_gold_dev_to_dev
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
    dag_id="gold_season_kpi_monthly",
    description="Monthly Gold player_season_kpis with league-wide ranking",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule="0 4 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["footballiq", "gold", "season-kpi", "monthly"],
    params={"season_id": DEFAULT_SEASON_ID},
) as dag:

    start = EmptyOperator(task_id="start")

    gold_season_kpi = BashOperator(
        task_id="gold_season_kpi",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/gold_season_kpi_job.py",
            [
                "--season-id {{ params.season_id }}",
                "--target-branch gold_dev",
                "--base-branch dev",
            ],
        ),
    )

    gold_season_validate = BashOperator(
        task_id="gold_season_validate",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/post_write_validate.py",
            [
                "--table nessie.gold.player_season_kpis",
                "--branch gold_dev",
                "--season-id {{ params.season_id }}",
                "--grain-cols season_id player_id",
                "--min-rows 1",
            ],
        ),
    )

    promote_gold_to_dev = BashOperator(
        task_id="promote_gold_to_dev",
        bash_command=python_in_airflow(
            f"{AIRFLOW_WORKDIR}/jobs/promote_branch.py",
            ["--from-ref gold_dev", "--into-ref dev"],
        ),
        cwd=AIRFLOW_WORKDIR,
    )

    end = EmptyOperator(task_id="end")

    start >> gold_season_kpi >> gold_season_validate >> promote_gold_to_dev >> end
