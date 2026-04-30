"""Every-5-weeks Gold player_form_last5 pipeline.

Schedule: every 5 weeks from the configured start_date. Cron does not
support a strict "every N weeks" cadence, so we use a fixed
`schedule=timedelta(weeks=5)` interval with `catchup=False` to prevent
backfilling many runs at first deploy.

Flow:
    gold_form_last5 -> gold_form_validate -> promote_gold_dev_to_dev
"""

from __future__ import annotations

from datetime import datetime, timedelta
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
    dag_id="gold_form_last5_5weeks",
    description="Gold player_form_last5 rolling-form mart, every 5 weeks",
    default_args=DEFAULT_DAG_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule=timedelta(weeks=5),
    catchup=False,
    max_active_runs=1,
    tags=["footballiq", "gold", "form-kpi", "5-weeks"],
    params={"season_id": DEFAULT_SEASON_ID},
) as dag:

    start = EmptyOperator(task_id="start")

    gold_form_last5 = BashOperator(
        task_id="gold_form_last5",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/gold_form_last5_job.py",
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

    gold_form_validate = BashOperator(
        task_id="gold_form_validate",
        pool=SPARK_POOL,
        bash_command=spark_submit(
            f"{SPARK_WORKDIR}/jobs/post_write_validate.py",
            [
                "--table",
                "nessie.gold.player_form_last5",
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

    start >> gold_form_last5 >> gold_form_validate >> promote_gold_to_dev >> end
