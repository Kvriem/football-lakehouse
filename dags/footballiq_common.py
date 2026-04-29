"""Shared constants and helpers for FootballIQ Airflow DAGs.

All DAGs use Spark via `docker exec` into the existing `spark` container.
This avoids bundling a Spark client inside the Airflow image and keeps
the Iceberg/Nessie/S3 jar setup unchanged.
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Iterable, List

# --- Static config ---------------------------------------------------------

# Default scope for scheduled runs. Override at trigger time via params.
DEFAULT_COMPETITION_ID = int(os.environ.get("FOOTBALLIQ_COMPETITION_ID", "9"))
DEFAULT_SEASON_ID = int(os.environ.get("FOOTBALLIQ_SEASON_ID", "281"))

# Container that runs Spark / spark-submit.
SPARK_CONTAINER = os.environ.get("FOOTBALLIQ_SPARK_CONTAINER", "spark")
SPARK_MASTER = os.environ.get("FOOTBALLIQ_SPARK_MASTER", "spark://spark:7077")

# Repo path inside the spark container (matches the docker-compose mount).
SPARK_WORKDIR = "/opt/spark/work-dir"

# Repo path inside the airflow container (matches the docker-compose mount).
AIRFLOW_WORKDIR = "/opt/airflow/work-dir"

# Single shared pool name. Limit = 1 ensures only one Spark write happens
# at a time across all DAGs to avoid catalog contention on the same tables.
SPARK_POOL = "spark_jobs"

# --- Default DAG arguments -------------------------------------------------

DEFAULT_DAG_ARGS = {
    "owner": "footballiq",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=45),
}


# --- Bash command builders -------------------------------------------------


def docker_exec(command: str) -> str:
    """Wrap a command so it runs inside the spark container."""
    return f'docker exec {SPARK_CONTAINER} bash -lc "{command}"'


def spark_submit(script_path_in_container: str, args: Iterable[str]) -> str:
    """Build a spark-submit command for a Python job inside the spark container."""
    flat_args: List[str] = list(args)
    args_str = " ".join(flat_args)
    return docker_exec(
        f"/opt/spark/bin/spark-submit "
        f"--master {SPARK_MASTER} "
        f"{script_path_in_container} {args_str}"
    )


def python_in_airflow(script_path: str, args: Iterable[str]) -> str:
    """Build a python invocation that runs inside the airflow worker."""
    args_str = " ".join(list(args))
    return f"python {script_path} {args_str}"
