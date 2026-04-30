"""Shared Spark session factory used by Airflow-driven jobs.

Most catalog properties come from `spark/conf/spark-defaults.conf`. This
helper only overrides the Nessie ref (branch) per job so writes can be
isolated to `dev`, `gold_dev`, etc.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def build_spark(app_name: str, nessie_ref: str) -> SparkSession:
    """Create or reuse a SparkSession pinned to the requested Nessie branch."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.catalog.nessie.ref", nessie_ref)
        .getOrCreate()
    )
