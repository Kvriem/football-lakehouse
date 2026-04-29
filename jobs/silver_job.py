#!/usr/bin/env python3
"""Bronze -> Silver Spark job.

Mirrors the logic in `jobs/silver_clean_enrich_bronze_fresh.ipynb`:

1. Ensure target Nessie branch exists.
2. Read Bronze: `nessie.bronze.player_team_match_stats_raw`.
3. Canonicalize columns and apply typed Silver schema.
4. Apply hard quality checks (null keys, key null ratio).
5. Soft-monitor xg mean and duplicate keys.
6. Idempotent partition overwrite to `nessie.silver.player_team_match_stats`.

Run with `spark-submit` so Iceberg/Nessie/S3 jars from
`spark/conf/spark-defaults.conf` are picked up automatically.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

# Allow direct `spark-submit jobs/silver_job.py` invocation
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from nessie_utils import ensure_branch  # noqa: E402
from spark_session import build_spark  # noqa: E402


logger = logging.getLogger("silver_job")


SILVER_TABLE = "nessie.silver.player_team_match_stats"
BRONZE_TABLE = "nessie.bronze.player_team_match_stats_raw"

CANONICAL_COLUMNS = [
    "match_id",
    "player_id",
    "team_id",
    "season_id",
    "match_week",
    "player_name",
    "team_name",
    "competition_id",
    "source",
    "event_count",
    "pass_count",
    "shot_count",
    "goal_count",
    "assist_count",
    "shot_assist_count",
    "xg",
    "ingested_at_utc",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze -> Silver Spark job")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument(
        "--match-week",
        type=int,
        default=None,
        help="Optional ISO week filter; if omitted all weeks present in Bronze are processed.",
    )
    parser.add_argument(
        "--target-branch",
        default="dev",
        help="Nessie branch to write Silver into.",
    )
    parser.add_argument(
        "--base-branch",
        default="main",
        help="Nessie branch used as parent when creating the target branch.",
    )
    parser.add_argument(
        "--null-ratio-threshold",
        type=float,
        default=0.01,
        help="Maximum allowed null-key ratio before failing the run.",
    )
    return parser.parse_args()


def canonicalize(bronze_df: DataFrame) -> DataFrame:
    return bronze_df.select(
        F.col("match_id").alias("match_id"),
        F.col("player__id").alias("player_id"),
        F.col("player__name").alias("player_name"),
        F.col("team__id").alias("team_id"),
        F.col("team__name").alias("team_name"),
        F.col("season_id_input").alias("season_id"),
        F.col("match_week").alias("match_week"),
        F.col("competition_id_input").alias("competition_id"),
        F.col("source").alias("source"),
        F.col("event_count").alias("event_count"),
        F.col("pass_count").alias("pass_count"),
        F.col("shot_count").alias("shot_count"),
        F.col("goal_count").alias("goal_count"),
        F.col("assist_count").alias("assist_count"),
        F.col("shot_assist_count").alias("shot_assist_count"),
        F.col("xg").alias("xg"),
        F.col("ingested_at_utc").alias("ingested_at_utc"),
    )


def apply_typed_contract(df: DataFrame) -> DataFrame:
    int_cols = [
        "match_id",
        "player_id",
        "team_id",
        "season_id",
        "match_week",
        "competition_id",
        "event_count",
        "pass_count",
        "shot_count",
        "goal_count",
        "assist_count",
        "shot_assist_count",
    ]
    typed = df
    for col in int_cols:
        typed = typed.withColumn(col, F.col(col).cast("int"))
    typed = typed.withColumn("xg", F.col("xg").cast("double"))
    typed = typed.withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc"))
    return typed.select(*CANONICAL_COLUMNS)


def apply_quality_filters(df: DataFrame) -> DataFrame:
    keys = ["match_id", "player_id", "team_id", "season_id", "match_week"]
    cleaned = df
    for key in keys:
        cleaned = cleaned.filter(F.col(key).isNotNull())
    return cleaned.filter(F.col("xg") >= 0)


def hard_checks(df: DataFrame, null_ratio_threshold: float) -> int:
    valid_count = df.count()
    if valid_count == 0:
        raise RuntimeError("Hard check failed: no valid Silver rows.")

    null_keys = df.filter(
        F.col("match_id").isNull()
        | F.col("player_id").isNull()
        | F.col("team_id").isNull()
    ).count()
    null_ratio = null_keys / valid_count
    if null_ratio > null_ratio_threshold:
        raise RuntimeError(
            f"Hard check failed: key null ratio {null_ratio:.2%} > {null_ratio_threshold:.2%}"
        )
    return valid_count


def soft_observability(df: DataFrame) -> None:
    xg_mean_row = df.agg(F.mean("xg").alias("xg_mean")).first()
    duplicate_keys = (
        df.groupBy("match_id", "player_id", "team_id")
        .count()
        .filter("count > 1")
        .count()
    )
    logger.info(
        "[SOFT] xg_mean=%s duplicate_keys=%s",
        xg_mean_row["xg_mean"] if xg_mean_row else None,
        duplicate_keys,
    )


def ensure_silver_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            match_id INT,
            player_id INT,
            player_name STRING,
            team_id INT,
            team_name STRING,
            season_id INT,
            match_week INT,
            competition_id INT,
            source STRING,
            event_count INT,
            pass_count INT,
            shot_count INT,
            goal_count INT,
            assist_count INT,
            shot_assist_count INT,
            xg DOUBLE,
            ingested_at_utc TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (season_id, match_week)
        TBLPROPERTIES (
          'write.distribution-mode'='hash',
          'write.target-file-size-bytes'='268435456',
          'history.expire.max-snapshot-age-ms'='604800000',
          'history.expire.min-snapshots-to-keep'='5',
          'write.delete.mode'='merge-on-read',
          'write.update.mode'='merge-on-read',
          'write.merge.mode'='merge-on-read'
        )
        """
    )


def filter_scope(
    df: DataFrame,
    season_id: int,
    match_week: Optional[int],
) -> DataFrame:
    scoped = df.filter(F.col("season_id") == F.lit(season_id))
    if match_week is not None:
        scoped = scoped.filter(F.col("match_week") == F.lit(match_week))
    return scoped


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()

    ensure_branch(args.target_branch, base=args.base_branch)

    spark = build_spark("silver_job", nessie_ref=args.target_branch)

    bronze_df = spark.table(BRONZE_TABLE)
    canonical_df = canonicalize(bronze_df)
    typed_df = apply_typed_contract(canonical_df)
    cleaned_df = apply_quality_filters(typed_df)

    scoped_df = filter_scope(cleaned_df, args.season_id, args.match_week)

    if scoped_df.rdd.isEmpty():
        logger.warning(
            "No Bronze rows match season_id=%s match_week=%s. Nothing to write.",
            args.season_id,
            args.match_week,
        )
        spark.stop()
        return

    valid_count = hard_checks(scoped_df, args.null_ratio_threshold)
    soft_observability(scoped_df)

    ensure_silver_table(spark)

    scoped_df.writeTo(SILVER_TABLE).overwritePartitions()

    written_count = spark.table(SILVER_TABLE).filter(
        F.col("season_id") == F.lit(args.season_id)
    ).count()

    logger.info(
        "Silver write OK: scope_rows=%s table_rows_in_season=%s table=%s branch=%s",
        valid_count,
        written_count,
        SILVER_TABLE,
        args.target_branch,
    )

    spark.stop()


if __name__ == "__main__":
    main()
