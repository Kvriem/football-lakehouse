#!/usr/bin/env python3
"""Silver -> Gold (player_match_kpis) Spark job.

Builds the player-match KPI mart at the same grain as Silver, adding
contribution and efficiency KPIs. Idempotent partition overwrite by
(season_id, match_week).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from nessie_utils import ensure_branch  # noqa: E402
from spark_session import build_spark  # noqa: E402


logger = logging.getLogger("gold_match_kpi_job")

SILVER_TABLE = "nessie.silver.player_team_match_stats"
GOLD_TABLE = "nessie.gold.player_match_kpis"

KEY_COLS = ["season_id", "match_week", "match_id", "player_id"]
NON_NEGATIVE_COLS = [
    "event_count",
    "pass_count",
    "shot_count",
    "goal_count",
    "assist_count",
    "shot_assist_count",
    "xg",
    "goal_contribution",
    "involvement_index",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver -> Gold player_match_kpis")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument("--match-week", type=int, default=None)
    parser.add_argument("--target-branch", default="gold_dev")
    parser.add_argument("--base-branch", default="dev")
    return parser.parse_args()


def build_match_kpis(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(
            "season_id",
            "match_week",
            "competition_id",
            "match_id",
            "player_id",
            "player_name",
            "team_id",
            "team_name",
            "event_count",
            "pass_count",
            "shot_count",
            "goal_count",
            "assist_count",
            "shot_assist_count",
            "xg",
        )
        .groupBy(
            "season_id",
            "match_week",
            "competition_id",
            "match_id",
            "player_id",
            "player_name",
            "team_id",
            "team_name",
        )
        .agg(
            F.sum("event_count").alias("event_count"),
            F.sum("pass_count").alias("pass_count"),
            F.sum("shot_count").alias("shot_count"),
            F.sum("goal_count").alias("goal_count"),
            F.sum("assist_count").alias("assist_count"),
            F.sum("shot_assist_count").alias("shot_assist_count"),
            F.sum("xg").alias("xg"),
        )
        .withColumn("goal_contribution", F.col("goal_count") + F.col("assist_count"))
        .withColumn(
            "involvement_index",
            F.col("goal_count") * F.lit(4)
            + F.col("assist_count") * F.lit(3)
            + F.col("shot_count")
            + F.col("shot_assist_count"),
        )
        .withColumn(
            "xg_per_shot",
            F.when(
                F.col("shot_count") > 0,
                F.col("xg") / F.col("shot_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "shot_conversion",
            F.when(
                F.col("shot_count") > 0,
                F.col("goal_count") / F.col("shot_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "shot_assist_ratio",
            F.when(
                F.col("pass_count") > 0,
                F.col("shot_assist_count") / F.col("pass_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
    )


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            season_id INT,
            match_week INT,
            competition_id INT,
            match_id INT,
            player_id INT,
            player_name STRING,
            team_id INT,
            team_name STRING,
            event_count BIGINT,
            pass_count BIGINT,
            shot_count BIGINT,
            goal_count BIGINT,
            assist_count BIGINT,
            shot_assist_count BIGINT,
            xg DOUBLE,
            goal_contribution BIGINT,
            involvement_index BIGINT,
            xg_per_shot DOUBLE,
            shot_conversion DOUBLE,
            shot_assist_ratio DOUBLE
        )
        USING iceberg
        PARTITIONED BY (season_id, match_week)
        """
    )


def gold_quality_checks(df: DataFrame) -> int:
    total = df.count()
    if total == 0:
        raise RuntimeError(f"{GOLD_TABLE}: hard check failed, no rows.")

    null_filter = " OR ".join([f"{c} IS NULL" for c in KEY_COLS])
    if df.filter(null_filter).count() > 0:
        raise RuntimeError(f"{GOLD_TABLE}: hard check failed, null keys present.")

    neg_filter = " OR ".join([f"{c} < 0" for c in NON_NEGATIVE_COLS])
    if df.filter(neg_filter).count() > 0:
        raise RuntimeError(f"{GOLD_TABLE}: hard check failed, negative KPI rows.")

    duplicates = df.groupBy(*KEY_COLS).count().filter("count > 1").count()
    if duplicates > 0:
        raise RuntimeError(
            f"{GOLD_TABLE}: hard check failed, duplicate grain rows={duplicates}"
        )

    return total


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

    spark = build_spark("gold_match_kpi_job", nessie_ref=args.target_branch)

    silver_df = spark.table(SILVER_TABLE)
    scoped_silver = filter_scope(silver_df, args.season_id, args.match_week)

    if scoped_silver.rdd.isEmpty():
        logger.warning(
            "No Silver rows for season_id=%s match_week=%s. Nothing to write.",
            args.season_id,
            args.match_week,
        )
        spark.stop()
        return

    match_kpis_df = build_match_kpis(scoped_silver)
    row_count = gold_quality_checks(match_kpis_df)

    ensure_table(spark)
    match_kpis_df.writeTo(GOLD_TABLE).overwritePartitions()

    logger.info(
        "Gold match KPI write OK: rows=%s table=%s branch=%s",
        row_count,
        GOLD_TABLE,
        args.target_branch,
    )

    spark.stop()


if __name__ == "__main__":
    main()
