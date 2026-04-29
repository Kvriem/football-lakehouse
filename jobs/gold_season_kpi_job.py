#!/usr/bin/env python3
"""Gold (player_match_kpis) -> Gold (player_season_kpis) Spark job.

Aggregates match-level KPIs to player-season grain, computes weighted
season metrics, and applies dense_rank windows for league-wide ranking
within each season. Idempotent partition overwrite by season_id.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from nessie_utils import ensure_branch  # noqa: E402
from spark_session import build_spark  # noqa: E402


logger = logging.getLogger("gold_season_kpi_job")

MATCH_KPI_TABLE = "nessie.gold.player_match_kpis"
SEASON_KPI_TABLE = "nessie.gold.player_season_kpis"

KEY_COLS = ["season_id", "player_id"]
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
    "matches_played",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold match -> Gold season KPI job")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument("--target-branch", default="gold_dev")
    parser.add_argument("--base-branch", default="dev")
    return parser.parse_args()


def build_season_kpis(match_kpis_df: DataFrame) -> DataFrame:
    aggregated = (
        match_kpis_df.groupBy("season_id", "player_id")
        .agg(
            F.first("player_name", ignorenulls=True).alias("player_name"),
            F.first("team_id", ignorenulls=True).alias("team_id"),
            F.first("team_name", ignorenulls=True).alias("team_name"),
            F.sum("event_count").alias("event_count"),
            F.sum("pass_count").alias("pass_count"),
            F.sum("shot_count").alias("shot_count"),
            F.sum("goal_count").alias("goal_count"),
            F.sum("assist_count").alias("assist_count"),
            F.sum("shot_assist_count").alias("shot_assist_count"),
            F.sum("xg").alias("xg"),
            F.sum("goal_contribution").alias("goal_contribution"),
            F.sum("involvement_index").alias("involvement_index"),
            F.countDistinct("match_id").alias("matches_played"),
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

    w_goal = Window.partitionBy("season_id").orderBy(F.col("goal_contribution").desc())
    w_eff = Window.partitionBy("season_id").orderBy(F.col("xg_per_shot").desc_nulls_last())
    w_conv = Window.partitionBy("season_id").orderBy(F.col("shot_conversion").desc_nulls_last())
    w_asr = Window.partitionBy("season_id").orderBy(F.col("shot_assist_ratio").desc_nulls_last())

    return (
        aggregated.withColumn("goal_contribution_rank", F.dense_rank().over(w_goal))
        .withColumn("efficiency_rank", F.dense_rank().over(w_eff))
        .withColumn("conversion_rank", F.dense_rank().over(w_conv))
        .withColumn("assist_ratio_rank", F.dense_rank().over(w_asr))
    )


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SEASON_KPI_TABLE} (
            season_id INT,
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
            matches_played BIGINT,
            xg_per_shot DOUBLE,
            shot_conversion DOUBLE,
            shot_assist_ratio DOUBLE,
            goal_contribution_rank INT,
            efficiency_rank INT,
            conversion_rank INT,
            assist_ratio_rank INT
        )
        USING iceberg
        PARTITIONED BY (season_id)
        """
    )


def gold_quality_checks(df: DataFrame) -> int:
    total = df.count()
    if total == 0:
        raise RuntimeError(f"{SEASON_KPI_TABLE}: hard check failed, no rows.")

    null_filter = " OR ".join([f"{c} IS NULL" for c in KEY_COLS])
    if df.filter(null_filter).count() > 0:
        raise RuntimeError(f"{SEASON_KPI_TABLE}: hard check failed, null keys present.")

    neg_filter = " OR ".join([f"{c} < 0" for c in NON_NEGATIVE_COLS])
    if df.filter(neg_filter).count() > 0:
        raise RuntimeError(
            f"{SEASON_KPI_TABLE}: hard check failed, negative KPI rows present."
        )

    duplicates = df.groupBy(*KEY_COLS).count().filter("count > 1").count()
    if duplicates > 0:
        raise RuntimeError(
            f"{SEASON_KPI_TABLE}: hard check failed, duplicate grain rows={duplicates}"
        )

    return total


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()

    ensure_branch(args.target_branch, base=args.base_branch)

    spark = build_spark("gold_season_kpi_job", nessie_ref=args.target_branch)

    match_kpis_df = spark.table(MATCH_KPI_TABLE).filter(
        F.col("season_id") == F.lit(args.season_id)
    )

    if match_kpis_df.rdd.isEmpty():
        logger.warning(
            "No player_match_kpis rows for season_id=%s. Nothing to write.",
            args.season_id,
        )
        spark.stop()
        return

    season_kpis_df = build_season_kpis(match_kpis_df)
    row_count = gold_quality_checks(season_kpis_df)

    ensure_table(spark)
    season_kpis_df.writeTo(SEASON_KPI_TABLE).overwritePartitions()

    logger.info(
        "Gold season KPI write OK: rows=%s table=%s branch=%s",
        row_count,
        SEASON_KPI_TABLE,
        args.target_branch,
    )

    spark.stop()


if __name__ == "__main__":
    main()
