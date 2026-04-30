#!/usr/bin/env python3
"""Gold (player_match_kpis) -> Gold (player_form_last5) Spark job.

Computes rolling 5-match form metrics per player and a `form_trend` flag
(improving / stable / declining) using ordered windows. Idempotent
partition overwrite by (season_id, match_week).
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
from pyspark.sql.window import Window  # noqa: E402

from nessie_utils import ensure_branch  # noqa: E402
from spark_session import build_spark  # noqa: E402


logger = logging.getLogger("gold_form_last5_job")

MATCH_KPI_TABLE = "nessie.gold.player_match_kpis"
FORM_TABLE = "nessie.gold.player_form_last5"

KEY_COLS = ["season_id", "match_week", "match_id", "player_id"]
NON_NEGATIVE_COLS = [
    "goal_count",
    "assist_count",
    "shot_count",
    "xg",
    "goals_last5",
    "assists_last5",
    "shots_last5",
    "xg_last5",
    "goal_contribution_last5",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold match -> Gold form last5 job")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument(
        "--match-week",
        type=int,
        default=None,
        help="Optional ISO week filter; if omitted all weeks present in upstream are processed.",
    )
    parser.add_argument("--target-branch", default="gold_dev")
    parser.add_argument("--base-branch", default="dev")
    return parser.parse_args()


def build_form_last5(match_kpis_df: DataFrame) -> DataFrame:
    base = match_kpis_df.select(
        "season_id",
        "match_week",
        "competition_id",
        "match_id",
        "player_id",
        "player_name",
        "team_id",
        "team_name",
        "goal_count",
        "assist_count",
        "shot_count",
        "xg",
        "goal_contribution",
    )

    # Use match_id for ordering to avoid ISO week wrap issues at year boundaries.
    w_last5 = (
        Window.partitionBy("season_id", "player_id")
        .orderBy(F.col("match_id").asc())
        .rowsBetween(-4, 0)
    )

    rolling = (
        base.withColumn("matches_in_window", F.count(F.lit(1)).over(w_last5))
        .withColumn("goals_last5", F.sum("goal_count").over(w_last5))
        .withColumn("assists_last5", F.sum("assist_count").over(w_last5))
        .withColumn("shots_last5", F.sum("shot_count").over(w_last5))
        .withColumn("xg_last5", F.sum("xg").over(w_last5))
        .withColumn("goal_contribution_last5", F.sum("goal_contribution").over(w_last5))
        .withColumn("avg_xg_last5", F.avg("xg").over(w_last5))
        .withColumn(
            "shot_conversion_last5",
            F.when(
                F.col("shots_last5") > 0,
                F.col("goals_last5") / F.col("shots_last5"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("first_xg_in_window", F.first("xg", ignorenulls=True).over(w_last5))
        .withColumn("last_xg_in_window", F.last("xg", ignorenulls=True).over(w_last5))
        .withColumn(
            "form_trend",
            F.when(
                F.col("last_xg_in_window") > F.col("first_xg_in_window"),
                F.lit("improving"),
            )
            .when(
                F.col("last_xg_in_window") < F.col("first_xg_in_window"),
                F.lit("declining"),
            )
            .otherwise(F.lit("stable")),
        )
        .drop("first_xg_in_window", "last_xg_in_window")
    )

    return rolling.filter(F.col("matches_in_window") == 5)


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {FORM_TABLE} (
            season_id INT,
            match_week INT,
            competition_id INT,
            match_id INT,
            player_id INT,
            player_name STRING,
            team_id INT,
            team_name STRING,
            goal_count INT,
            assist_count INT,
            shot_count INT,
            xg DOUBLE,
            goal_contribution INT,
            matches_in_window BIGINT,
            goals_last5 BIGINT,
            assists_last5 BIGINT,
            shots_last5 BIGINT,
            xg_last5 DOUBLE,
            goal_contribution_last5 BIGINT,
            avg_xg_last5 DOUBLE,
            shot_conversion_last5 DOUBLE,
            form_trend STRING
        )
        USING iceberg
        PARTITIONED BY (season_id, match_week)
        """
    )


def gold_quality_checks(df: DataFrame) -> int:
    total = df.count()
    if total == 0:
        raise RuntimeError(f"{FORM_TABLE}: hard check failed, no rows.")

    null_filter = " OR ".join([f"{c} IS NULL" for c in KEY_COLS])
    if df.filter(null_filter).count() > 0:
        raise RuntimeError(f"{FORM_TABLE}: hard check failed, null keys present.")

    neg_filter = " OR ".join([f"{c} < 0" for c in NON_NEGATIVE_COLS])
    if df.filter(neg_filter).count() > 0:
        raise RuntimeError(f"{FORM_TABLE}: hard check failed, negative rolling rows.")

    duplicates = df.groupBy(*KEY_COLS).count().filter("count > 1").count()
    if duplicates > 0:
        raise RuntimeError(
            f"{FORM_TABLE}: hard check failed, duplicate grain rows={duplicates}"
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

    spark = build_spark("gold_form_last5_job", nessie_ref=args.target_branch)

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

    form_df = build_form_last5(match_kpis_df)
    scoped_df = filter_scope(form_df, args.season_id, args.match_week)

    if scoped_df.rdd.isEmpty():
        logger.warning(
            "No 5-match window rows for season_id=%s match_week=%s. "
            "Need at least 5 matches per player to write.",
            args.season_id,
            args.match_week,
        )
        spark.stop()
        return

    row_count = gold_quality_checks(scoped_df)

    ensure_table(spark)
    scoped_df.writeTo(FORM_TABLE).overwritePartitions()

    logger.info(
        "Gold form last5 write OK: rows=%s table=%s branch=%s",
        row_count,
        FORM_TABLE,
        args.target_branch,
    )

    spark.stop()


if __name__ == "__main__":
    main()
