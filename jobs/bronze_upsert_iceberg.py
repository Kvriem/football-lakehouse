#!/usr/bin/env python3
"""
Bronze Iceberg upsert with idempotent partition overwrite.

Reads StatsBomb bronze parquet files and writes to Nessie/Iceberg bronze tables.
Idempotency is achieved by overwriting `match_week` partitions on each run.
"""

from __future__ import annotations

import argparse
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upsert bronze data into Iceberg tables with partition overwrite.")
    parser.add_argument(
        "--input-dir",
        default="data/bronze/statsbomb",
        help="Directory containing competitions/matches/events parquet files.",
    )
    parser.add_argument("--competition-id", type=int, required=True, help="Competition id to load.")
    parser.add_argument("--season-id", type=int, required=True, help="Season id to load.")
    parser.add_argument(
        "--match-week",
        type=int,
        default=None,
        help="Optional ISO week number filter; if omitted all weeks in input are processed.",
    )
    parser.add_argument(
        "--matches-table",
        default="nessie.bronze.matches_raw",
        help="Fully qualified Iceberg table name for matches.",
    )
    parser.add_argument(
        "--events-table",
        default="nessie.bronze.events_raw",
        help="Fully qualified Iceberg table name for events.",
    )
    return parser.parse_args()


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("bronze_upsert_iceberg").getOrCreate()


def cast_all_columns_to_string(df: DataFrame) -> DataFrame:
    """Bronze schema-on-read philosophy: persist all fields as STRING."""
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])


def table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except AnalysisException:
        return False


def latest_snapshot_id(spark: SparkSession, table_name: str) -> Optional[int]:
    try:
        row = (
            spark.sql(
                f"SELECT snapshot_id FROM {table_name}.snapshots ORDER BY committed_at DESC LIMIT 1"
            ).first()
        )
        return int(row["snapshot_id"]) if row else None
    except AnalysisException:
        return None


def create_table_if_missing(
    spark: SparkSession,
    table_name: str,
    source_df: DataFrame,
) -> None:
    if table_exists(spark, table_name):
        return

    source_df.limit(0).createOrReplaceTempView("stg_empty")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING iceberg
        PARTITIONED BY (season, match_week)
        AS
        SELECT * FROM stg_empty WHERE 1 = 0
        """
    )


def load_and_prepare(input_dir: str, competition_id: int, season_id: int, match_week: Optional[int]) -> tuple[DataFrame, DataFrame]:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Active Spark session not found.")

    matches = spark.read.parquet(f"{input_dir}/matches.parquet")
    events = spark.read.parquet(f"{input_dir}/events.parquet")

    matches = (
        matches.filter(
            (F.col("competition_id_input") == competition_id) & (F.col("season_id_input") == season_id)
        )
        .withColumn("match_date_ts", F.to_date("match_date"))
        .withColumn("season", F.col("season_id_input").cast("string"))
        .withColumn("match_week", F.weekofyear("match_date_ts").cast("string"))
        .drop("match_date_ts")
    )

    if match_week is not None:
        matches = matches.filter(F.col("match_week") == F.lit(str(match_week)))

    matches = cast_all_columns_to_string(matches)

    events = (
        events.filter(
            (F.col("competition_id_input") == competition_id) & (F.col("season_id_input") == season_id)
        )
        .withColumn("match_id", F.col("match_id").cast("string"))
        .join(matches.select("match_id", "season", "match_week"), on="match_id", how="inner")
    )
    events = cast_all_columns_to_string(events)

    return matches, events


def overwrite_partitions(spark: SparkSession, table_name: str, df: DataFrame) -> tuple[Optional[int], Optional[int]]:
    before = latest_snapshot_id(spark, table_name)
    create_table_if_missing(spark, table_name, df)
    df.writeTo(table_name).overwritePartitions()
    after = latest_snapshot_id(spark, table_name)
    return before, after


def main() -> None:
    args = parse_args()
    spark = get_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.bronze")

    matches_df, events_df = load_and_prepare(
        input_dir=args.input_dir,
        competition_id=args.competition_id,
        season_id=args.season_id,
        match_week=args.match_week,
    )

    if matches_df.rdd.isEmpty():
        print("No rows found for requested competition/season/week. Nothing to write.")
        spark.stop()
        return

    matches_before, matches_after = overwrite_partitions(spark, args.matches_table, matches_df)
    events_before, events_after = overwrite_partitions(spark, args.events_table, events_df)

    print("Bronze upsert completed.")
    print(
        args.matches_table + ":",
        f"rows={matches_df.count():,}",
        f"snapshot_before={matches_before}",
        f"snapshot_after={matches_after}",
    )
    print(
        args.events_table + ":",
        f"rows={events_df.count():,}",
        f"snapshot_before={events_before}",
        f"snapshot_after={events_after}",
    )

    spark.stop()


if __name__ == "__main__":
    main()
