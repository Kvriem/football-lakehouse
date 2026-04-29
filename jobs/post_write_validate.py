#!/usr/bin/env python3
"""Lightweight post-write validation.

Reads a Nessie/Iceberg table on a given branch and asserts:
  - row count > 0 for the given scope
  - no duplicates on the configured grain

This runs as its own spark-submit so DAGs can validate writes
independently from the writing job.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pyspark.sql import functions as F  # noqa: E402

from spark_session import build_spark  # noqa: E402


logger = logging.getLogger("post_write_validate")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-write Iceberg table validator.")
    parser.add_argument("--table", required=True, help="Fully qualified table name.")
    parser.add_argument("--branch", required=True, help="Nessie branch to read from.")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument(
        "--match-week",
        type=int,
        default=None,
        help="Optional ISO week to scope validation.",
    )
    parser.add_argument(
        "--grain-cols",
        nargs="+",
        required=True,
        help="Columns whose combination should be unique.",
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        default=1,
        help="Minimum row count required in scope.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()

    spark = build_spark("post_write_validate", nessie_ref=args.branch)

    df = spark.table(args.table).filter(F.col("season_id") == F.lit(args.season_id))
    if args.match_week is not None:
        df = df.filter(F.col("match_week") == F.lit(args.match_week))

    total = df.count()
    if total < args.min_rows:
        raise RuntimeError(
            f"Validation failed: {args.table}@{args.branch} rows={total} < min_rows={args.min_rows}"
        )

    duplicates = df.groupBy(*args.grain_cols).count().filter("count > 1").count()
    if duplicates > 0:
        raise RuntimeError(
            f"Validation failed: {args.table}@{args.branch} duplicate grain rows={duplicates}"
        )

    logger.info(
        "Validation OK: table=%s branch=%s rows=%s duplicates=%s",
        args.table,
        args.branch,
        total,
        duplicates,
    )

    spark.stop()


if __name__ == "__main__":
    main()
