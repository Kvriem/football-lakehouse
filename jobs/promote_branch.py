#!/usr/bin/env python3
"""Promote a Nessie source ref into a target ref via merge.

Used by Airflow DAGs as the final task once writes and validations
succeed. Intentionally a thin wrapper around `nessie_utils.merge_branch`
so it can be invoked from BashOperator without spinning up Spark.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from nessie_utils import merge_branch  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge one Nessie branch into another.")
    parser.add_argument("--from-ref", required=True)
    parser.add_argument("--into-ref", required=True)
    parser.add_argument(
        "--from-hash",
        default=None,
        help="Optional source commit hash to merge deterministically.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    merge_branch(args.from_ref, args.into_ref, from_hash=args.from_hash)


if __name__ == "__main__":
    main()
