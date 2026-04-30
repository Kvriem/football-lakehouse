#!/usr/bin/env python3
"""Print current hash for a Nessie branch.

Used in Airflow DAGs to capture a validated source hash and pass it into
promotion steps for deterministic merges.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from nessie_utils import get_branch_hash  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve Nessie branch hash.")
    parser.add_argument("--ref", required=True, help="Nessie branch name")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print(get_branch_hash(args.ref))


if __name__ == "__main__":
    main()
