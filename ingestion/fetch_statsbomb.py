#!/usr/bin/env python3
"""
StatsBomb Open Data ingestion for Bronze layer.

This script pulls raw JSON from the StatsBomb open-data repository, normalizes it
into flat pandas DataFrames, and writes Bronze outputs to local files.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

import pandas as pd
import requests


BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"


def _fetch_json(url: str) -> Any:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.json()


def _to_scalar(value: Any) -> Any:
    """Keep scalar values as-is and serialize nested values to JSON strings."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _normalize(records: list[dict[str, Any]], extra_cols: dict[str, Any] | None = None) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    df = pd.json_normalize(records, sep="__")
    if extra_cols:
        for col, value in extra_cols.items():
            df[col] = value

    # Bronze-friendly: convert any remaining complex values to JSON strings.
    for col in df.columns:
        df[col] = df[col].map(_to_scalar)

    return df


def _write_df(df: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(output_path, index=False)
        return output_path
    except Exception:
        fallback = output_path.with_suffix(".csv")
        df.to_csv(fallback, index=False)
        return fallback


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest StatsBomb open-data into Bronze files.")
    parser.add_argument("--competition-id", type=int, required=True, help="StatsBomb competition_id")
    parser.add_argument("--season-id", type=int, required=True, help="StatsBomb season_id")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/bronze/statsbomb"),
        help="Directory to write bronze files",
    )
    parser.add_argument(
        "--max-matches",
        type=int,
        default=None,
        help="Optional cap for number of matches (useful for quick tests)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ingest_ts = dt.datetime.now(dt.timezone.utc).isoformat()

    competitions = _fetch_json(f"{BASE_URL}/competitions.json")
    matches = _fetch_json(f"{BASE_URL}/matches/{args.competition_id}/{args.season_id}.json")
    if args.max_matches:
        matches = matches[: args.max_matches]

    # Pull events per match and tag with match_id before normalizing.
    events_records: list[dict[str, Any]] = []
    for m in matches:
        match_id = m["match_id"]
        match_events = _fetch_json(f"{BASE_URL}/events/{match_id}.json")
        for event in match_events:
            event["match_id"] = match_id
            events_records.append(event)

    competitions_df = _normalize(competitions, {"ingested_at_utc": ingest_ts, "source": "statsbomb_open_data"})
    matches_df = _normalize(
        matches,
        {
            "ingested_at_utc": ingest_ts,
            "source": "statsbomb_open_data",
            "competition_id_input": args.competition_id,
            "season_id_input": args.season_id,
        },
    )
    events_df = _normalize(
        events_records,
        {
            "ingested_at_utc": ingest_ts,
            "source": "statsbomb_open_data",
            "competition_id_input": args.competition_id,
            "season_id_input": args.season_id,
        },
    )

    comp_out = _write_df(competitions_df, args.output_dir / "competitions.parquet")
    match_out = _write_df(matches_df, args.output_dir / "matches.parquet")
    events_out = _write_df(events_df, args.output_dir / "events.parquet")

    print("Ingestion complete.")
    print(f"- Competitions: {len(competitions_df):,} rows -> {comp_out}")
    print(f"- Matches: {len(matches_df):,} rows -> {match_out}")
    print(f"- Events: {len(events_df):,} rows -> {events_out}")


if __name__ == "__main__":
    main()
