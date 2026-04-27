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


def _build_player_team_match_stats(
    matches: list[dict[str, Any]],
    events_records: list[dict[str, Any]],
    lineups_by_match: dict[int, list[dict[str, Any]]],
    competition_id: int,
    season_id: int,
    ingest_ts: str,
) -> pd.DataFrame:
    """Create player-team-match performance dataset from raw events + lineups."""
    if not events_records:
        return pd.DataFrame()

    events_df = pd.json_normalize(events_records, sep="__")
    if events_df.empty:
        return pd.DataFrame()

    required_cols = [
        "match_id",
        "player__id",
        "player__name",
        "team__id",
        "team__name",
        "type__name",
        "shot__statsbomb_xg",
        "pass__goal_assist",
        "pass__shot_assist",
    ]
    for col in required_cols:
        if col not in events_df.columns:
            events_df[col] = None

    events_df = events_df[events_df["player__id"].notna()].copy()
    events_df["shot_xg"] = pd.to_numeric(events_df["shot__statsbomb_xg"], errors="coerce").fillna(0.0)
    events_df["is_pass"] = (events_df["type__name"] == "Pass").astype(int)
    events_df["is_shot"] = (events_df["type__name"] == "Shot").astype(int)
    events_df["is_goal"] = (
        (events_df["type__name"] == "Shot") & (events_df.get("shot__outcome__name") == "Goal")
    ).astype(int)
    events_df["is_assist"] = events_df["pass__goal_assist"].fillna(False).astype(bool).astype(int)
    events_df["is_shot_assist"] = events_df["pass__shot_assist"].fillna(False).astype(bool).astype(int)

    perf = (
        events_df.groupby(
            ["match_id", "player__id", "player__name", "team__id", "team__name"],
            dropna=False,
            as_index=False,
        )
        .agg(
            event_count=("type__name", "count"),
            pass_count=("is_pass", "sum"),
            shot_count=("is_shot", "sum"),
            goal_count=("is_goal", "sum"),
            assist_count=("is_assist", "sum"),
            shot_assist_count=("is_shot_assist", "sum"),
            xg=("shot_xg", "sum"),
        )
    )

    # Add "started" signal from lineups.
    lineup_rows: list[dict[str, Any]] = []
    for match in matches:
        match_id = int(match["match_id"])
        for team in lineups_by_match.get(match_id, []):
            team_id = team.get("team_id")
            team_name = team.get("team_name")
            for player in team.get("lineup", []):
                lineup_rows.append(
                    {
                        "match_id": match_id,
                        "player__id": player.get("player_id"),
                        "team__id": team_id,
                        "team__name": team_name,
                        "started": 1,
                    }
                )

    lineup_df = pd.DataFrame(lineup_rows)
    if not lineup_df.empty:
        perf = perf.merge(
            lineup_df.drop_duplicates(["match_id", "player__id", "team__id"]),
            on=["match_id", "player__id", "team__id", "team__name"],
            how="left",
        )
    perf["started"] = perf["started"].fillna(0).astype(int)

    # Add season and match week context.
    match_meta = pd.json_normalize(matches, sep="__")
    match_meta = match_meta[["match_id", "match_date"]].copy()
    match_meta["match_date"] = pd.to_datetime(match_meta["match_date"], errors="coerce")
    match_meta["match_week"] = match_meta["match_date"].dt.isocalendar().week.astype("Int64")
    perf = perf.merge(match_meta[["match_id", "match_week"]], on="match_id", how="left")
    perf["season"] = str(season_id)

    perf["competition_id_input"] = competition_id
    perf["season_id_input"] = season_id
    perf["ingested_at_utc"] = ingest_ts
    perf["source"] = "statsbomb_open_data"

    # Bronze-friendly scalar conversion.
    for col in perf.columns:
        perf[col] = perf[col].map(_to_scalar)

    return perf


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
    lineups_by_match: dict[int, list[dict[str, Any]]] = {}
    for m in matches:
        match_id = m["match_id"]
        match_events = _fetch_json(f"{BASE_URL}/events/{match_id}.json")
        lineups_by_match[match_id] = _fetch_json(f"{BASE_URL}/lineups/{match_id}.json")
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
    player_team_match_stats_df = _build_player_team_match_stats(
        matches=matches,
        events_records=events_records,
        lineups_by_match=lineups_by_match,
        competition_id=args.competition_id,
        season_id=args.season_id,
        ingest_ts=ingest_ts,
    )

    comp_out = _write_df(competitions_df, args.output_dir / "competitions.parquet")
    match_out = _write_df(matches_df, args.output_dir / "matches.parquet")
    events_out = _write_df(events_df, args.output_dir / "events.parquet")
    player_perf_out = _write_df(
        player_team_match_stats_df,
        args.output_dir / "player_team_match_stats.parquet",
    )

    print("Ingestion complete.")
    print(f"- Competitions: {len(competitions_df):,} rows -> {comp_out}")
    print(f"- Matches: {len(matches_df):,} rows -> {match_out}")
    print(f"- Events: {len(events_df):,} rows -> {events_out}")
    print(
        f"- PlayerTeamMatchStats: {len(player_team_match_stats_df):,} rows -> {player_perf_out}"
    )


if __name__ == "__main__":
    main()
