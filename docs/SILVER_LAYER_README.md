# Silver Layer Summary

This document summarizes what is implemented in `jobs/silver_clean_enrich_bronze_fresh.ipynb`: core topics, transformation logic, sample outputs, and strongest engineering points.

## Topics Implemented

- Nessie branch workflow (`main` -> `dev`) with idempotent branch creation checks
- Spark + Iceberg + Nessie + MinIO runtime configuration
- Bronze profiling and schema inspection
- Canonical Silver schema mapping
- Explicit type casting (string Bronze -> typed Silver)
- Data quality contract with hard and soft checks
- Idempotent Silver writes using Iceberg partition overwrite
- Iceberg table property tuning
- Partition-level compaction validation (`rewrite_data_files`)

## Strong Points (Depth)

- Clear Bronze -> Silver contract enforcement (canonical + typed schema)
- Practical quality strategy (hard failure + soft observability)
- Correct idempotent incremental write pattern with partition overwrite
- Iceberg operational thinking (table properties + compaction check)
- Catalog governance mindset via Nessie branch-based workflow

## Core Logic (End-to-End)

1. Connect to Nessie and ensure target branch exists.
2. Start Spark session with Iceberg/Nessie catalog and S3/MinIO config.
3. Read Bronze source table: `nessie.bronze.player_team_match_stats_raw`.
4. Rename Bronze columns to canonical Silver names.
5. Cast to target data types (`INT`, `DOUBLE`, `TIMESTAMP`).
6. Apply quality filters:
   - required keys non-null
   - `xg >= 0`
7. Run checks:
   - hard fail if no valid rows
   - hard fail if key null ratio > 1%
   - soft monitor for `xg_mean` and duplicate key count
8. Create Silver Iceberg table (if missing) partitioned by:
   - `season_id`
   - `match_week`
9. Write using idempotent pattern:
   - `overwritePartitions()`
10. Validate partition row counts and table properties.
11. Run compaction test for one partition and compare before/after file metrics.

## Silver Schema Logic

### Canonicalization examples

- `player__id` -> `player_id`
- `player__name` -> `player_name`
- `team__id` -> `team_id`
- `team__name` -> `team_name`
- `season_id_input` -> `season_id`
- `competition_id_input` -> `competition_id`

### Typed contract

- IDs and count metrics -> `INT`
- `xg` -> `DOUBLE`
- `ingested_at_utc` -> `TIMESTAMP`

## Data Quality Logic

### Hard checks

- `valid_count > 0`
- key null ratio threshold <= 1%

### Soft checks

- monitor `xg_mean`
- monitor duplicates on `(match_id, player_id, team_id)`

## Idempotency Strategy Implemented

Current strategy is partition overwrite (Option A):

- Silver table partitioned by `(season_id, match_week)`
- reruns replace touched partitions without accumulating duplicates

Write pattern:

- `silver_scope_df.writeTo("nessie.silver.player_team_match_stats").overwritePartitions()`

## Iceberg Features Applied

- Silver table created as Iceberg table (`format-version=2`)
- Table properties configured:
  - `write.distribution-mode=hash`
  - `write.target-file-size-bytes=268435456`
  - `history.expire.max-snapshot-age-ms=604800000`
  - `history.expire.min-snapshots-to-keep=5`
  - merge-on-read modes for update/delete/merge

## Sample Results from Current Run

### Data profile sample

- Bronze rows observed in run: `306`
- Soft check output:
  - `xg_mean=0.10562219986764704`
  - `duplicate_keys=0`

### Silver partition distribution sample

- `season_id=281`
- `match_week` counts:
  - 5 -> 30
  - 8 -> 29
  - 10 -> 31
  - 11 -> 31
  - 13 -> 30
  - 14 -> 30
  - 15 -> 30
  - 18 -> 32
  - 19 -> 31
  - 20 -> 32

### Compaction sample

For partition `(season_id=281, match_week=14)`:

- before: `file_count=1`, `avg_size_mb=0.01`
- `rewrite_data_files` result: no files rewritten (`0`)
- after: unchanged

Interpretation: partition already had minimal files for this run.

## Current Limitations 
- Compaction demonstration on selected partition is a no-op due to already small file count.
