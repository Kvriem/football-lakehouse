# Bronze Layer Documentation

This document describes Bronze ingestion and Bronze Iceberg table logic for the Football Lakehouse project.

## Bronze Philosophy

Bronze follows a strict **schema-on-read** approach:

- Preserve source payload as raw as possible
- Store all business fields as `STRING`
- Defer strict typing and semantic cleanup to Silver
- Keep ingestion metadata for lineage and replayability

## Data Source

- Source: **StatsBomb open-data**
- Format: JSON files from the public repository

Ingestion script:

- `ingestion/fetch_statsbomb.py`

It pulls:

- `competitions.json`
- `matches/{competition_id}/{season_id}.json`
- `events/{match_id}.json`

## Ingestion Logic (Raw Extraction)

`fetch_statsbomb.py` does the following:

1. Fetches source JSON from StatsBomb open-data URLs
2. Flattens nested structures using `pandas.json_normalize`
3. Serializes complex nested/list values into JSON strings
4. Adds ingestion metadata:
   - `ingested_at_utc`
   - `source`
   - `competition_id_input`
   - `season_id_input`
5. Writes raw outputs to `data/bronze/statsbomb/` as parquet (CSV fallback)

Produced raw files:

- `competitions.parquet`
- `matches.parquet`
- `events.parquet`

## Bronze Iceberg Write Logic

Spark writer script:

- `jobs/bronze_upsert_iceberg.py`

Main responsibilities:

1. Read raw parquet from `data/bronze/statsbomb`
2. Filter by requested `competition_id` and `season_id`
3. Derive partition columns:
   - `season` (from `season_id_input`, cast to string)
   - `match_week` (from `match_date`, ISO week, cast to string)
4. Cast **all fields** to `STRING` before writing
5. Write to Nessie/Iceberg with partition overwrite for idempotency

Default target tables:

- `nessie.bronze.matches_raw`
- `nessie.bronze.events_raw`

## Bronze Iceberg Schema

### Schema rule

- All payload columns are `STRING`
- Partition columns are also `STRING`:
  - `season`
  - `match_week`

### Partition spec

Bronze tables are partitioned by:

- `season`
- `match_week`

This supports weekly replay/reload and bounded overwrite scope.

## Idempotency Strategy

Idempotency is implemented using **dynamic partition overwrite**:

- `df.writeTo(<table>).overwritePartitions()`

If the same season/week input is rerun:

- Existing partition files are replaced
- Rows do not accumulate duplicates for that partition

The script also inspects Nessie snapshot lineage:

- Reads `snapshot_before` from `<table>.snapshots`
- Writes data
- Reads `snapshot_after`

This confirms each run is a committed table version while preserving idempotent partition state.

## Runbook

### 1) Fetch raw data

```bash
.venv/bin/python ingestion/fetch_statsbomb.py \
  --competition-id 9 \
  --season-id 281 \
  --max-matches 10 \
  --output-dir data/bronze/statsbomb
```

### 2) Write Bronze Iceberg tables

```bash
docker exec spark /opt/spark/bin/spark-submit --master local[*] /tmp/bronze_upsert_iceberg.py \
  --input-dir /opt/spark/work-dir/data/bronze/statsbomb \
  --competition-id 9 \
  --season-id 281
```

### 3) Validate schema and partitions

```bash
docker exec spark /opt/spark/bin/spark-sql --master local[*] -e "
DESCRIBE TABLE nessie.bronze.matches_raw;
DESCRIBE TABLE EXTENDED nessie.bronze.matches_raw;
DESCRIBE TABLE nessie.bronze.events_raw;
DESCRIBE TABLE EXTENDED nessie.bronze.events_raw;
"
```

Expected:

- business columns are `string`
- partition section includes `season` and `match_week`

