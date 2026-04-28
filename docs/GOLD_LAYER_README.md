# Gold Layer Summary

This document summarizes the implemented Gold layer for player performance analytics, based on `jobs/gold_silver.ipynb`.

## Goal

Deliver business-ready player performance marts from Silver with:

- stable KPI definitions
- idempotent incremental writes
- validation and branch-based promotion

## Modeling Phases Implemented

1. **Branch + runtime setup**
   - Created and used Nessie `gold_dev` branch from `dev`
   - Spark configured to write to `gold_dev`

2. **Base semantic mart (G1)**
   - Built `nessie.gold.player_match_kpis` from Silver
   - Added KPI classes:
     - volume: `event_count`, `pass_count`, `shot_count`
     - efficiency: `xg_per_shot`, `shot_conversion`, `shot_assist_ratio`
     - contribution: `goal_contribution`, `involvement_index`

3. **Season mart (G4)**
   - Built `nessie.gold.player_season_kpis` from `player_match_kpis`
   - Added season totals, weighted efficiency metrics, and dense ranks

4. **Form mart (G5)**
   - Built `nessie.gold.player_form_last5` from `player_match_kpis`
   - Added rolling last-5 match metrics and `form_trend`

5. **Operational layer**
   - Partition overwrite writes for idempotency
   - Gold audit table used in flow: `nessie.gold_audit.run_metrics`
   - Promoted `gold_dev -> dev -> main` via Nessie merge workflow

## Tables and Data Lineage

- `nessie.silver.player_team_match_stats`
  -> `nessie.gold.player_match_kpis`
  -> `nessie.gold.player_season_kpis`
  -> `nessie.gold.player_form_last5`

Detailed lineage by table:

- `nessie.gold.player_match_kpis`
  - **Source:** `nessie.silver.player_team_match_stats`
  - **Grain:** player-match
  - **Partition:** `(season_id, match_week)`
  - **Role:** base KPI semantic layer

- `nessie.gold.player_season_kpis`
  - **Source:** `nessie.gold.player_match_kpis`
  - **Grain:** player-season
  - **Partition:** `(season_id)`
  - **Role:** season leaderboard + rankings

- `nessie.gold.player_form_last5`
  - **Source:** `nessie.gold.player_match_kpis`
  - **Grain:** player-match with rolling window context
  - **Partition:** `(season_id, match_week)`
  - **Role:** momentum and recent-form analysis

## Final Metrics Snapshot

From current run:

- `gold.player_match_kpis`
  - season `281` weekly counts:
    - wk5: 30, wk8: 29, wk10: 31, wk11: 31, wk13: 30, wk14: 30, wk15: 30, wk18: 32, wk19: 31, wk20: 32

- `gold.player_season_kpis`
  - season `281`: **176** player-season rows

- `gold.player_form_last5`
  - season `281` weekly counts:
    - wk13: 9, wk14: 9, wk15: 13, wk18: 11, wk19: 13, wk20: 16

## Good Points 

- Clear semantic mart layering (`silver -> gold_match -> gold_season/gold_form`)
- KPI taxonomy design (volume vs efficiency vs contribution)
- Window analytics for rolling form and ranking
- Idempotent incremental writes with Iceberg partition overwrite
- Nessie branch promotion workflow for controlled release
- Gold audit + validation mindset for production readiness

