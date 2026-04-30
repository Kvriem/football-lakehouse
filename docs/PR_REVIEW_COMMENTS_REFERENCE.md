# PR Review Comments Reference

Reference tracker for all automated review comments on PR [#1](https://github.com/Kvriem/football-lakehouse/pull/1).

Use this file as the working checklist to resolve each point and link follow-up commits.

## Status Legend

- `OPEN`: not addressed yet
- `IN_PROGRESS`: currently being fixed
- `DONE`: fixed and pushed
- `WONTFIX`: intentionally not applied (add rationale)

## Copilot Comments (10)

| # | File | Comment focus | Link | Status | Notes |
|---|---|---|---|---|---|
| 1 | `jobs/silver_job.py` | Hard check order bypasses null-ratio gate | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614805) | OPEN |  |
| 2 | `dags/footballiq_common.py` | Shell quoting/injection risk in args join | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614847) | OPEN |  |
| 3 | `jobs/gold_match_kpi_job.py` | Reads from pinned branch; possible stale upstream source | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614875) | OPEN |  |
| 4 | `jobs/gold_season_kpi_job.py` | Same branch-source drift concern | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614907) | OPEN |  |
| 5 | `jobs/gold_form_last5_job.py` | Same branch-source drift concern | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614941) | OPEN |  |
| 6 | `docker-compose.yaml` | `|| true &&` chaining can hide failures | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614964) | OPEN |  |
| 7 | `jobs/nessie_utils.py` | `ensure_branch()` lifecycle/reset semantics | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614981) | OPEN |  |
| 8 | `scripts/smoke-test.sh` | MinIO bucket check failure suppressed | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167614997) | DONE | f5e36a5 |
| 9 | `docker-compose.yaml` | Hardcoded local credentials/secret key | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167615016) | OPEN |  |
| 10 | `docker-compose.yaml` | Webserver docker socket mount may be unnecessary | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167615040) | OPEN |  |

## CodeRabbit Comments (16)

| # | File | Severity | Comment focus | Link | Status | Notes |
|---|---|---|---|---|---|---|
| 1 | `dags/footballiq_common.py` | Critical | Shell-safe command building in DAG helpers | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634545) | OPEN |  |
| 2 | `dags/gold_match_weekly_dag.py` | Critical | Promotion should merge validated commit/hash deterministically | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634555) | OPEN |  |
| 3 | `dags/gold_season_monthly_dag.py` | Major | Put promotion task in single-writer pool | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634558) | OPEN |  |
| 4 | `docker-compose.yaml` | Major | Improve airflow-init command failure behavior | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634576) | OPEN |  |
| 5 | `docs/AIRFLOW_README.md` | Minor | Clarify required non-Airflow services in startup steps | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634579) | DONE | f5e36a5 |
| 6 | `jobs/gold_form_last5_job.py` | Critical | Rolling window order by ISO week is unsafe across year boundary | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634583) | OPEN |  |
| 7 | `jobs/gold_match_kpi_job.py` | Major | Aggregated sum columns should be BIGINT-safe | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634588) | OPEN |  |
| 8 | `jobs/nessie_utils.py` | Major | Make `ensure_branch` idempotent under concurrent creators | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634598) | OPEN |  |
| 9 | `jobs/nessie_utils.py` | Major | Restrict `_http_json` URL schemes to http/https | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634606) | OPEN |  |
| 10 | `jobs/post_write_validate.py` | Major | Add null checks on grain columns before duplicate check | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634611) | OPEN |  |
| 11 | `jobs/silver_job.py` | Major | Run hard checks before quality-filter null removal | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634620) | OPEN |  |
| 12 | `scripts/run-manual-backfill.sh` | Minor | Validate numeric CLI args before embedding into JSON | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634629) | DONE | f5e36a5 |
| 13 | `scripts/run-manual-backfill.sh` | Major | Do not echo live Airflow password in examples | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634633) | DONE | f5e36a5 |
| 14 | `scripts/setup_spark_jars.sh` | Major | Download to temp file + atomic move + non-zero checks | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634638) | DONE | f5e36a5 |
| 15 | `scripts/smoke-test.sh` | Major | Add curl timeouts in health checks | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634651) | DONE | f5e36a5 |
| 16 | `scripts/smoke-test.sh` | Major | Remove `|| true` from bucket existence check | [link](https://github.com/Kvriem/football-lakehouse/pull/1#discussion_r3167634656) | DONE | f5e36a5 |

## Suggested Next Workflow

1. Mark 3-5 comments as `IN_PROGRESS` per fix batch.
2. Push one isolated commit per batch (security, correctness, ops/docs).
3. Update `Status` + `Notes` with commit SHA per comment.
4. Resolve conversations on GitHub after each batch.
