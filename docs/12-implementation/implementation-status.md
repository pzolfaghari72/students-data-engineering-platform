# Implementation Status

**Evidence cut-off: 2026-11-21.** This is a **point-in-time local training-project assessment**, not a live probe or production certification. It supersedes the old current-status summaries for the same components, while the [2026-11-16 final project report](./final-project-report.md) remains a historical record. See [Validation Update](../13-validation/validation-update-2026-11-21.md).

| Component | Status | Evidence | Still to verify / implement |
| --- | --- | --- | --- |
| SQL Server source | Full snapshot staging previously succeeded | Successful full staging batches for 15 tables and populated warehouse | Fresh source settings / change-test permissions not independently checked in latest report |
| PostgreSQL batch staging | Implemented, populated | Healthy container, full run / batches and `raw` schema evidence | Fresh same-boundary source reconciliation / retention |
| Airflow batch | Running, DAGs parse | API healthy; scheduler / DAG processor running; no local import errors | Actual post-patch warehouse task execution not proven by runtime preflight alone |
| Spark batch runtime | Prerequisites verified | Java 17 and Spark 3.5.6 in Airflow scheduler; scripts readable | Controlled run results and publication evidence with locally patched code |
| ClickHouse batch warehouse | Implemented, populated | Published analytical views: 3,724,654 enrollments, 59,279 offerings, 600,000 applications | New incremental/SCD2 analytical refresh is not implemented |
| Kafka and Kafka Connect | Healthy in combined local report | Services running; 15 source-topic boundaries processed | Verify current connector and task status directly when needed |
| Spark CDC streaming | Running with progress | Kafka offset progression and CDC event writes; initial snapshot offsets later reached 100% | Active generation publication and subsequent `c/u/d` behavior |
| PostgreSQL CDC history | Populated | `history.cdc_events`, `metadata.streaming_generations` with boundary captured and complete | Reconcile historical event counts by generation and offset if needed |
| Legacy PostgreSQL SCD2 | Implemented alternative, not active | `scd.entity_versions` logic in Airflow CDC consumer | Keep its DAG paused while Spark CDC consumes source events |
| ClickHouse CDC event model | Populated, generation publication unconfirmed | `cdc_events` had increasing records; 14/15 table counts matched PG at one snapshot; `CourseOfferings` historical difference needs lineage comparison | Active generation, current-state/Student SCD2 projection, change and replay verification |
| Grafana | Operational datasource | ClickHouse datasource health OK; 18-panel dashboard described in saved dashboard export | Verify provisioned number formatting and future freshness requirements |
| Automatic incremental analytical warehouse | **Not implemented** | Batch Spark reads latest successful **full** run | Design CDC delta source, versions, deletes, facts, watermark, retry-safe publish, then schedule |
| Automated end-to-end tests | Partial | Dated unit tests / health report exist | Source-controlled change/replay and SCD2 integration tests |
| Backup/restore | Not established | Named volumes persist | Verify engine-specific backups before treating data as durable |

## Warehouse DAG local patch versus GitHub

A locally applied safety update added Airflow `verify_spark_runtime` and a guard in `spark/load_warehouse.py` to skip an already published full run. It retained `schedule=None`. The GitHub default branch may not yet contain these uncommitted local changes; synchronize source and docs before sharing the project as a reproducible end state.

## Current next milestones

1. Confirm **PostgreSQL generation `ACTIVE`** and ClickHouse published/active stream generation after the initial snapshot reached `snapshot_events_complete=t` and 100% offsets. Last supplied status was still `BOOTSTRAPPING`.
2. Test controlled Kafka/source change operations and retry/replay correctness, without resetting checkpoints.
3. Decide whether the training project requires **incremental analytical SCD2**. If yes, implement it as a distinct, safe loader before enabling periodic Airflow scheduling.

Older readiness reports that state the connector was absent, Spark never started, or ClickHouse CDC was empty describe the earlier date and are **not** current findings.
