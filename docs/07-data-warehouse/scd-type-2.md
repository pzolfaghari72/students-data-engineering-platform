# Slowly Changing Dimension Type 2

SCD Type 2 preserves a version of an entity each time tracked attributes change. Versions normally include a business key, `valid_from`, `valid_to`, current flag, and deleted flag. This is **not** the same as retaining complete warehouse snapshots under different `snapshot_id` values.

## What is implemented today?

| Location | Model | Current operational meaning |
| --- | --- | --- |
| PostgreSQL `scd.entity_versions` | Generic source-key JSONB versions with validity bounds and deleted/current markers | The **legacy Airflow CDC consumer** performs selected SCD2 transitions; its DAG is paused while Spark CDC runs. |
| ClickHouse `cdc_student_scd2` | Query-time Student version history from CDC events in the **active streaming generation** | SQL view exists; live results remain dependent on generation publication and correctness checks. |
| ClickHouse `dim_student_snapshots`, other `dim_*_snapshots` | Full batch dimension images grouped by snapshot ID | Historical **snapshots**, not per-business-key SCD2 validity intervals. |
| Incremental CDC-to-batch star schema | No unified materialized SCD2 model | **Not implemented yet**. |

Spark streaming writes PostgreSQL history and ClickHouse CDC events but does not apply PostgreSQL `scd.entity_versions`. The paused Airflow consumer can apply SCD2 in PostgreSQL, but do not start it concurrently just to populate versions. `CourseEnrollments` has fact grain and is not part of the legacy dimension SCD2 table set.

## Correct future incremental design

To maintain queryable warehouse SCD2 dimensions from PostgreSQL CDC safely, a new loader needs to read a consistent **published generation** and a saved offset/watermark, deduplicate source positions, identify tracked attribute changes, preserve closed versions, write new current versions, handle deletes/reactivation, update facts, and commit its warehouse publication/checkpoint only after validation. It must tolerate retries without duplicate fact/current-version rows. Only then should its Airflow schedule be automated.

The 2026-11-21 source snapshot offsets reached 100%, but publication activation and SCD2 delta correctness have not yet been proven. See [CDC Warehouse Design](./cdc-warehouse-design.md) and [Streaming Execution](../10-operations/streaming-execution.md).
