# Slowly Changing Dimension Type 2

SCD Type 2 preserves successive entity attribute versions with validity
intervals and a current/deleted marker. CDC describes what changed at the
source; SCD2 is one way to model those changes for analysis.

## Existing implementations

| Location | Behavior | Status |
| --- | --- | --- |
| PostgreSQL `scd.entity_versions` | Generic JSONB history with `valid_from`, `valid_to`, `is_current`, `is_deleted`, source key and LSN. The Airflow CDC consumer applies transitions for `SCD2_TABLES`; `CourseEnrollments` is excluded. | Implemented legacy staging/history path; consumer DAG paused. |
| ClickHouse `cdc_student_scd2` view | Query-time Student versions derived from active generation's CDC event log, ordered by source version/Kafka position. | DDL configured; no live rows/validation in readiness evidence. |
| Batch dimensions | Complete snapshot versions selected by `snapshot_id`. | Not SCD Type 2 row-level history. |

These are not a single jointly maintained authoritative SCD2. The streaming
Spark code writes PostgreSQL event history but does not apply its `scd` table;
the ClickHouse view is a separate projection. Do not run both consumer paths
as competing owners. `CourseEnrollments` is a fact and is not copied into
entity SCD2. See [CDC warehouse design](./cdc-warehouse-design.md) and
[the lifecycle diagram](../02-architecture/diagrams/streaming-pipeline.md).
