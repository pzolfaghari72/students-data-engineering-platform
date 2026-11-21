# PostgreSQL Staging

PostgreSQL holds typed source snapshots, CDC event history, metadata and the
legacy SCD2 staging model. It also hosts the Airflow metadata database in a
separate database. It is not the analytical warehouse.

| Schema | Implemented tables / role |
| --- | --- |
| `raw` | 15 typed source tables with source keys, `_batch_id`, `_extracted_at`, and per-batch primary keys. |
| `metadata` | `pipeline_runs`, `extraction_batches`, `table_load_status`, `extraction_watermarks`, `kafka_offsets`, `streaming_generations`. |
| `history` | `cdc_events` with source images, source LSN fields and unique Kafka topic/partition/offset. |
| `scd` | `entity_versions` JSONB attributes, validity period, current/deleted flags, and a partial unique current-row index. |

The Airflow CDC consumer applies SCD2 to source/reference entities in
`SCD2_TABLES`; it excludes `CourseEnrollments`, which is retained in event
history. Spark streaming persists event rows and generation metadata but does
not update this PostgreSQL SCD2 table.

DDL is in `db/staging_postgres/staging_ddl/`. The read-only SQL Server CDC
prerequisite check is maintained at
`db/source_assessment/sqlserver_cdc_prerequisites.sql`; it does not enable CDC
or change source settings, and DBA authorization is required to resolve any
reported prerequisites. Current row counts and the
earlier CDC-lineage mismatch are in the
[readiness report](../13-validation/cdc-readiness-report.md). See the
[data model](../02-architecture/data-model.md) for grains and
[idempotency](../06-data-processing/idempotency-and-retries.md) for keys.
