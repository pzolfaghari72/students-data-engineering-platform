# Data model

The [source assessment](../../db/source_assessment/results) contains the historical table inventory, columns, keys, counts, and profiling workbook. The [SQL Server DDL](../../db/source_assessment/source_ddl) records the assessed source contract. The [source relationship diagram](diagrams/source-entity-relationship.png) shows the original relationships.

## Source and staging

The [source mapping](../../airflow/dags/students_staging/source_tables.py) selects 15 business tables. PostgreSQL `raw` preserves source columns with `_batch_id` and `_extracted_at`. Keys are scoped to `_batch_id`, including the composite source key for Courses. Operational metadata lives in `metadata.pipeline_runs`, `metadata.extraction_batches`, and `metadata.table_load_status`. The source row-count and persisted staging row-count checks run before a table is considered complete.

## Batch database

| Fact view | Grain |
|---|---|
| `fact_application` | One source application ID |
| `fact_course_offering` | One source course offering ID |
| `fact_course_enrollment` | One source enrollment attempt ID |

Dimensions cover applicant, class standing, course, application date, degree, department, grade, student and term. Student and applicant keys are pseudonymized with a deployment-local HMAC key. Application statistics have no department grain. Enrollment counts represent attempts, and weighted grade points use the assessed source grade scale. The batch warehouse publishes whole snapshots under `snapshot_id`; the current views and `batch_publication_status` resolve the most recent validated source run.

## CDC database

| Object | Role |
|---|---|
| `cdc_events` | Restricted transport records with Kafka positions, canonical source keys, envelopes, SQL Server ordering and record fingerprints. |
| `cdc_generations`, `cdc_batch_commits`, `cdc_publications` | Broker lineage, bootstrap end offsets, verified writes and publication. |
| `cdc_committed_records`, `cdc_serving_records` | Verified transport data, scoped for serving to the active generation and its completed baseline. |
| `cdc_logical_events` | One logical event per table, key, operation and source version inside the active generation. |
| `cdc_current_state` | Latest nondeleted image per table and key. |
| `cdc_business_kpis`, `cdc_pipeline_health` | Approved Grafana aggregates and publication/progress data. |

The operational event store has no cross-pipeline dependency. Pipelines share an analytical grain for reconciliation of application, offering and enrollment counts. Grafana receives aggregates from CDC and cannot read the row images or keys. Retained events support historical analysis while the current-state projection serves the live dashboard.
