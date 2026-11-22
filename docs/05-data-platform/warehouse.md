# ClickHouse data products

ClickHouse stores columns separately and favors analytical scans and aggregations. MergeTree tables sort and index data by their configured keys and merge parts in the background. Those merges are asynchronous. The platform makes serving visibility explicit through marker tables and views so readers get consistent results while publishers retry. [MergeTree documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)

## Batch dimensional model

Each physical table appends full snapshot rows under `snapshot_id`. A fact describes one business event or entity at a fixed grain. Reusable dimensions provide descriptive keys for filtering and joining. The following views resolve the selected snapshot through `warehouse_model_loads` and `batch_publication_status`.

| Published view | Physical table | Grain within one snapshot |
|---|---|---|
| `fact_application` | `fact_application_snapshots` | One source applicant/application record |
| `fact_course_offering` | `fact_course_offering_snapshots` | One `CourseOfferingId` |
| `fact_course_enrollment` | `fact_enrollment_snapshots` | One `CourseEnrollmentId` |
| `dim_applicant` | `dim_applicant_snapshots` | One pseudonymous applicant |
| `dim_class_standing` | `dim_class_standing_snapshots` | One class-standing code |
| `dim_course` | `dim_course_snapshots` | One department/course-number pair |
| `dim_date` | `dim_date_snapshots` | One application calendar day |
| `dim_degree` | `dim_degree_snapshots` | One degree ID |
| `dim_department` | `dim_department_snapshots` | One department code |
| `dim_grade` | `dim_grade_snapshots` | One grade code |
| `dim_student` | `dim_student_snapshots` | One pseudonymous student |
| `dim_term` | `dim_term_snapshots` | One term code |

Applications attach to applicant, received date and entry term. Their source relationship has no department grain. Offerings attach to course, department and term. Enrollments inherit academic context through the offering and degree/class standing through the student. Grade and class standing can be absent. Spark generates the date range from application received dates and uses a deployment-local HMAC key for student and applicant joins. These identities are pseudonyms.

Count seat capacity from the offering fact before comparing it with attempts from the enrollment fact. Summing seat capacity after a one-to-many enrollment join would multiply the seats. Published snapshots can support analysis over time by snapshot identity. Slowly changing dimension Type 2 would instead add a validity window and a new dimension version per attribute change. The current batch model tracks history at whole-snapshot granularity. [Batch DDL](../../db/warehouse_clickhouse/warehouse_ddl.sql) · [Model diagram](../02-architecture/data-model.md#clickhouse-batch)

## CDC event and current-state model

| Object | Role |
|---|---|
| `cdc_generations` | Bootstrap end offsets and broker lineage |
| `cdc_events` | Verified candidates at transport grain, with normalized key and source version |
| `cdc_batch_commits` | Seals that record accepted microbatches, digests and offset ranges |
| `cdc_publications`, `cdc_active_generation` | Activation marker and accepted baseline boundary |
| `cdc_committed_records`, `cdc_serving_records` | Sealed records filtered to the active generation and eligible baseline |
| `cdc_logical_events` | One eligible logical change per generation, table, canonical key, operation and source version |
| `cdc_current_state` | One latest surviving image per source table and canonical key |
| `cdc_business_kpis`, `cdc_pipeline_health` | Approved aggregate and status views for Grafana |

`argMax` uses source version and transport tie-breakers for deterministic latest-state queries. A latest delete drops a key from the current state. The KPI view computes its needed latest-image fields from the serving layer with the same ordering. The logical event view preserves eligible deletes and groups re-emitted source events. The current deployment publishes no row-level SCD Type 2 dimension in this database. [CDC DDL](../../db/warehouse_clickhouse/cdc_warehouse_ddl.sql) · [CDC processing](../04-data-pipelines/cdc.md)

The two databases have distinct writers and separate serving contracts. The Grafana reader can query defined aggregates and status without selecting raw CDC images. The shared server needs enough memory, disk and I/O for candidate validation, streaming writes and query-time grouping. [Grafana](../06-observability-and-quality/grafana.md) · [Publication and reconciliation](../04-data-pipelines/reconciliation.md)
