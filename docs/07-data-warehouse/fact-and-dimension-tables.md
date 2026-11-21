# Fact and Dimension Tables

The batch model in `db/warehouse_clickhouse/warehouse_ddl.sql` contains nine dimensions and three facts. It is an **append-only full-snapshot model**, not row-level SCD Type 2. All physical rows carry a `snapshot_id`, and views filter to the latest completed row in `warehouse_model_loads`.

## Current model names and grains

| Published view | Physical table | Grain / key |
| --- | --- | --- |
| `dim_applicant` | `dim_applicant_snapshots` | One pseudonymous `ApplicantId` / `applicant_key` |
| `dim_class_standing` | `dim_class_standing_snapshots` | One class-standing code |
| `dim_course` | `dim_course_snapshots` | One `(DepartmentCode, CourseNumber)` |
| `dim_date` | `dim_date_snapshots` | One calendar day / `date_key` |
| `dim_degree` | `dim_degree_snapshots` | One `DegreeId` |
| `dim_department` | `dim_department_snapshots` | One department code |
| `dim_grade` | `dim_grade_snapshots` | One grade code |
| `dim_student` | `dim_student_snapshots` | One pseudonymous `StudentId` / `student_key` |
| `dim_term` | `dim_term_snapshots` | One term code |
| `fact_application` | `fact_application_snapshots` | One source `ApplicantId` row |
| `fact_course_offering` | `fact_course_offering_snapshots` | One `CourseOfferingId` |
| `fact_course_enrollment` | `fact_enrollment_snapshots` | One `CourseEnrollmentId` |
| Publication marker | `warehouse_model_loads` | Completed `snapshot_id`, `source_run_id`, row counts and `completed_at` |

**Do not use** old `_v2_snapshots` physical table names or `warehouse_model_loads_v2` in commands against the current schema. Prior versions may remain in older reports or historic databases, but the supplied current DDL uses the names above.

## Publication and interpretation

Spark reads successful `raw` batch IDs, builds transformed data, checks row counts and key uniqueness, appends all dimension/fact snapshots, then appends the publication marker. Grafana queries published **views**, not every physical snapshot row. Counting all physical rows over all `snapshot_id` values would overcount repeated full loads.

The local 2026-11-21 safety patch adds a guard against publishing the same full staging run twice. It does not apply incremental CDC events or change ClickHouse table definitions. Failed writes can leave unpublished physical rows. Student and applicant keys are pseudonymous, not anonymous; source identifiers are not published.

For incremental data, `cdc_events`, `cdc_current_state` and `cdc_student_scd2` are **separate** CDC objects. They do not update the batch star schema automatically. See [CDC Warehouse Design](./cdc-warehouse-design.md), [SCD Type 2](./scd-type-2.md), and [Publication](./data-publication-and-reconciliation.md).
