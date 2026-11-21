# Data Model

The SQL Server source has 15 assessed business tables. The current ClickHouse analytical model serves enrollment, offerings, applications, and related dimensions; it does not reproduce all source entities. The source keys below are based on the captured DDL and assessment exports in `db/source_assessment/`, not a new live SQL Server catalog query.

## Source and staging

| Subject | Main source keys | Related tables |
| --- | --- | --- |
| Students | `Students(StudentId)` | `StudentProfiles`, `Degrees`, `ClassStandings`, `Terms` |
| Applications | `Applicants(ApplicantId)` | `Terms` |
| Enrollments | `CourseEnrollments(CourseEnrollmentId)` | `Students`, `CourseOfferings`, `Grades` |
| Courses | `CourseOfferings(CourseOfferingId)`, `Courses(DepartmentCode, CourseNumber)` | `Departments`, `Terms` |
| Academic references | `Terms(TermCode)`, `Sessions(SessionCode)`, `Degrees(DegreeId)` | Degree types, levels, requirements, grades, departments |

| PostgreSQL schema | Meaning |
| --- | --- |
| `raw` | Batch-scoped typed source rows (source key + `_batch_id`) |
| `metadata` | Full/CDC run metadata, extraction batches, streaming generations / offsets |
| `history` | CDC event images, source metadata and Kafka-position identity |
| `scd` | Legacy Airflow CDC SCD2 entity versions; Spark stream does not maintain it |

## Published ClickHouse batch model

The nine dimensions are Applicant, Class Standing, Course, Date, Degree, Department, Grade, Student, and Term. The three facts have these grains:

| View | Grain |
| --- | --- |
| `fact_application` | One `ApplicantId` row in the selected full snapshot |
| `fact_course_offering` | One `CourseOfferingId` |
| `fact_course_enrollment` | One `CourseEnrollmentId` |

Physical tables use `dim_*_snapshots` / `fact_*_snapshots` with `snapshot_id`. A row in `warehouse_model_loads` is inserted last; current views expose only the latest published snapshot. `StudentProfiles` and `DegreeRequirements` are staged but not part of the 12-table published model. Student and applicant identifiers are pseudonymized; direct identifiers are not included in the published model.

## Separate CDC model

ClickHouse `cdc_events` preserves streaming envelopes, and CDC views such as `cdc_current_state`, `cdc_course_enrollments`, and `cdc_student_scd2` derive projections once a streaming generation is active. These are not joined into the standard batch fact/dimension views. Snapshot IDs are **not** row-level SCD2 validity intervals.

See [Warehouse Table Guide](../07-data-warehouse/fact-and-dimension-tables.md) and [SCD Type 2](../07-data-warehouse/scd-type-2.md).
