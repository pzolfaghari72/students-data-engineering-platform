# Fact and Dimension Tables

`warehouse_ddl.sql` defines append-only, versioned physical tables and
current-facing views. The v2 model has nine dimensions and three facts. Each
physical row belongs to one `snapshot_id`.

| Published view | Physical table | Grain and key attributes |
| --- | --- | --- |
| `dim_applicant` | `dim_applicant_v2_snapshots` | One source `ApplicantId` per snapshot; pseudonymous key, application type and status. |
| `dim_class_standing` | `dim_class_standing_v2_snapshots` | One class-standing code; name, degree-level attributes and required credits. |
| `dim_course` | `dim_course_v2_snapshots` | One `(DepartmentCode, CourseNumber)`; department, title, credits and maximum section size. |
| `dim_date` | `dim_date_v2_snapshots` | One calendar day in the application-received-date range; calendar attributes and `YYYYMMDD` key. |
| `dim_degree` | `dim_degree_v2_snapshots` | One `DegreeId`; degree, department, type and level descriptions. |
| `dim_department` | `dim_department_v2_snapshots` | One department code and name. |
| `dim_grade` | `dim_grade_v2_snapshots` | One grade code; nullable points and `IncludeInGpa`. |
| `dim_student` | `dim_student_v2_snapshots` | One student per snapshot; pseudonymous key, status, degree, standing and enrollment term. |
| `dim_term` | `dim_term_v2_snapshots` | One term code; session name and year. |
| `fact_application` | `fact_application_v2_snapshots` | One `ApplicantId` source row; application date, entry term, count and numeric application measures. |
| `fact_course_offering` | `fact_course_offering_v2_snapshots` | One `CourseOfferingId`; course, department, term, section, capacity and offering count. |
| `fact_course_enrollment` | `fact_enrollment_v2_snapshots` | One `CourseEnrollmentId`; student, offering, course, department, term, grade, degree, standing and enrollment count. |
| `warehouse_model_loads_v2` | `warehouse_model_loads_v2` | One successful v2 publication record; source run and row counts for all 12 target tables. |

Applicant and student names, contact details, addresses, and dates of birth
are not published. Applicant and student keys use the configured HMAC secret;
other dimensions use deterministic, namespaced SHA-256 keys. Student keys
retain their prior HMAC input format for cross-snapshot stability.

Spark writes all dimensions and facts only after validating their joins and
keys, then inserts the model-load record last. Current views filter to that
record's `snapshot_id`. Failed attempts may leave unpublished rows in physical
tables. There is no automated cleanup. The original physical tables and
`warehouse_loads` remain untouched for rollback.

The batch facts are refreshed from a full source snapshot, not incrementally
updated. CDC has a separate `cdc_events` MergeTree log and query-time
`cdc_course_enrollments` projection. CDC is not merged into these batch facts;
see [CDC warehouse design](./cdc-warehouse-design.md).
