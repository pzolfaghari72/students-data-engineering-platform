# Data Model

The SQL Server source model has 15 business tables. The current ClickHouse
analytical slice is enrollment-focused; it does not reproduce the entire
source model. Source keys/relationships below are based on the captured DDL
and assessment exports under `db/source_assessment/`.

## Source entities and relationships

| Subject | Tables and observed keys | Important relationships |
| --- | --- | --- |
| Student/application | `Students(StudentId)`, `StudentProfiles(StudentId)`, `Applicants(ApplicantId)` | Students reference `Degrees`, `ClassStandings`, `Terms`; Applicants reference `Terms`. |
| Enrollment | `CourseEnrollments(CourseEnrollmentId)` | References `Students`, `CourseOfferings`, and nullable `Grades`. This is the largest table and the warehouse fact grain. |
| Course catalog | `CourseOfferings(CourseOfferingId)`, `Courses(DepartmentCode, CourseNumber)` | Offering references course by composite key and `Terms`; Courses reference `Departments`. |
| Academic calendar | `Terms(TermCode)`, `Sessions(SessionCode)` | Terms reference Sessions. |
| Degree/reference | `Degrees(DegreeId)`, `DegreeTypes(DegreeTypeCode)`, `DegreeLevels(DegreeLevelCode)`, `Departments(DepartmentCode)`, `ClassStandings(ClassStandingCode)`, `DegreeRequirements(RequirementId)`, `Grades(Grade)` | Captured foreign-key results are the authoritative relationship list; review source DDL before assuming additional business semantics. |

## PostgreSQL schemas

| Schema | Grain / purpose | Key implementation |
| --- | --- | --- |
| `raw` | One source row per table and extraction batch | Source primary key plus `_batch_id`; append-only. |
| `metadata` | One row per pipeline run, extraction batch, table status, Kafka position, or stream generation | Run/batch states and unique offset keys support audit/retry. |
| `history` | One Debezium event per Kafka topic/partition/offset | `uq_cdc_events_kafka_position` enforces event identity. |
| `scd` | One version of an entity key | Partial unique current-version index; deleted versions are tombstones. |

## ClickHouse dimensional model

The published model has nine dimensions and three facts. Each fact is one row
per source identity in the selected successful snapshot:

| Fact | Grain | Main dimension keys |
| --- | --- | --- |
| `fact_application` | One row per `ApplicantId` source row | Applicant, application-received date, entry term |
| `fact_course_offering` | One row per `CourseOfferingId` | Course, department, term |
| `fact_course_enrollment` | One row per `CourseEnrollmentId` | Student, course, department, term, grade, degree, class standing |

The nine dimensions are Applicant, Class Standing, Course, Date, Degree,
Department, Grade, Student, and Term. `StudentProfiles` is not merged into
Student: its `ProfileId` profiling is inconsistent and the source does not
declare a foreign key. `DegreeRequirements` remains in staging and is not
forced into one of the three fact grains.

All warehouse tables append rows under a new `snapshot_id`. The versioned
`*_v2_snapshots` physical tables are retained alongside the original physical
tables. The `warehouse_model_loads_v2` row is written last, and the published
views expose only its completed snapshot. Applicant and student keys are
pseudonymized; direct identity/contact fields and date of birth are not
published.

The CDC DDL defines an append-only `cdc_events` table and query-time views
(`cdc_current_state`, `cdc_course_enrollments`, `cdc_student_scd2`) for the
active streaming generation. It is separate from the published batch star
schema and has not been populated in the observed runtime.

See the canonical [ER diagram](./diagrams/system-context.md) and the detailed
[warehouse table strategy](../07-data-warehouse/fact-and-dimension-tables.md).
