# SQL Server source and data assessment

SQL Server is the operational relational database. Applications and enrollments live alongside academic reference tables. Keys, foreign keys, nullable fields and type definitions describe the source contract. Batch ingestion uses the Microsoft ODBC driver from Airflow, while Debezium uses its SQL Server connector to capture row changes.

| Entity group | Tables in the shared allowlist |
|---|---|
| Applicants and students | Applicants, Students, StudentProfiles |
| Teaching activity | Courses, CourseOfferings, CourseEnrollments |
| Academic calendar | Terms, Sessions |
| Degrees and progress | Degrees, DegreeLevels, DegreeTypes, DegreeRequirements, ClassStandings |
| Reference | Departments, Grades |

The [source mapping](../../airflow/dags/students_staging/source_tables.py) defines the 15-table selection and PostgreSQL names. The [source DDL](../../db/source_assessment/source_ddl) records the assessed keys and columns. `Courses` uses the composite department/course number key. `StudentProfiles` is keyed by `StudentId` in the source DDL. Enrollment grain is `CourseEnrollmentId`.

Before changing a mapping, study the [profiling SQL](../../db/source_assessment/sql_scripts) and [saved results](../../db/source_assessment/results), including the workbook. They cover inventory, keys, types, cardinality and integrity examples. The historical count export records 3,724,654 enrollment attempts, 600,000 applications and 59,279 offerings. Live counts may change. The [source ERD](../02-architecture/diagrams/source-entity-relationship.png) labels sensitive columns without showing their values. Keep raw personal fields inside the authorized environment.

A current source connection needs working DNS, port, TLS and access rights. [Batch extraction](source-extraction.md) · [CDC readiness](../05-streaming-and-cdc/sql-server-cdc.md)
