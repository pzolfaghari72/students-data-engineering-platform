# SQL Server source and PostgreSQL staging

SQL Server owns the transactional academic model. The [assessed schema and profiling outputs](../../db/source_assessment) describe keys, relationships, type distributions and recorded counts. The code [allowlist](../../airflow/dags/students_staging/source_tables.py) selects the same 15 business tables for batch and CDC.

| Area | Source tables |
|---|---|
| People and admissions | Applicants, Students, StudentProfiles |
| Teaching activity | Courses, CourseOfferings, CourseEnrollments |
| Calendar | Terms, Sessions |
| Degrees and progression | Degrees, DegreeLevels, DegreeTypes, DegreeRequirements, ClassStandings |
| Reference | Departments, Grades |

Courses uses `(DepartmentCode, CourseNumber)`. An offering belongs to a course and academic term. An enrollment attempt links a student to an offering and can carry a grade. StudentProfiles is keyed by `StudentId`. The source profiling export recorded 3,724,654 enrollment attempts, 600,000 applications and 59,279 offerings. These are counts at the assessed boundary. [Source ERD and downstream model](../02-architecture/data-model.md)

## Snapshot access

Airflow uses Microsoft ODBC Driver 18 through `pyodbc`. SQL Server snapshot isolation provides one consistent read of the selected tables and their source counts while transactional work can continue. A DBA enables `ALLOW_SNAPSHOT_ISOLATION` and supplies an approved reader, trusted TLS configuration and network route. Long extracts retain row versions on the source, so coordinate work with the source owner. Verify schema and type compatibility before allowing a source projection change.

## Staging contract

PostgreSQL is a transactional, typed landing area. The `raw` schema carries source columns, `_batch_id` and `_extracted_at`. The source key and batch ID enforce uniqueness per extract and permit multiple snapshots. The `metadata` schema records the pipeline run, per-table extraction batch and most recent table status. A successful run requires full table coverage and matching counts for source, fetched and stored rows. Each raw batch can be traced to that exact source run. Airflow task metadata lives in its own database. [Staging DDL](../../db/staging_postgres/staging_ddl)

The source and staging databases commit independently. A failed extract can leave staging rows for diagnosis. The warehouse pins a finalized, successful manifest, so those rows stay outside later snapshots. Source PII remains in the restricted raw layer, and the analytical model publishes pseudonymous keys where appropriate. See [batch execution](../04-data-pipelines/batch.md).

## CDC capture prerequisites

SQL Server CDC uses capture instances to expose keyed row changes from the transaction log. An authorized DBA confirms database-level and table-level CDC, capture and cleanup jobs, required reader access and a retention window longer than expected operational interruption. SQL Server retention and Kafka retention govern different replay segments. The [readiness SQL](../../db/source_assessment/sqlserver_cdc_prerequisites.sql) is read-only. The [enablement SQL](../../db/staging_postgres/enable_source_cdc.sql) is for authorized source setup. A reader may have access to capture metadata without access to Agent jobs. [Microsoft CDC overview](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server) · [CDC data path](../04-data-pipelines/cdc.md)
