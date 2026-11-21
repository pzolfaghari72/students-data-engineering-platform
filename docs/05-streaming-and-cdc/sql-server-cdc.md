# SQL Server CDC

SQL Server CDC captures table changes into change tables using SQL Server
Agent jobs. Debezium reads these changes; the project does not poll raw source
tables to simulate CDC.

## Project prerequisites

`db/staging_postgres/enable_source_cdc.sql` is an administrative script for
the `Students` database. It enables database CDC if absent, creates
`students_debezium_cdc_reader`, and enables capture for the 15 assessed
`dbo` tables with net changes disabled. It must be reviewed/run only by an
authorized DBA. The script does not ensure the SQL Server Agent service is
running or prove connector permissions.

The readiness check found database CDC enabled and all 15 capture instances,
but the configured login could not inspect SQL Agent/job status or retention.
The source's `ALLOW_SNAPSHOT_ISOLATION` is also OFF, separately blocking the
Airflow batch transaction. See the [dated DBA findings](../13-validation/cdc-readiness-report.md)
and the read-only readiness SQL at
`db/source_assessment/sqlserver_cdc_prerequisites.sql`.

The DBA must validate the capture and cleanup jobs, retention, SQL Server
Agent, connector login, TLS and permissions appropriate for the installed
Debezium version. Do not infer job health from database CDC being enabled.
