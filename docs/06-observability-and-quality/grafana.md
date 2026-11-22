# Grafana dashboards and observability

Grafana provisions both dashboards from the repository. Sign in with the Grafana admin in `.env`. The local ClickHouse datasources use the ClickHouse admin. Persist the UIDs when updating an existing deployment. [Provisioned datasources](../../grafana/provisioning/datasources/clickhouse.yml)

| Dashboard | Scope and datasource |
|---|---|
| [Batch Snapshot — Student Enrollment, Capacity & Grades](../../grafana/dashboards/enrollment-performance.json) | Latest published dimensional snapshot through `students-warehouse`, with term and department filters |
| [Live Student Analytics — CDC](../../grafana/dashboards/live-students-cdc.json) | Current source totals, progress and processed source changes through `students-cdc`. Latest Batch publication uses `students-warehouse`. |

## Business metrics

| Metric | Meaning |
|---|---|
| Students | CDC counts current nondeleted source student rows. Batch **Unique students** counts distinct pseudonymous students in the selected enrollment attempts. |
| Offerings and seats | One row per course offering. Seats sum capacity, including offerings without enrollments. |
| Enrollments and capacity ratio | Enrollments count attempts. The ratio divides attempts by offered seats within the selected scope. |
| Applications | Applicant/application records. Application panels use overall or entry-term scope because the fact has no department grain. |
| Grade coverage and weighted grade points | Coverage divides attempts with a nonblank grade by all selected attempts. Weighted grade points use valid points and positive credits. |

## CDC progress and events

- **CDC progress and freshness** shows the active generation, verified bootstrap, last batch seal and offsets behind at that seal. Use Connect status and Spark logs to check health between seals.
- **Live CDC Events** and **CDC Events Over Time** count deduplicated inserts, updates and deletes in the selected range by first verified processing time. Snapshot reads and tombstones stay outside these counts. The rate is an average per selected minute.
- **Latest CDC Changes** shows up to 25 recent changes in the active generation. It separates Debezium event time from the Spark batch seal and hides keys for people-related tables.
- **CDC processing activity** reports the active generation's cumulative live event count, last processing time and latest event-to-seal delay when the underlying event timestamp is available and precedes the seal.

An empty CDC activity panel is expected during first bootstrap, and a time-filtered panel can be empty during source inactivity. If queries fail, check the datasource UID, database, ClickHouse credentials and view names. If data looks stale, compare the last verified batch with Connect status and worker logs. [Runbook](../07-operations/runbook.md#troubleshooting)

## Batch reader grant repair

A historical deployment used a restricted ClickHouse reader whose grant referred to `fact_enrollment`. The published view is `fact_course_enrollment`. The grant was repaired before the switch to the local ClickHouse admin. The preserved screenshot records the issue at the time. [Diagnostic capture](../assets/screenshots/grafana-batch-dashboard-diagnostics.png) · [Current Batch DDL](../../db/warehouse_clickhouse/warehouse_ddl.sql)
