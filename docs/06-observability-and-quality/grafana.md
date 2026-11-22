# Grafana dashboards and observability

Grafana issues the SQL configured for each panel against an identified datasource. The local image provides the ClickHouse plugin, and dashboard provisioning loads the repository definitions. The restricted dashboard reader receives published aggregates and status while raw CDC row images remain outside its grants. Preserve the provisioned UID and corresponding name when upgrading persisted Grafana state.

| Dashboard | Datasource | Scope |
|---|---|---|
| [Live Student Analytics — CDC](../../grafana/dashboards/live-students-cdc.json) | `students-cdc` | Current source totals and CDC progress. Its batch-publication panel uses `students-warehouse`. |
| [Batch Snapshot — Student Enrollment, Capacity & Grades](../../grafana/dashboards/enrollment-performance.json) | `students-warehouse` | Latest published dimensional snapshot with term and department filters. |

## Interpret the business metrics

| Metric | Meaning |
|---|---|
| Live students | Current nondeleted rows in source `Students`. Batch **Unique students** counts distinct pseudonymous student keys in the selected enrollment attempts. |
| Offerings and offered seats | One row per offering, including offerings without enrollments. Offered seats sums offering capacity. |
| Enrollments and capacity ratio | Enrollments count attempts. Batch enrollment/capacity divides attempts by offered seats in the selected scope. It can exceed 100%. |
| Applications | Applicant/application records. Batch panels show overall totals, status and entry term. A department filter cannot be inferred from the application fact. |
| Recorded grades and coverage | Attempts with a nonblank grade code. Batch coverage divides recorded grades by all attempts in scope. |
| Credit-weighted grade points | Sum of grade points times credits divided by credits for eligible recorded grades with valid points and positive credits. Batch also groups grade metrics by term and department. |

## Diagnose missing or stale results

1. If a panel shows an error, check the provisioned datasource UID and health. Check the native ClickHouse endpoint, database name, expected views and restricted grants before interpreting the panel as empty.
2. If queries succeed with no data, check for a batch publication and the active CDC generation. Initial replay can have sealed transport records before any generation becomes visible.
3. If active CDC appears stale, read the last verified microbatch, sampled offsets behind and Connect task state together. A zero sampled lag refers to the last verified batch. A quiet source can leave the timestamp unchanged.
4. Compare business counts using [reconciliation](../04-data-pipelines/reconciliation.md). Read each dashboard with its own metric grain, filters and publication boundary.

[Provisioned datasources](../../grafana/provisioning/datasources/clickhouse.yml) · [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) · [Visual evidence](../assets/README.md)
