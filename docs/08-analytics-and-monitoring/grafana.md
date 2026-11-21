# Grafana

Grafana is retained for business analytics only. Its `Students Warehouse`
data source reads ClickHouse; infrastructure monitoring and Prometheus
exporters are not part of this project configuration. Dashboard JSON is under
`grafana/dashboards/`; datasource provisioning is under
`grafana/provisioning/datasources/`.

| Dashboard | Data source | Configured panels | Validation |
| --- | --- | --- | --- |
| Student Enrollment & Performance | `Students Warehouse` (ClickHouse) | Enrollments, unique students, course offerings, offered seats, applications, enrollment by term, and applications by status. | Dashboard configuration targets the published three-fact model. |

The dashboard is snapshot-based. Refreshing it queries ClickHouse; it does not
ingest source data or trigger a pipeline run. The Grafana `dashboards` Compose
profile starts Grafana after ClickHouse is healthy.

KPI semantics and unsupported metrics are in
[Business KPIs](./business-kpis.md). Startup commands are centralized in
[operations](../10-operations/operational-runbook.md).
