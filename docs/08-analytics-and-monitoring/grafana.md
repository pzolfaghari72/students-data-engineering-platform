# Grafana analytics and observability

Grafana queries ClickHouse and renders the data product selected for each panel. A datasource defines the database endpoint, account, protocol and stable UID. Dashboard provisioning keeps query SQL and panel configuration in version control. The local image supplies the ClickHouse plugin.

| Dashboard | UID | Source and purpose |
|---|---|---|
| Live Student Analytics — CDC | `students-live-cdc` | `students-cdc` for aggregate current totals and CDC health. The batch publication panel uses `students-warehouse`. |
| Batch Snapshot — Student Enrollment, Capacity & Grades | `students-enrollment-kpis` | `students-warehouse` for published fact/dimension views, term and department filters, and capacity/admissions/grade panels. |

Use the live dashboard for the active generation ID, bootstrap progress and sampled lag. `last_verified_at` tells when a successful microbatch was sealed. The latest broker lag estimate is sampled at that batch. During source inactivity, a quiet plot is compatible with an idle stream. Check the Spark query and Connect task before diagnosing an outage. The timestamp of an older batch publication stays fixed through ordinary Grafana refreshes.

When a panel errors, separate datasource health, permissions and view existence from an empty publication. On an upgrade, preserve the persisted datasource name and UID together to avoid a provisioning collision. The dashboard account can read the designated views and aggregate results while raw CDC row images remain restricted.

[Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/) · [Datasource YAML](../../grafana/provisioning/datasources/clickhouse.yml) · [Dashboard SQL](../../grafana/dashboards) · [Recovered dashboard captures](../14-screenshots/README.md#grafana-analytics)
