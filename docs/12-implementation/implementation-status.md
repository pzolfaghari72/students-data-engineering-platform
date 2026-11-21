# Implementation Status

Status reflects repository implementation and the dated runtime evidence. A
configured component is not considered end-to-end validated merely because
its container or configuration exists.

| Component | Status | Evidence | Remaining work |
| --- | --- | --- | --- |
| SQL Server source and CDC | Blocked by External Dependency | SQL Server reachable; database CDC enabled with 15 capture instances. | DBA verification of Agent jobs, retention, permissions, and snapshot isolation. |
| Batch extraction | Blocked by External Dependency | One connection and one `SNAPSHOT` transaction across serialized table extraction. | `ALLOW_SNAPSHOT_ISOLATION` is OFF; enable/verify and rerun. |
| PostgreSQL raw staging/metadata | Implemented and Validated | Core database healthy; required schemas and 15 successful snapshot batches exist. | Fresh end-to-end reconciliation; backup/retention. |
| Airflow batch orchestration | Implemented but Not Validated | Finite DAG and batch task structure exist; Airflow services are stopped. | Validate current DAG code against a permitted source snapshot. |
| Spark batch | Implemented and Validated | Existing published batch data is present in ClickHouse. | Re-run and reconcile against a fresh source snapshot. |
| ClickHouse batch warehouse | Implemented and Validated | Published snapshot generation and latest-load views are present. | Fresh reconciliation and retention/cleanup policy. |
| Kafka and Kafka Connect | Implemented and Validated | Broker, metadata, Connect REST, plugin, and metrics checks passed. | Source topics and connector are absent; resolve source/lineage gates. |
| PostgreSQL CDC history/SCD2 | Partially Implemented | 710,000 event rows and three offsets exist; consumer code and constraints exist. | Reconcile topic/offset lineage and resolve the stale Airflow consumer run. |
| Spark Structured Streaming | Implemented but Not Validated | Streaming entry point, Kafka dependency, named checkpoint, and sinks exist. | Validate event flow, failure/restart, and cross-sink replay. |
| ClickHouse CDC projections | Implemented but Not Validated | CDC DDL and current-state/Student SCD2 projections exist; zero CDC events are loaded. | Exercise insert/update/delete, duplicates, publication, and reconciliation. |
| Grafana business dashboards | Implemented but Not Validated | Dashboard JSON and ClickHouse datasource provisioning exist. | Start Grafana and validate live query semantics. |
| Infrastructure monitoring | Removed | Prometheus and exporter services, provisioning, and dashboards were removed. The Connect JMX snapshot signal remains only as an internal Spark bootstrap gate. | None; Grafana is reserved for business dashboards. |
| Automated integration/end-to-end tests | Not Implemented | Unit suite passes 13 tests; integration steps are documented. | Add controlled integration tests after external gates are cleared. |
| Backup and restore | Not Implemented | Persistent volumes exist, but no validated backup/restore workflow. | Add and exercise engine-specific procedures. |

For verified runtime results, source-side blockers, and next-stage gates, see
the [final readiness report](./final-project-report.md), [runtime validation
report](../13-validation/runtime-validation-report.md), and [CDC readiness
report](../13-validation/cdc-readiness-report.md).
