# Implementation and validation status

**Evidence date — 22 November 2026**

The repository implements the independent batch and live CDC paths described in the [architecture](../02-architecture/architecture-overview.md). The controlled tests used the pinned images with disposable data. The running installation still has the legacy CDC generation and checkpoint.

| Validation | Result |
|---|---|
| Host unit and static checks | 25 unit tests passed. Compose interpolation, environment modes, installed Python dependency consistency, syntax and dashboard datasource references were checked. |
| Airflow | The two supported batch DAGs parsed with zero import errors in the running Airflow image. |
| Source readiness | The configured SQL Server reader connected, reported snapshot isolation `ON` and database CDC enabled, and saw 15 capture instances. Agent and job-retention queries returned SQLSTATE `42000` for this reader and require DBA evidence. |
| Spark Batch | A synthetic staging run published through pinned Spark and the real PostgreSQL and ClickHouse JDBC libraries. It selected run 1 while run 2 existed, validated persisted results and skipped a repeated publication. A separate run published the all-empty fixture and verified its zero counts. |
| ClickHouse SQL | Synthetic batch and CDC publication assertions passed in ClickHouse 24.8. They cover no visibility before publication, source-run precedence, verified CDC batches, baseline filtering, logical replay, deletes and current state. |
| Spark CDC | The disposable Kafka → Spark → ClickHouse fixture passed. It injected a failure after the ClickHouse microbatch was sealed, resumed the same checkpoint, and produced five logical events from seven Kafka records with the expected delete, reinsert and offering update. Its Connect offset response was a fixture. |
| Grafana and reconciliation | Both datasources and dashboards provisioned in an isolated Grafana process. Its ClickHouse plugin queried the live CDC KPI definition using fixture data. The restricted reader could not select raw event images. The cross-database reconciliation query also executed. |
| Legacy baseline check | The existing connector and task were running. Kafka reported 15 partitions and 4,623,487 retained records from offset zero. Legacy batch and CDC snapshot counts matched for 600,000 applications, 59,279 offerings and 3,724,654 enrollments. The comparison covers counts at those historical boundaries. |

A combined test attempt reached batch publication and then an OOM restart interrupted its empty-run check. ClickHouse recovered to healthy, and the empty case passed when tested alone. The obsolete Spark batch Compose service, its inactive container, and the disposable fixture resources were removed.

## Live acceptance remaining

A fresh full extract from the external source through the new Airflow DAG chain, repeatable source mutations through Debezium, and replay of the complete retained source into the new CDC database have not been executed. The external source was accessed read-only. The active legacy connector, topics, warehouse objects, PostgreSQL CDC tables, volumes and Spark checkpoint remain available for rollback. Follow the [cutover runbook](../10-operations/operational-runbook.md) before switching Grafana or retiring that state.

The [21 November record](../13-validation/validation-update-2026-11-21.md) describes the earlier deployment. The [acceptance checklist](../09-development-and-testing/integration-testing.md) describes how to reproduce the current tests and record the source-specific results.
