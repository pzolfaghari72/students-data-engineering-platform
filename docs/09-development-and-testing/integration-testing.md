# Integration Testing

Integration tests require real dependencies and source permissions; do not
simulate them with successful configuration checks.

| Test | External requirement | Required evidence |
| --- | --- | --- |
| Full source snapshot | Reachable SQL Server; DBA enables snapshot isolation | All 15 table batches and one run succeed; one source transaction used. |
| CDC bootstrap | SQL Server CDC/Agent/jobs, connector permissions, compatible Kafka topic lineage | Snapshot metrics complete, tasks RUNNING, expected topics/events and metadata inspected. |
| Spark stream sinks | Kafka topics, staging/warehouse schemas and checkpoint volume | Insert/update/delete visible in PG history and ClickHouse projection; replay behavior measured. |
| Recovery | Running stream and persisted checkpoint | Restart resumes without data loss; duplicate ClickHouse physical rows documented/queried correctly. |
| BI dashboard | Grafana and ClickHouse warehouse | Configured business KPI queries return expected results. |
| Full lifecycle | All above and sufficient local RAM | Compose startup/shutdown preserves named volumes and a complete source-to-dashboard path is evidenced. |

The current CDC blockers make these checks unsafe or unavailable. Follow the
[CDC readiness report](../13-validation/cdc-readiness-report.md) and do not
start competing consumers. No source mutation test should run without explicit
owner approval and a controlled test dataset.
