# Debezium SQL Server Connector

Debezium runs inside Kafka Connect. It captures SQL Server CDC data from the configured 15 `dbo` tables and writes keyed JSON event envelopes into Kafka topics.

## Implemented configuration

| Setting | Behavior |
| --- | --- |
| Connector class | `io.debezium.connector.sqlserver.SqlServerConnector` |
| Captured tables | Explicit 15-table source include list |
| Initial snapshot | `snapshot.mode=initial` |
| Event topics | `<topic.prefix>.<database>.<schema>.<table>` |
| Payload | JSON converter with schema embedding disabled |
| Connector creation | `debezium/register_connector.py` creates only if absent; an existing connector is not overwritten |
| Completion gate | Snapshot metric from Connect/JMX plus Kafka partition offsets; REST `RUNNING` alone does not establish completeness |

## Evidence chronology

- **2026-11-16 (historical readiness):** connector was absent, and source-topic lineage needed reconciliation. The corresponding [readiness report](../13-validation/cdc-readiness-report.md) remains a valid record of that date.
- **2026-11-21 (newer local check):** Kafka and Connect were healthy, Spark consumed Kafka source topics for 15 tables, and the initial snapshot boundary reached 100% in PostgreSQL metadata. That demonstrates a connector-driven data path since the older readiness report, but the final connector/task REST status and generation publication were not shown in the latest 100% check.

Do not re-register or overwrite the working connector just because a dated report says it was absent. Do not reset Kafka topics or offsets, and leave the alternative Airflow CDC DAG paused while Spark runs.

See [Streaming Execution](../10-operations/streaming-execution.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
