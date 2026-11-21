# Streaming Architecture

The repository contains SQL Server CDC capture through Debezium and Kafka,
an existing bounded Airflow consumer, and an independent Spark Structured
Streaming application. These are not interchangeable, and only one consumer
should own the intended flow at a time.

| Path | Behavior implemented | Validation boundary |
| --- | --- | --- |
| SQL Server → Debezium → Kafka | Connector create-only registration helper, initial snapshot configuration, SQL Server topic filter and Kafka Connect service. | Plugin config was validated; no connector is registered and no source topics are present in the observed cluster. |
| Kafka → Airflow → PostgreSQL | Scheduled consumer normalizes `c/r/u/d/t`, inserts history idempotently by Kafka position, applies PostgreSQL SCD2 for selected tables, and commits staging offsets transactionally. | DAG paused; existing history is not reconciled with current Kafka topics. |
| Kafka → Spark → PostgreSQL + ClickHouse | Structured Streaming normalizes the envelope, persists PostgreSQL event metadata, appends ClickHouse CDC event rows and tracks generation/checkpoint offsets. | Code/configuration only; Spark was not started. Cross-sink atomicity is not provided. |

The streaming app detects Debezium snapshot completion using Connect's JMX
snapshot metric, captures Kafka end offsets, and waits until consumed offsets
pass those per-partition bounds before publishing a generation. This internal
control signal is not a Prometheus monitoring stack. It is not proof of a
globally atomic SQL Server snapshot or an end-to-end exactly-once guarantee.

Operational startup, the persisted checkpoint, and external blockers are
described in the [streaming runbook](../10-operations/streaming-execution.md)
and [readiness report](../13-validation/cdc-readiness-report.md). The
canonical diagrams are in [streaming-pipeline.md](./diagrams/streaming-pipeline.md).
