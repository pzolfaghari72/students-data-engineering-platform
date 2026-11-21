# Phase 4 — Streaming Pipeline

**Objectives:** understand CDC capture, Debezium envelopes, Kafka positions,
Spark checkpoints and multi-sink retry limits.

**Read:** [CDC basics](../05-streaming-and-cdc/cdc-fundamentals.md),
[Debezium](../05-streaming-and-cdc/debezium.md),
[Kafka offsets](../05-streaming-and-cdc/kafka-topics-partitions-offsets.md),
[Spark streaming](../05-streaming-and-cdc/spark-structured-streaming.md), and
[recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).

**Inspect:** `cdc_loader.py`, `register_connector.py`, `stream_cdc.py`,
`history_events.sql`, `cdc_warehouse_ddl.sql`, and unit tests.

**Exercise:** run the existing synthetic unit tests and trace one snapshot
read, update, delete and tombstone from parser to destination. Do not register
the connector or start Spark until the DBA and Kafka-lineage blockers in the
[readiness report](../13-validation/cdc-readiness-report.md) are cleared.

**Expected result:** explain per-partition Kafka order, source LSN metadata,
the handoff guard and why PostgreSQL + ClickHouse are not exactly-once.

**Validation:** unit tests pass; live integration remains explicitly
unverified until a controlled source exercise is approved.
