# Streaming architecture

The CDC system streams database changes through an ordered, retained Kafka log. Kafka Connect hosts the Debezium source connector and stores its own source offsets and schema history. Spark Structured Streaming runs as a separate Compose service and is the single CDC writer. PostgreSQL owns batch staging and Airflow metadata. Existing PostgreSQL CDC tables from the legacy deployment are historical rollback state.

## Ownership of progress

| Layer | What progress means |
|---|---|
| SQL Server | Position within the asynchronous capture instance history and its retention window |
| Kafka Connect | Persisted SQL Server source position for the connector |
| Kafka | Per-partition transport offsets in the retained topic log |
| Spark | Checkpoint progress and committed microbatch identities |
| ClickHouse | Physically verified microbatch seals and published generation |

A verified first baseline requires all tracked table topics, evidence of snapshot completion, and coverage through the saved Kafka cut. CDC serving continues against the active generation after activation. Current state uses SQL Server version ordering; the broker defines ordering only within each partition.

Spark's sink write and its checkpoint commit are separate. Replay compares the saved seal and actual ClickHouse records, while logical event views collapse re-emitted source changes. These rules provide a stable analytical result across the tested replay case without relying on physical uniqueness in MergeTree.

[Pipeline and state transitions](diagrams/streaming-pipeline.md) · [Processing semantics](../05-streaming-and-cdc/event-processing.md) · [CDC data model](../07-data-warehouse/cdc-warehouse-design.md)
