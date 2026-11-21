# Streaming Execution

The Spark service is independent of Airflow and has a persistent checkpoint.
It must not be started until the source, Kafka lineage, Debezium snapshot and
consumer ownership prerequisites are verified.

## Current gate

The current [CDC readiness report](../13-validation/cdc-readiness-report.md)
blocks registration and streaming because PostgreSQL holds CDC history/offsets
without matching source topics in the current Kafka cluster; SQL Server Agent
and retention are unverified; and an old Airflow consumer run remains
unresolved. Keep the Airflow CDC DAG paused. Do not run `make cdc-up` or
`make streaming-up` while these blockers remain.

## After an explicit readiness decision

1. Resolve DBA/source and historical topic-lineage blockers.
2. Confirm one consumer owns the stream and the checkpoint is the intended
   persisted state.
3. Start Kafka/Connect, register only if the connector is still absent, and
   verify actual connector/task state, snapshot metrics and source events.
4. Only then start Spark and check application progress, PostgreSQL event
   history, ClickHouse CDC events and the published generation.
5. On a Spark error, stop the one stream, preserve the checkpoint and compare
   Kafka beginning/end offsets with PostgreSQL metadata before retry.

The Makefile targets exist but do not implement these readiness gates
themselves. Startup/recovery commands are centralized in
[Operational Runbook](./operational-runbook.md); sink retry limitations are
in [Checkpoints and Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).
