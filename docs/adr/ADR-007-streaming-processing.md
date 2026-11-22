# ADR-007: Spark Structured Streaming

## Status

**Implemented and observed running on 2026-11-21.** Event ingestion to PostgreSQL and ClickHouse was evidenced. Snapshot consumption reached 100%, but final generation activation, replay, and post-snapshot insert/update/delete correctness have not been established by the supplied checks.

## Context

The platform needs a real Kafka consumer with a checkpointed lifecycle independent of finite Airflow DAGs.

## Decision

Use a separate `spark-cdc-stream` service with Spark Structured Streaming's Kafka source, `foreachBatch`, and a persistent named checkpoint. Append PostgreSQL CDC event history and streaming-generation metadata plus ClickHouse `cdc_events`. Keep the legacy Airflow CDC consumer paused to avoid competing ownership.

## Alternatives considered

- Running a never-ending consumer inside Airflow: not suitable for finite DAG orchestration.
- Polling PostgreSQL: bypasses the Kafka consumer demonstration.
- Repeated Spark batch polling: lacks the selected checkpointed Kafka streaming lifecycle.

## Consequences

PostgreSQL and ClickHouse sink writes are not one transaction. PostgreSQL Kafka-position uniqueness prevents repeated history inserts, but ClickHouse physical `MergeTree` event rows may be duplicated if a failed batch is replayed. Kafka retention and checkpoint persistence must be protected. CDC events do **not** automatically update the batch star-schema facts/dimensions.

## Implementation references

`spark/stream_cdc.py`, `spark/Dockerfile`, `spark-streaming-checkpoints` in Compose, [Checkpoint Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
