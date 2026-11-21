# ADR-007: Spark Structured Streaming

## Status

Implemented in code/configuration; end-to-end behavior not validated.

## Context

The target architecture needs a real Kafka consumer with independent
checkpoint/restart lifecycle, not an indefinitely running Airflow task.

## Decision

Implement a separate `spark-cdc-stream` service using Spark Structured
Streaming Kafka source, `foreachBatch`, and a named persistent checkpoint.
It writes PostgreSQL event/generation metadata and ClickHouse CDC event rows.

## Alternatives considered

- Airflow polling forever: rejected as the stream runtime.
- PostgreSQL polling as a Kafka substitute: rejected because it is not Kafka
  streaming.
- Spark batch-only polling: does not provide checkpointed streaming reads.

## Consequences

The two sinks do not share a transaction. PostgreSQL idempotency protects
event history by Kafka position, but ClickHouse MergeTree may contain physical
duplicates after retry. Kafka and checkpoint retention must be coordinated.

## Implementation references

`spark/stream_cdc.py`, `spark/Dockerfile`,
`spark-streaming-checkpoints` in Compose, and
[Checkpoint and Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).
