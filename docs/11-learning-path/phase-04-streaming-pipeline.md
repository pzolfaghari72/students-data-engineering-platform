# Phase 4 - Streaming Pipeline

**Objective:** understand SQL Server CDC, Debezium envelopes, Kafka topic offsets, Spark microbatches, generation publication and recovery.

**Read:** [CDC Fundamentals](../05-streaming-and-cdc/cdc-fundamentals.md), [Debezium](../05-streaming-and-cdc/debezium.md), [Kafka Offsets](../05-streaming-and-cdc/kafka-topics-partitions-offsets.md), [Spark Streaming](../05-streaming-and-cdc/spark-structured-streaming.md), and [Checkpoint Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).

**Inspect:** `spark/stream_cdc.py`, `airflow/dags/students_staging/cdc_loader.py`, `debezium/register_connector.py`, `history.cdc_events`, ClickHouse `cdc_events`, and `metadata.streaming_generations`.

**Exercise:** first review the 2026-11-21 evidence that 15 topic boundaries reached 100% with `snapshot_events_complete=t`. Explain why the remaining `BOOTSTRAPPING` status must not be reported as published or active without a matching ClickHouse generation marker. Draw one microbatch's Kafka-position -> PostgreSQL -> ClickHouse -> checkpoint flow. Use existing unit tests for synthetic insert/update/delete scenarios; do not alter a live source just for documentation.

**Expected result:** explain source-version ordering, one chosen consumer owner, offset and checkpoint recovery, and why two separately committed sinks do not guarantee exactly-once physical writes.

**Validation boundary:** the stream was running and consuming; active generation, tested live `c/u/d` changes, and replay correctness were not established by the supplied output. Keep the Airflow CDC DAG paused while Spark runs. See [Validation Update](../13-validation/validation-update-2026-11-21.md).
