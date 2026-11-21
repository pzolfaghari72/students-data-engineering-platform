# Spark Structured Streaming

`spark/stream_cdc.py` is an independent, long-running Spark 3.5.6 application. It is **not** an Airflow DAG task. It uses the Kafka source, ten-second processing-time triggers, bounded records per trigger, `foreachBatch`, and a persistent checkpoint volume.

## Microbatch flow

1. Subscribe to Debezium topics for the configured SQL Server database/schema/table pattern.
2. Decode Kafka JSON envelopes and retain source operation, key, images, source LSN/version, topic, partition and offset.
3. Insert replay-safe CDC event identities into PostgreSQL `history.cdc_events` and update generation offsets/metadata.
4. Append corresponding CDC event rows into ClickHouse `students_warehouse.cdc_events`.
5. Once Debezium's snapshot completion signal is seen, capture and consume every topic's initial snapshot end offset before publishing the ClickHouse generation and activating the PostgreSQL generation.
6. Continue reading later CDC changes from the Spark checkpoint.

## What has been demonstrated

A read-only health report on **2026-11-21** observed the Spark CDC container running, event counts increasing in PostgreSQL and ClickHouse, and no obvious errors in recent logs. The initial source snapshot contained **4,623,487** Kafka records across 15 topics. A later PostgreSQL query reported **100% consumed offsets** and `snapshot_events_complete=t`.

The **same later query** still showed `status=BOOTSTRAPPING`. That is not proof that ClickHouse has an active stream generation; confirm `warehouse_stream_generations`, `active_stream_generations`, and PostgreSQL final status before marking the stream's published model active. Insert/update/delete behavior after bootstrap and replay across both sinks have not been fully verified.

## Important distinctions

- Spark streaming populates PostgreSQL **history**, not PostgreSQL `scd.entity_versions`.
- ClickHouse CDC projection views use the separately published stream generation; batch `dim_*` and `fact_*` views remain full-snapshot models.
- PostgreSQL and ClickHouse writes are not one transaction. A ClickHouse write retried after PostgreSQL commits may add duplicate **physical** rows. Do not call the two-sink path exactly-once.
- The alternate Airflow CDC consumer stays paused while this stream owns CDC.

Keep the checkpoint volume and Kafka offsets intact. To inspect, use [Streaming Execution](../10-operations/streaming-execution.md) and [Recovery](./checkpoints-and-recovery.md). The older [CDC readiness report](../13-validation/cdc-readiness-report.md) is historical, not a current startup prohibition.
