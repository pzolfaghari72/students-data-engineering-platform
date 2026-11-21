# Streaming Architecture

The streaming side consists of a Debezium SQL Server connector inside Kafka Connect, Kafka table topics, and a checkpointed Spark Structured Streaming application. A separate, finite Airflow CDC consumer also exists but is **paused** to avoid competing ownership of the same intended sinks.

| Segment | Implemented behavior | Evidence / limit |
| --- | --- | --- |
| SQL Server -> Debezium -> Kafka | Capture the configured 15 `dbo` tables and publish source envelopes | Kafka/Connect healthy in the 2026-11-21 report; connector task state not independently shown by that summary |
| Kafka -> Spark | Ten-second Structured Streaming trigger, checkpointed offsets, bounded Kafka input batches | Spark running and consuming in local health report |
| Spark -> PostgreSQL | Append CDC event history and update streaming-generation offsets | `history.cdc_events` populated; initial snapshot offsets reached 100% |
| Spark -> ClickHouse | Append CDC event rows; publish a generation only after snapshot boundary completion | `cdc_events` populated; active-generation publication not confirmed after final check |
| Kafka -> Airflow CDC -> PostgreSQL | Alternative bounded consumer, with legacy SCD2 transitions | DAG remains paused while Spark owns streaming |

Spark writes PostgreSQL and ClickHouse separately. A write failure/retry can lead to duplicate **physical** ClickHouse events; the event identities and query-time projection must be tested. Neither the Spark streaming sink nor the ClickHouse CDC projections merge incremental changes into the current batch fact/dimension views.

Debezium's snapshot-complete metric and Kafka offsets establish a captured boundary, not one globally atomic SQL Server snapshot. The last user-supplied status after 100% was still `BOOTSTRAPPING`, requiring publication verification. See [Streaming Diagram](./diagrams/streaming-pipeline.md), [Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md), and [Streaming Execution](../10-operations/streaming-execution.md).
