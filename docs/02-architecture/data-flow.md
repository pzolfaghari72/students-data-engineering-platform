# Data Flow

| Flow | Source -> destination | Trigger | Durable record |
| --- | --- | --- | --- |
| Full snapshot | SQL Server -> Airflow -> PostgreSQL `raw` | Manual Airflow DAG | `metadata.pipeline_runs`, extraction batches and raw rows |
| Batch warehouse | PostgreSQL latest successful full run -> Spark -> ClickHouse `*_snapshots` | Manual warehouse DAG or alternative one-shot Compose job | `warehouse_model_loads`, published `snapshot_id` |
| CDC capture | SQL Server CDC -> Debezium Connect -> Kafka | Connector lifecycle | Kafka table topics and Connect offsets |
| Continuous CDC | Kafka -> Spark -> PostgreSQL `history.cdc_events` | Independent streaming container | Spark checkpoint and PostgreSQL generation metadata |
| CDC event warehouse | Same Spark microbatches -> ClickHouse `cdc_events` | Streaming write and publication gate | Events plus `warehouse_stream_generations` / `active_stream_generations` |
| Legacy SCD2 staging | Kafka -> paused Airflow CDC consumer -> PostgreSQL `scd` | Alternative; **do not run with Spark** | Source entity versions and Airflow offsets |
| BI | ClickHouse batch published views -> Grafana | Dashboard queries | Grafana state and ClickHouse snapshots |

The current batch warehouse uses full snapshots, not CDC history. CDC streaming does not yet update the published batch dimensions or facts, even if it reaches a complete snapshot boundary. The source lineage, live generation status, and CDC replay behavior must be checked before claiming finished incremental analytics.

See [Batch Diagram](./diagrams/batch-pipeline.md), [Streaming Diagram](./diagrams/streaming-pipeline.md), and [2026-11-21 validation](../13-validation/validation-update-2026-11-21.md).
