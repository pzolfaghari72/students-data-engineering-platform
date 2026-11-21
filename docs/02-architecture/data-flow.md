# Data Flow

This page is the concise map of where data actually moves. Canonical diagrams
are linked rather than repeated.

| Flow | Source → destination | Trigger | Durable state |
| --- | --- | --- | --- |
| Source assessment | SQL Server metadata/data profiling → manual CSV/workbook outputs | Human-run SQL | Historical files under `db/source_assessment/results/` |
| Full snapshot | SQL Server → Airflow → PostgreSQL `raw` | Manual DAG run | Typed rows, `metadata.pipeline_runs` and `metadata.extraction_batches` |
| Batch analytics | PostgreSQL selected raw batches → local Spark in Airflow → ClickHouse snapshot tables | Warehouse task in the successful snapshot DAG | `warehouse_model_loads_v2` publishes a completed snapshot |
| CDC capture | SQL Server CDC → Debezium Connect → Kafka | Connector lifecycle | Kafka topics, schema history and Connect internal topics |
| Legacy CDC staging | Kafka → Airflow → PostgreSQL `history` and `scd` | One-minute DAG schedule, currently paused | CDC event rows and PostgreSQL offsets |
| Spark CDC | Kafka → Spark Structured Streaming → PostgreSQL history and ClickHouse CDC events | Independent restartable service | Spark checkpoint volume; PostgreSQL generation metadata |
| Business analytics | ClickHouse → Grafana | Dashboard query | ClickHouse warehouse and Grafana state volumes |

The batch path does not consume CDC history. Streaming writes CDC events but
does not update the current batch fact view. These are separate models, with
separate completeness and freshness semantics. See the [batch diagram](./diagrams/batch-pipeline.md)
and [streaming diagram](./diagrams/streaming-pipeline.md).
