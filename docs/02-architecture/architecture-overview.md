# Architecture Overview

The platform has two distinct paths: a **manual full-snapshot warehouse pipeline** and a **separate, continuous CDC event pipeline**. Both were operational to different degrees in the 2026-11-21 local checks, but they do not yet form a unified incremental SCD Type 2 star schema.

## Component responsibilities

| Component | Input | Purpose | Output |
| --- | --- | --- | --- |
| SQL Server `Students` | Operational data | Full snapshot source and CDC source | Source rows and CDC capture data |
| Airflow full snapshot DAG | SQL Server + run metadata | Single consistent source snapshot | PostgreSQL `raw` and `metadata` |
| PostgreSQL | Batch rows and Kafka CDC events | Raw staging, history, run/batch/stream generation metadata | Successful batches, `history.cdc_events` |
| Debezium / Kafka Connect | SQL Server CDC | Serialize source events into Kafka topics | Kafka event log |
| Kafka | Debezium topics | Retain events and offsets | Source stream for one intended consumer |
| Spark Structured Streaming | Kafka event envelopes | Persist PostgreSQL CDC history and ClickHouse CDC events; track checkpoints | Event stores and generation metadata |
| Airflow CDC DAG | Alternative finite Kafka consumer | Legacy history and PostgreSQL `scd.entity_versions` updates | **Paused**; not used concurrently with Spark |
| Spark warehouse batch | Latest successful PostgreSQL full snapshot | Build and validate nine dimensions / three facts | ClickHouse `*_snapshots` tables and publication marker |
| ClickHouse | Batch snapshot rows + separate CDC event rows | Published business views; CDC query-time projections | Analytical views and CDC views |
| Grafana | Published ClickHouse business views | KPI dashboards | No ingestion or orchestration |

## Key boundary

`PostgreSQL raw` -> `Spark batch` -> `ClickHouse fact/dimension snapshots` is **not** a CDC incremental pipeline. `Kafka` -> `Spark stream` -> `PostgreSQL history` plus `ClickHouse cdc_events` is a separate model. A checked high watermark or generation publication does not itself update `fact_course_enrollment` or `dim_student` batch views.

The legacy Airflow consumer and long-running Spark consumer should not both be enabled against the intended shared sink. Keep Airflow CDC paused while Spark is running. The source event log and both sinks are not one distributed transaction; replay can create physical ClickHouse duplicates.

## Verification and next development

On 2026-11-21, the local Spark stream consumed the snapshot offset boundary (100%), but PostgreSQL still showed `BOOTSTRAPPING` in the supplied query. Publication activation and source-change reconciliation remain unverified. The incremental CDC-to-star-schema SCD2 loader also remains to be implemented and tested before scheduling the warehouse DAG.

See the [Batch Architecture](./batch-architecture.md), [Streaming Architecture](./streaming-architecture.md), [Data Flow](./data-flow.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
