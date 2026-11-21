# Project Overview

This is a local data engineering learning platform for SQL Server student data. It demonstrates a consistent full snapshot, PostgreSQL staging and run tracking, a Spark-built ClickHouse dimensional warehouse, an independently checkpointed CDC stream, and Grafana analytics.

## Implemented paths

| Capability | Implementation | Last reported validation boundary |
| --- | --- | --- |
| Source assessment | `db/source_assessment/` | Historical source DDL and inspection evidence. |
| Full snapshot extraction | `sqlserver_to_postgres_staging` DAG; `raw` and `metadata` schemas | Fifteen successful staged table batches in the observed full run. |
| Batch transformation | `spark/load_warehouse.py` and warehouse DAG | Nine dimensions, three facts, populated ClickHouse views; source-to-current-output reconciliation still separate. |
| CDC capture | Debezium in Kafka Connect to Kafka topics | Kafka and Connect healthy in 2026-11-21 report; full source task/Agent health not independently probed there. |
| Spark streaming | `spark/stream_cdc.py` | Running with progressing offsets; snapshot-consumed flag reached true; final publication unconfirmed. |
| CDC staging | `history.cdc_events`, generation metadata | Events persisted in PostgreSQL during streaming. |
| CDC warehouse | ClickHouse `cdc_events` and query-time CDC projections | Event table populated; active generation still needed confirmation. |
| Legacy PostgreSQL SCD2 | `scd.entity_versions` via alternative Airflow CDC consumer | Implementation exists; Airflow CDC DAG is paused while Spark owns CDC. |
| Grafana | ClickHouse datasource, Student Enrollment, Capacity & Grades | Datasource healthy; 18-panel dashboard reported, including grades. |
| Incremental analytical-star SCD2 | Not yet connected to CDC | Needs separate incremental loader, high-watermark/offset handling, and tests. |

## Two important data models

The **batch star schema** reads the latest successful PostgreSQL `raw` batches and publishes complete snapshots. It does not automatically incorporate streaming events. The **CDC model** stores event history and derives current state or Student SCD2 at query time after a streaming generation is active. These are distinct models.

The source is external to Docker. Do not commit student records or `.env`. Use [Architecture](../02-architecture/architecture-overview.md), [Quick Start](./quick-start.md), and [2026-11-21 validation](../13-validation/validation-update-2026-11-21.md) for the current project narrative.
