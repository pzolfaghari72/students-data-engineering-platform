# Quick Start

This guide covers a local training environment. It separates initial full snapshots, independent Spark CDC streaming, and the ClickHouse analytical warehouse. For observations dated 2026-11-21, see [Validation Update](../13-validation/validation-update-2026-11-21.md).

## Fresh installation

| Stage | Action | Expected result |
| --- | --- | --- |
| Prepare | Configure `.env`, then `make init`. | Required settings and Compose configuration validate. |
| Core | Run `make up-core`. | PostgreSQL and ClickHouse start; schema initialization succeeds. |
| Batch orchestration | `make up-batch`. | Airflow API server, scheduler, and DAG processor available. |
| Full source load | Manually trigger `sqlserver_to_postgres_staging` if a new baseline is required. | One successful run and the required 15 table batches in PostgreSQL. |
| Warehouse | Manually trigger `postgres_to_clickhouse_warehouse` after successful staging. | Spark validates and publishes a completed ClickHouse snapshot or skips an already published source run with the **local safety patch**. |
| Dashboard | `make dashboards-up`. | Grafana queries the published ClickHouse views. |
| Streaming | Only for a new, deliberately configured CDC deployment, follow the [streaming runbook](../10-operations/streaming-execution.md). | A single chosen consumer, preserved checkpoints and validated Kafka lineage. |

These are **setup commands**. If the system is already running, do not run them again simply to check its status. Use `make status` or the read-only health script instead. Do not run the Airflow CDC DAG while Spark CDC is the consumer owner.

## Existing installation (the observed 2026-11-21 state)

The batch analytical views and Grafana datasource were populated. Spark streaming was running, and the CDC initial snapshot offsets reached **100%** with `snapshot_events_complete=t`. The last supplied PostgreSQL generation status remained `BOOTSTRAPPING`; final activation and ClickHouse generation publication were not independently confirmed. Do not reset anything to make the status change.

The normal full-snapshot warehouse DAG remains **manual**. A locally applied patch checks Java/Spark and skips an already published full run. This does not implement incremental CDC-to-star-schema SCD2 processing; do not put that DAG on a 15-minute schedule.

## Useful addresses

Airflow: `http://localhost:18080`; Grafana: `http://localhost:13000`; Kafka Connect REST: `http://localhost:18083`; PostgreSQL host port: `15433`; ClickHouse HTTP host port: `18123`.

The main command reference is the [Operational Runbook](../10-operations/operational-runbook.md). Read [Environment Configuration](./environment-configuration.md) before editing `.env`; never share its secrets.
