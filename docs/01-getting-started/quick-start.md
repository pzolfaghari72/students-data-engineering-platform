# Quick start

Complete [installation](installation.md) and choose the path you want to exercise.

## Publish a batch snapshot

1. Run `make up` to start the databases, Airflow and Grafana.
2. Open Airflow at `http://localhost:18080` with the local administrator credentials.
3. Unpause `sqlserver_to_postgres_staging` and `postgres_to_clickhouse_warehouse` as needed. Trigger the staging DAG once.
4. Track the resulting `source_run_id` from staging into the warehouse DAG. Open the **Batch Snapshot** dashboard after the load publishes.

The source requires SQL Server snapshot isolation. A successful extract and warehouse trigger precede the ClickHouse publication. [Batch execution](../10-operations/batch-execution.md)

## Run live CDC

1. Complete the [CDC source readiness checks](../05-streaming-and-cdc/sql-server-cdc.md).
2. Run `make streaming-up`. Use `make status` for containers and `make logs` for progress.
3. Open Grafana at `http://localhost:13000`. The **Live Student Analytics — CDC** dashboard shows bootstrap progress and publishes current totals when ClickHouse activates the generation.

Use the [upgrade and recovery runbook](../10-operations/operational-runbook.md) before applying a new deployment to an existing volume. Preserve the broker, connector lineage and checkpoint during ordinary recovery.
