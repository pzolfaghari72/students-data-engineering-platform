# Batch Execution

The batch workflow uses two separate, manually triggered Airflow DAGs:
one extracts a consistent source snapshot, and the other builds the ClickHouse
dimensions and facts from the successful staging snapshot.

1. Confirm SQL Server access and snapshot-isolation prerequisites in
   [Snapshot Consistency](../04-data-ingestion/snapshot-consistency.md).
2. Start the regular platform with `make up`.
3. In Airflow, manually trigger `sqlserver_to_postgres_staging`.
4. Wait for `create_pipeline_run`, `load_consistent_snapshot`, and
   `finish_pipeline_run` to succeed. Confirm that all 15 expected snapshot
   batches are successful in PostgreSQL.
5. Manually trigger `postgres_to_clickhouse_warehouse`. It runs the
   `load_clickhouse_warehouse` Spark task against the latest successful full
   staging run; it is not automatically triggered by the extraction DAG.
6. Confirm the warehouse task succeeded. In ClickHouse, inspect
   `warehouse_model_loads_v2` and compare its source run and row counts with
   PostgreSQL `metadata.pipeline_runs` and `metadata.extraction_batches`.

The Spark task runs locally in the Airflow scheduler; no Docker socket is
mounted. The `make warehouse-load` command remains available for a direct
one-shot Compose Spark run, but it is an alternative to the Airflow warehouse
DAG and must not run concurrently with it.

A failed attempt can leave physical rows under an unpublished snapshot ID;
the previous published views remain visible. There is no automatic garbage
collection. Detailed model semantics are in
[Fact and Dimension Tables](../07-data-warehouse/fact-and-dimension-tables.md).

Do not rerun an entire full snapshot just to retry one failed table. Follow
the Airflow task recovery guidance in the
[operational runbook](./operational-runbook.md).
