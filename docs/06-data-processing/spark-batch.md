# Spark batch warehouse loader

The [Spark batch entry point](../../spark/load_warehouse.py) requires `--source-run-id`. Airflow passes the numeric staging ID. The loader resolves the run and its reconciled table batches, reads only those raw batches through PostgreSQL JDBC and builds the documented dimensional model.

The loader uses run-scoped counts from staging as expectations. The result checks include relevant joins, one record per target key, and selected totals. Empty input is handled deliberately, including the date dimension and zero-row output tables. A candidate write uses a fresh `snapshot_id`. Publication is a separate insert after ClickHouse read-back validation.

Read/fetch/write sizes and shuffle partitions are configurable. Airflow provides the executing environment, including the pinned Spark and JDBC packages. Reusing PostgreSQL staging allows a warehouse rerun even when the source database is unavailable, as long as the referenced batches are present.

Check Airflow's `verify_spark_runtime` task on missing Java or Spark. For a JDBC error distinguish staging reads from candidate writes. After a warehouse failure use the same staging ID where eligible. [Transforms](transformations.md) · [Dimensional grains](../07-data-warehouse/fact-and-dimension-tables.md) · [Batch operations](../10-operations/batch-execution.md)
