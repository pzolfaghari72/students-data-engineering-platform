# Source extraction

The [extractor](../../airflow/dags/students_staging/loader.py) uses Python to coordinate data movement and SQL for stable source counting and typed writes. It constructs an ODBC connection from configuration, quotes identifiers and maps table names through the static allowlist. The source projection uses `SELECT *`, so a column change needs a compatibility review against the staging DDL.

1. Allocate a full pipeline run and register each extraction batch.
2. Compare `COUNT_BIG(*)` with the actual fetched rows within the same SQL Server snapshot. Write bounded chunks through the PostgreSQL connection.
3. Count stored PostgreSQL rows by `_batch_id` and record the reconciled result.
4. Return the completed manifest after the source transaction closes. The finalizer checks the complete table set before triggering Spark.

SQL Server and PostgreSQL use independent connections and transactions. A halfway failure can leave diagnostic staging rows. Inspect the failed run and start a new full extraction when the underlying cause is fixed. The same run rejects a second extraction if batch records already exist. Use the exact successful run ID for a warehouse-only retry.

On schema or type errors compare the assessed source metadata, the actual allowed columns and the staging constraints. On network or TLS errors check the configured ODBC endpoint from Airflow. [Batch execution](../10-operations/batch-execution.md) · [Run-scoped checks](../06-data-processing/data-quality.md)
