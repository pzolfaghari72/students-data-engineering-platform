# Batch architecture

Batch separates source extraction from analytical processing. Staging protects the source from repeated Spark reads and provides a durable, identifiable basis for warehouse recovery. A full snapshot can be replayed to the warehouse without reopening the source transaction.

1. The extractor opens one SQL Server `SNAPSHOT` transaction for 15 tables. It writes typed rows to PostgreSQL and checks the source count, fetched count and stored count.
2. The finalizer compares the extractor's manifest with the successful, reconciled batches. Airflow triggers the warehouse DAG with the exact `source_run_id`.
3. Spark reads the corresponding staging batches through JDBC, uses SQL/DataFrames to build nine dimensions and three facts, and validates grain and referenced lookups.
4. The loader writes candidate tables under a new `snapshot_id`, reads them back from ClickHouse, and appends the publication marker after verification. Views rank published source runs before publication timestamps.

The two DAGs use LocalExecutor. The warehouse task launches Spark locally from the Airflow environment. Snapshot scheduling is optional, and the warehouse DAG receives an explicit run ID for both automated handoff and manual retries.

ClickHouse plain MergeTree keeps candidate rows even if a write attempt fails. The publication marker, distinct-key checks and serialized DAG runs address visibility and retry scope. [Batch sequence](diagrams/batch-pipeline.md) · [Batch loader](../06-data-processing/spark-batch.md) · [Operations](../10-operations/batch-execution.md)
