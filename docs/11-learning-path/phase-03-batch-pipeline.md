# Phase 3 - Batch Pipeline

**Objectives:** trace a consistent SQL Server full snapshot, PostgreSQL metadata / raw batches, Spark transformation and ClickHouse publication.

**Read:** [Source Extraction](../04-data-ingestion/source-extraction.md), [Snapshot Consistency](../04-data-ingestion/snapshot-consistency.md), [Airflow](../04-data-ingestion/airflow.md), and [Spark Batch](../06-data-processing/spark-batch.md).

**Inspect:** the source table list, `students_staging/loader.py`, staging DDL, `spark/load_warehouse.py`, and `db/warehouse_clickhouse/warehouse_ddl.sql`.

**Exercise:** starting from an existing **successful** full staging run, identify its table batch IDs and check three fact grains. Find the ClickHouse **`warehouse_model_loads`** publication row (not the obsolete `_v2` marker), and explain why only its referenced `snapshot_id` is visible in published views. The local safety patch skips an already published full run.

**Expected result:** explain the one-transaction SQL Server source read, separate PostgreSQL target commits, and why the warehouse loader selects the latest successful full run instead of CDC history.

**Validation:** confirm actual source isolation setting with the DBA before a new extract. Older reports of isolation `OFF` were recorded in November 16 evidence; do not treat them as a current measurement. A 15-minute schedule would **not** convert this full-loader into an incremental SCD2 job.
