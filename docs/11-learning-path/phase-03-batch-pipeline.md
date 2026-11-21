# Phase 3 — Batch Pipeline

**Objectives:** follow a consistent SQL Server snapshot, staging metadata and
warehouse publication.

**Read:** [source extraction](../04-data-ingestion/source-extraction.md),
[snapshot consistency](../04-data-ingestion/snapshot-consistency.md),
[Airflow](../04-data-ingestion/airflow.md), and
[Spark batch](../06-data-processing/spark-batch.md).

**Inspect:** `loader.py`, `source_tables.py`, metadata/raw DDL,
`load_warehouse.py`, and `warehouse_ddl.sql`.

**Exercise:** using existing run metadata, trace the selected source run,
table batch IDs, all three fact-grain checks, and the final ClickHouse
`warehouse_model_loads_v2` publication row. Running a new source load
requires SQL Server and DBA approval; current snapshot isolation is disabled.

**Expected result:** identify that the same SQL Server transaction reads all
15 tables and Spark selects a completed full run rather than mixing batches.

**Validation:** compare each fact's output rows to its source rows and verify
the publication marker is last. Do not bypass missing source/DBA prerequisites.
