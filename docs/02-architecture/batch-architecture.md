# Batch Architecture

The batch path has **two separate, manually triggered Airflow DAGs**. A source-consistent full snapshot is loaded into PostgreSQL first. After validation, a Spark batch task reads the latest successful full run and publishes the ClickHouse analytical warehouse.

## Execution

1. `sqlserver_to_postgres_staging` creates a run in PostgreSQL metadata.
2. The extractor reads the 15 SQL Server source tables through a single `SNAPSHOT` transaction and records table-specific batches in `raw` / `metadata`.
3. Its final task records `SUCCESS`, `PARTIAL`, or `FAILED`; a warehouse job must only use a fully successful run.
4. `postgres_to_clickhouse_warehouse` is triggered separately. The **locally patched DAG** adds `verify_spark_runtime` ahead of `load_clickhouse_warehouse`; GitHub may still contain the one-task version until the local change is committed.
5. Spark loads required raw tables, builds nine dimensions and three facts, checks key uniqueness / row-count preservation, and appends rows under a new `snapshot_id`.
6. Spark inserts the `warehouse_model_loads` record last. The published ClickHouse views select that completed snapshot.

The locally patched Spark loader checks whether the source full run has already been published and skips it when possible. This guard is **full-snapshot deduplication**, not incremental CDC processing. It does not solve concurrent multi-writer races; do not run the Airflow and standalone Compose warehouse publishers together.

The current physical tables are `dim_*_snapshots` and `fact_*_snapshots`, **without** the obsolete `_v2` suffix. SQL Server snapshot-isolation setting reports in dated 2026-11-16 material are historical, not proof of current configuration; confirm prerequisites when starting a new full extract.

See [Batch Diagrams](./diagrams/batch-pipeline.md), [Warehouse Tables](../07-data-warehouse/fact-and-dimension-tables.md), and [Batch Execution](../10-operations/batch-execution.md).
