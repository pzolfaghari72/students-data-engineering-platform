# Spark Batch Warehouse Loader

The entry point `spark/load_warehouse.py` reads the latest successful **full** PostgreSQL raw snapshot and uses Spark JDBC transformations to produce nine dimensions and three facts in ClickHouse. It does **not** read `history.cdc_events` or maintain an incremental SCD Type 2 star schema.

## Transformations and checks

- Require a successful source full run and successful batches for the 13 input tables used by the model.
- Read large tables through JDBC with bounded fetch settings and configured local Spark resources.
- Pseudonymize applicant and student keys, derive date / academic dimensions, join related source tables.
- Validate dimension key uniqueness, required relationships and fact-grain/source count preservation before publication.
- Append rows to `dim_*_snapshots` and `fact_*_snapshots` using one UUID `snapshot_id`.
- Insert `warehouse_model_loads` **last**, allowing published views to select only a completed snapshot. A failure may leave unpublished rows; cleanup is not automated.

## Local safety patch (2026-11-21)

A locally applied update adds `_published_snapshot_exists` and skips rebuilding an already published **source full run**. The locally updated Airflow DAG also adds `verify_spark_runtime`; Java 17 and Spark 3.5.6 were checked in `airflow-scheduler`. These changes may be absent from the GitHub default branch until committed. The guard prevents common duplicate full publications, not multi-writer races and not delta processing.

`make warehouse-load` is an optional standalone Compose batch publisher. Do not overlap it with the Airflow warehouse task or with another warehouse publish.

See [Batch Execution](../10-operations/batch-execution.md), [SCD Type 2](../07-data-warehouse/scd-type-2.md), and [Warehouse Tables](../07-data-warehouse/fact-and-dimension-tables.md).
