# Airflow in This Project

Airflow 3.1.5 uses LocalExecutor and a PostgreSQL metadata database. It orchestrates **finite** full-snapshot and warehouse jobs; the long-running Spark Structured Streaming application is independent of Airflow.

| DAG | Execution | Tasks | Scope |
| --- | --- | --- | --- |
| `sqlserver_to_postgres_staging` | Manual, `schedule=None` | `create_pipeline_run` -> `load_consistent_snapshot` -> `finish_pipeline_run` | Consistent full SQL Server snapshot into PostgreSQL |
| `postgres_to_clickhouse_warehouse` | Manual, `schedule=None` | Locally patched: `verify_spark_runtime` -> `load_clickhouse_warehouse`; GitHub source may have only loader task | Spark transforms the latest successful **full staging snapshot** |
| `sqlserver_debezium_cdc_to_staging` | Paused | `consume_debezium_events` | Legacy bounded Kafka consumer, **not used alongside Spark CDC** |

The scheduler launches Spark batch work locally; the standalone Compose `spark-warehouse` service is an alternative, not a second concurrent publisher. The warehouse DAG is **not** an incremental CDC/SCD2 job; merely changing its schedule would not implement delta processing.

## Verified prerequisites on 2026-11-21

- DAG processor's local import-error check returned `No data found`.
- Java 17 and `spark-submit` version 3.5.6 existed inside `airflow-scheduler`.
- `/opt/spark-apps/load_warehouse.py` and `warehouse_keys.py` were readable.
- The locally applied safety patch inserted a preflight task and a published-full-run guard. These are not evidence of an executed successful warehouse task or a merged GitHub commit.

## Operating rules

Trigger a new full extraction only when a controlled baseline refresh is needed. Check all required staging batches before running the warehouse DAG. Keep CDC Airflow paused while the Spark CDC stream owns the source stream. Preserve metadata, source offsets and Spark checkpoints.

See [Batch Execution](../10-operations/batch-execution.md), [Streaming Execution](../10-operations/streaming-execution.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
