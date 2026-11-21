# Batch Execution

Full source snapshot and warehouse publication are **two separately triggered Airflow DAGs**. Do not schedule the existing warehouse full-loader every 15 minutes: it still reads a complete PostgreSQL full snapshot rather than deltas.

## Controlled full-source refresh

1. Confirm source SQL Server availability, source permissions, and snapshot isolation. Historical reports of a disabled setting are dated, not proof of present configuration.
2. In Airflow (`http://localhost:18080`), trigger `sqlserver_to_postgres_staging` only if a new baseline is required.
3. Confirm `create_pipeline_run` -> `load_consistent_snapshot` -> `finish_pipeline_run` succeeded and all 15 extraction batches for that run are successful.
4. Trigger `postgres_to_clickhouse_warehouse` manually.
5. On the locally patched code, confirm `verify_spark_runtime` succeeded before `load_clickhouse_warehouse`. Verify the published ClickHouse row in `warehouse_model_loads` is linked to the intended `source_run_id`.
6. Check the exposed views (`fact_application`, `fact_course_offering`, `fact_course_enrollment`) against that published generation. Grafana uses those views.

The local 2026-11-21 loader patch checks whether the successful full `source_run_id` is already published and skips duplicate rebuilds. This protects routine sequential reruns but is **not** a lock for concurrent publishers or a CDC/SCD2 implementation. If GitHub has not yet received the patch, the warehouse DAG may show only one task.

## Read-only warehouse verification

```bash
docker compose exec -T clickhouse sh -c '
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database students_warehouse --query "
SELECT
  (SELECT count() FROM fact_course_enrollment) AS enrollments,
  (SELECT count() FROM fact_course_offering) AS offerings,
  (SELECT count() FROM fact_application) AS applications
"'
```

`make warehouse-load` is a one-shot alternative using `spark-warehouse`; do not run it at the same time as the Airflow warehouse DAG. Failed attempts can leave unpublished physical rows, not a new current view.

This is the **batch** flow only. CDC event writes and query-time CDC views are separate. See [Spark Batch](../06-data-processing/spark-batch.md), [Data Publication](../07-data-warehouse/data-publication-and-reconciliation.md), and [Airflow](../04-data-ingestion/airflow.md).
