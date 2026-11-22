# Airflow orchestration

Airflow represents finite work as DAGs. A DAG describes dependencies, a DAG run represents one execution, and a task instance tracks a task within that run. The DAG processor loads definitions, the scheduler queues tasks and LocalExecutor runs work on the scheduler host. The API server serves the UI and task execution API. The metadata database holds task and run state.

| DAG | Tasks and role |
|---|---|
| `sqlserver_to_postgres_staging` | Create a staging run, extract the consistent snapshot, finalize the manifest, then trigger the warehouse with its exact run ID |
| `postgres_to_clickhouse_warehouse` | Verify that Java and Spark are available, then run the Spark application for the supplied `source_run_id` |

XCom carries the staging run ID and a small manifest of table/batch metadata. The source rows stay in PostgreSQL. The dedicated warehouse DAG allows a successful staging run to be reprocessed without touching SQL Server again. Its single active run is part of the supported publishing contract. Batch has no default cadence. Set `BATCH_SNAPSHOT_SCHEDULE` only for a desired recurring snapshot.

On failures, compare the DAG graph, task logs, the metadata run and the ClickHouse publication view. A successful trigger only proves handoff. For parsing checks use `docker compose exec -T airflow-scheduler airflow dags list-import-errors --local`. [Official Airflow concepts](https://airflow.apache.org/docs/apache-airflow/3.1.5/core-concepts/index.html) · [Batch operation](../10-operations/batch-execution.md)
