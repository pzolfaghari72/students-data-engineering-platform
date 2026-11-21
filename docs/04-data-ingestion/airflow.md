# Airflow in This Project

Airflow orchestrates bounded workflows. Its services use LocalExecutor and a
PostgreSQL metadata database; it is not the runtime for an indefinitely
running Structured Streaming query.

| Airflow concept | Project use |
| --- | --- |
| DAG | `sqlserver_to_postgres_staging` and `postgres_to_clickhouse_warehouse` are manually triggered batch DAGs. `sqlserver_debezium_cdc_to_staging` is scheduled every five minutes and created paused. |
| Task | Snapshot DAG: create run → one `load_consistent_snapshot` task → finalize status. Warehouse DAG: one `load_clickhouse_warehouse` Spark task. CDC DAG: one bounded `consume_debezium_events` task. |
| Scheduler | Creates finite task runs and applies schedule/retry rules. |
| DAG processor | Parses mounted DAG files. |
| API server | UI/API endpoint. |
| Executor | LocalExecutor runs task processes within the Airflow deployment. |
| Retry/timeout | Snapshot uses no retry and a four-hour task limit; warehouse uses one retry, five-minute delay and four-hour task limit; CDC has one retry, two-minute delay and ten-minute task limit. |
| Catchup/backfill | All three DAGs set `catchup=False`; they do not replay every historical schedule automatically. |
| XCom | Snapshot run ID passes from run creation to snapshot/finalization tasks. |

The full snapshot is one transaction across source tables but writes target
chunks independently. The warehouse DAG is separate: after verifying the
staging run, an operator manually triggers it to publish the latest successful
snapshot. The CDC task bounds each run by record count/time and commits
PostgreSQL event history and consumer offsets in one transaction. The CDC DAG
is created paused; current runtime state and prerequisites are recorded in the
[readiness report](../13-validation/cdc-readiness-report.md).

Implementation map: DAGs and shared staging functions are under `airflow/dags/`;
the custom Airflow image is defined in `airflow/plugins/Dockerfile`.
Relevant entry points are `airflow/dags/sqlserver_to_postgres_staging.py`,
`airflow/dags/postgres_to_clickhouse_warehouse.py`,
`airflow/dags/sqlserver_debezium_cdc_to_staging.py`, and
`airflow/dags/students_staging/`. Use the
[batch sequence diagram](../02-architecture/diagrams/batch-pipeline.md) and
[runbook](../10-operations/batch-execution.md) for operations.
