
"""Extract one consistent SQL Server snapshot and trigger its publication."""

import os
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag
from airflow.task.trigger_rule import TriggerRule

from students_staging.loader import (
    DAG_ID,
    create_pipeline_run,
    finish_pipeline_run,
    load_consistent_snapshot,
)


@dag(
    dag_id=DAG_ID,
    description="Stage a full SQL Server snapshot and trigger its warehouse load.",
    schedule=os.getenv("BATCH_SNAPSHOT_SCHEDULE") or None,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(hours=4),
    },
    tags=["students", "staging", "initial-load"],
)
def sqlserver_to_postgres_staging():

    start = PythonOperator(
        task_id="create_pipeline_run",
        python_callable=create_pipeline_run,
    )

    snapshot = PythonOperator(
        task_id="load_consistent_snapshot",
        python_callable=load_consistent_snapshot,
        execution_timeout=timedelta(hours=4),
    )

    finish = PythonOperator(
        task_id="finish_pipeline_run",
        python_callable=finish_pipeline_run,
        trigger_rule=TriggerRule.ALL_DONE_MIN_ONE_SUCCESS,
    )

    # A deterministic ID makes a repeated handoff safe for this staging run.
    publish = TriggerDagRunOperator(
        task_id="trigger_warehouse",
        trigger_dag_id="postgres_to_clickhouse_warehouse",
        trigger_run_id="staging__{{ ti.xcom_pull(task_ids='finish_pipeline_run') }}",
        conf={
            "source_run_id": "{{ ti.xcom_pull(task_ids='finish_pipeline_run') }}",
        },
        skip_when_already_exists=True,
    )

    start >> snapshot >> finish >> publish
    # Finalize failed extraction only if the staging run was created.
    start >> finish


dag = sqlserver_to_postgres_staging()
