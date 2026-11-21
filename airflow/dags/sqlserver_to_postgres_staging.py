
"""One-time consistent SQL Server snapshot into PostgreSQL staging."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
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
    description="Initial full load from SQL Server into PostgreSQL staging.",
    schedule=None,
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
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> snapshot >> finish


dag = sqlserver_to_postgres_staging()
