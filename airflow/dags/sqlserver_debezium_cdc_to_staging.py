
"""Consume SQL Server Debezium CDC events into PostgreSQL staging."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag

from students_staging.cdc_loader import consume_debezium_events


@dag(
    dag_id="sqlserver_debezium_cdc_to_staging",
    description="Process Kafka CDC events into PostgreSQL history and SCD2.",
    schedule=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["students", "cdc", "postgres", "scd2"],
)
def sqlserver_debezium_cdc_to_staging():

    consume = PythonOperator(
        task_id="consume_debezium_events",
        python_callable=consume_debezium_events,
    )

    return consume


dag = sqlserver_debezium_cdc_to_staging()
