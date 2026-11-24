"""Publish verified CDC changes to the incremental warehouse every 15 minutes."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="postgres_to_clickhouse_incremental",
    description="Relay sealed CDC history and publish incremental analytical state.",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["students", "cdc", "warehouse"],
)
def postgres_to_clickhouse_incremental():
    BashOperator(
        task_id="publish_verified_changes",
        bash_command='exec "${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON:-python3}}" /opt/spark-apps/incremental_warehouse.py',
        execution_timeout=timedelta(hours=2),
    )


dag = postgres_to_clickhouse_incremental()
