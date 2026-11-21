
"""Load the PostgreSQL staging snapshot into ClickHouse."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag


@dag(
    dag_id="postgres_to_clickhouse_warehouse",
    description="Build ClickHouse warehouse from PostgreSQL staging.",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(hours=4),
    },
    tags=["students", "warehouse", "spark"],
)
def postgres_to_clickhouse_warehouse():

    # warehouse-safety-patch-v1
    # Keep schedule=None: periodic snapshot republishing is NOT CDC/SCD Type 2.
    verify_runtime = BashOperator(
        task_id="verify_spark_runtime",
        bash_command=(
            "set -e; "
            "command -v spark-submit >/dev/null; "
            "command -v java >/dev/null; "
            "test -r /opt/spark-apps/load_warehouse.py; "
            "test -r /opt/spark-apps/warehouse_keys.py"
        ),
        retries=0,
    )

    load_warehouse = BashOperator(
        task_id="load_clickhouse_warehouse",
        bash_command=(
            "spark-submit "
            "--py-files /opt/spark-apps/warehouse_keys.py "
            "--master 'local[2]' "
            "--driver-memory 768m "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--conf spark.ui.showConsoleProgress=false "
            "--conf spark.sql.shuffle.partitions=2 "
            "--packages "
            "org.postgresql:postgresql:42.7.5,"
            "com.clickhouse:clickhouse-jdbc-all:0.9.8 "
            "/opt/spark-apps/load_warehouse.py"
        ),
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=4),
    )

    verify_runtime >> load_warehouse


dag = postgres_to_clickhouse_warehouse()
