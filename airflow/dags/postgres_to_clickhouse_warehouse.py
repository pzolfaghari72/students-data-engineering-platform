
"""Load an explicitly selected PostgreSQL staging snapshot into ClickHouse."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag


@dag(
    dag_id="postgres_to_clickhouse_warehouse",
    description="Build ClickHouse warehouse from PostgreSQL staging.",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        "source_run_id": Param(
            type=["integer", "string"],
            minimum=1,
            maximum=2**63 - 1,
            pattern=r"^0*[1-9][0-9]*$",
            title="Successful staging run ID",
            description="Use the run_id from metadata.pipeline_runs for a completed staging snapshot.",
        ),
    },
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(hours=4),
    },
    tags=["students", "warehouse", "spark"],
)
def postgres_to_clickhouse_warehouse():

    # LocalExecutor runs Spark inside the Airflow task environment.
    verify_runtime = BashOperator(
        task_id="verify_spark_runtime",
        bash_command=(
            "set -e; "
            ': "${SPARK_BATCH_MASTER:?Missing SPARK_BATCH_MASTER}"; '
            ': "${SPARK_BATCH_DRIVER_MEMORY:?Missing SPARK_BATCH_DRIVER_MEMORY}"; '
            ': "${SPARK_BATCH_SHUFFLE_PARTITIONS:?Missing SPARK_BATCH_SHUFFLE_PARTITIONS}"; '
            "command -v spark-submit >/dev/null; "
            "command -v java >/dev/null; "
            "test -r /opt/spark-apps/load_warehouse.py; "
            "test -r /opt/spark-apps/warehouse_keys.py; "
            '"${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON:-python3}}" '
            '-c "import clickhouse_connect, pyspark"'
        ),
        retries=0,
    )

    load_warehouse = BashOperator(
        task_id="load_clickhouse_warehouse",
        bash_command=(
            "spark-submit "
            "--py-files /opt/spark-apps/warehouse_keys.py "
            '--master "$SPARK_BATCH_MASTER" '
            '--driver-memory "$SPARK_BATCH_DRIVER_MEMORY" '
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "--conf spark.ui.showConsoleProgress=false "
            '--conf "spark.sql.shuffle.partitions=$SPARK_BATCH_SHUFFLE_PARTITIONS" '
            "--packages "
            "org.postgresql:postgresql:42.7.5,"
            "com.clickhouse:clickhouse-jdbc-all:0.9.8 "
            '/opt/spark-apps/load_warehouse.py --source-run-id "$SOURCE_RUN_ID"'
        ),
        env={"SOURCE_RUN_ID": "{{ dag_run.conf.get('source_run_id', '') }}"},
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=4),
    )

    verify_runtime >> load_warehouse


dag = postgres_to_clickhouse_warehouse()
