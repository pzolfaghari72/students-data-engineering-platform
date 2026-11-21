# Project Structure

The repository is organized by runtime component and data boundary.

| Path | Role |
| --- | --- |
| `airflow/dags/` | Batch/CDC DAGs and shared staging functions. |
| `spark/` | Batch loader and streaming application; separate Docker image. |
| `debezium/` | Create-only Kafka Connect registration helper and JMX image config. |
| `db/source_assessment/` | Source DDL, inspection SQL, historical findings and ERD. |
| `db/staging_postgres/` | Source CDC prerequisite SQL and staging schemas. |
| `db/warehouse_clickhouse/` | ClickHouse batch and CDC DDL. |
| `grafana/` | Business KPI dashboard, ClickHouse datasource provisioning and plugin image. |
| `scripts/` | Local initialization, environment validation and project setup helpers. |
| `tests/unit/` | CDC normalization/offset/SCD2 and registration safety tests. |
| `docs/` | Canonical architecture, learning and operational handbook. |

Python dependencies are separated by runtime: `pyproject.toml` is canonical
for host utilities, `airflow/requirements.txt` is installed with the official
Airflow 3.1.5/Python 3.12 constraints, `airflow/requirements-spark.txt` is
installed separately without those constraints, and `spark/requirements.txt`
contains the Spark application's extra Python packages. Root
`requirements.txt` remains the host install entry point used by `make install`.
Both runtimes pin `kafka-python` 2.3.3: 2.0.2 fails importing `KafkaConsumer`
on Airflow's Python 3.12, while 2.3.3 imports on both runtime Python versions.
The Airflow constraints are supplied by the official base image; JDBC and Spark
Kafka Maven coordinates remain in the Spark submit configuration. Unit tests
use `unittest` from the standard library, so there is no separate test package
set. The local `.venv/`, `.env`, and Airflow logs are not dependency inputs.
