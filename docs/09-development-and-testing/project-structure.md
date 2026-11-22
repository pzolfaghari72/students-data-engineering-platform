# Project structure and tooling

| Path | Owns |
|---|---|
| `airflow/dags/` | Batch DAGs, ODBC extraction, staging metadata and the shared source allowlist |
| `spark/` | Batch transformation/publication, CDC normalization/streaming and the stream image |
| `debezium/` | Connect image, metrics and connector registration |
| `db/source_assessment/` | Captured source DDL, profiling SQL/results and read-only CDC readiness SQL |
| `db/staging_postgres/` | Staging DDL and the DBA-operated source CDC enablement file |
| `db/warehouse_clickhouse/` | Both warehouse schemas and read-only reconciliation |
| `grafana/` and `clickhouse/` | Provisioning, dashboards, role-facing configuration and listener settings |
| `scripts/` | Host setup, authentication roles, environment validation and topic preparation |
| `tests/unit/` and `tests/integration/` | Synthetic unit inputs and disposable runtime fixtures |
| `docs/` | Learning, operating guides, decisions and evidence |

Python manages orchestration, configuration, extraction and app-level validation. On the host, [pyproject.toml](../../pyproject.toml) supports Python 3.11–3.12 and declares the utility dependencies. In Airflow the pinned providers and Python DB clients are installed into the Airflow image. The separate Spark batch requirements preserve the Spark/Py4J versions. The [stream requirements](../../spark/requirements.txt) supply the ClickHouse and Kafka Python clients. JVM JDBC and Kafka connector dependencies are selected by the Spark submit configuration.

SQL expresses source reads, staging constraints, warehouse schemas, metric queries and diagnostics. Track the database dialect when adapting a snippet. SQL Server brackets, PostgreSQL quoted identifiers and ClickHouse server parameters serve different purposes. [Coding and configuration standards](coding-standards.md)
