# Prerequisites

Use the project as a single-node development stack, not as a production
deployment.

| Requirement | Why it is needed | Evidence / limitation |
| --- | --- | --- |
| Docker Engine or Docker Desktop with Compose v2 | Runs PostgreSQL, ClickHouse, Kafka, Connect, Spark, Airflow and optional Grafana KPI dashboards. | Compose definitions are in `docker-compose.yml`. |
| 16 GB host RAM recommended | Several services have explicit memory caps, but PostgreSQL, ClickHouse and Airflow do not all have Compose memory limits. | See [resource management](../03-infrastructure/resource-management.md). |
| Python 3.11 or 3.12 for host utilities/tests | `pyproject.toml` declares the host utility dependency; `requirements.txt` is the operational install entry point. Compose images provide workload dependencies. | Tests use Python's standard-library `unittest`; no third-party test dependency is required. |
| SQL Server access | Batch and Debezium connect to the external `Students` database. | Host route, credentials, ODBC/TLS settings and source permissions are external. |
| SQL Server DBA support for CDC | Enables database/table CDC and confirms SQL Agent/capture jobs and Debezium permissions. | CDC readiness report records the permission and snapshot-isolation blockers. |
| Outbound image/artifact access | Builds pull container images, Spark JDBC packages and the Grafana plugin. | Initial Spark package resolution may need Maven access. |

The source is not bundled as a runnable SQL Server container. Do not assume
that a successful Compose configuration test implies source reachability.
For setup and environment names see [installation](./installation.md) and
[environment configuration](./environment-configuration.md).
