# Prerequisites

This is a single-node local training stack, not a production deployment.

| Requirement | Why it matters | Guidance |
| --- | --- | --- |
| Docker Engine / Desktop with Compose v2 | Runs PostgreSQL, ClickHouse, Kafka, Connect, Spark, Airflow and Grafana | Use project Make targets for controlled startup; do not start all optional services blindly. |
| Sufficient RAM and disk | ClickHouse, Kafka, Spark and Airflow can contend for memory | Around 16 GB host RAM is recommended; the Docker allocation may be lower. See [Resource Management](../03-infrastructure/resource-management.md). |
| Python 3.11 or 3.12 | Host utilities and unit tests | Workload Python dependencies come from the container images. |
| External SQL Server `Students` access | Full snapshot and Debezium source | Required host routing, ODBC/TLS configuration and credentials are not bundled. |
| SQL Server source/DBA cooperation | Snapshot isolation and CDC capture/correction | A **2026-11-16 historical report** recorded permissions and isolation blockers; later successful staging and CDC progress mean those old observations must not be described as current without rechecking. |
| Network access for builds | Docker images, JDBC Maven packages, Grafana plugin | Needed for initial build/dependency downloads; not for reading already recorded health reports. |

SQL Server is external to the project, not an included container. Successful `docker compose config --quiet` validates Compose structure but does not verify source permissions or a complete pipeline. Do not share `.env` or student records.

See [Installation](./installation.md), [Environment Configuration](./environment-configuration.md), [Quick Start](./quick-start.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
