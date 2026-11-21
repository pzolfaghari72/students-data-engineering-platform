# Docker Fundamentals in This Repository

Docker packages each workload with its runtime and dependencies. This project
uses Compose for local service lifecycle and networking; it is not a
multi-host orchestrator.

| Concept | Project example | Why it matters |
| --- | --- | --- |
| Image | `postgres:16-alpine`, custom Airflow/Spark/Connect/Grafana images | An image is the reproducible template; application images add ODBC, JDBC or plugins. |
| Container | Compose service instance such as `postgres` | A running process with isolated filesystem/network lifecycle. |
| Dockerfile | `airflow/plugins/Dockerfile`, `spark/Dockerfile`, `debezium/Dockerfile`, `grafana/Dockerfile` | Installs runtime dependencies or the pinned plugin. |
| Service | `clickhouse`, `airflow-scheduler`, `spark-cdc-stream` | Names a container definition and is the internal DNS name. |
| Port mapping | Loopback-published host ports from `.env` | Host tools use published ports; containers use service DNS and internal ports. |
| Bind mount | `./spark:/opt/spark-apps:ro` | Shares source/configuration from the checkout; `:ro` prevents container writes. |
| Named volume | `postgres-data`, `kafka-data`, Spark checkpoint volume | Holds state independent of a container's writable layer. |
| Health check | PostgreSQL, ClickHouse and Kafka | Reports basic readiness; it does not prove a source-to-warehouse integration. |
| Restart policy | Long-running services use `unless-stopped`; jobs use `no` | A service restart is not a data recovery test. |

For actual dependencies, profiles, persistence and resource limits see
[Compose](./docker-compose.md), [networking](./services-and-networking.md),
[volumes](./volumes-and-persistence.md), and
[resource management](./resource-management.md). Routine commands are
centralized in the [operations runbook](../10-operations/operational-runbook.md).
