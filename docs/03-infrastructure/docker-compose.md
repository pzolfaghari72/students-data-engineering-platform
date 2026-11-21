# Docker Compose

`docker-compose.yml` is the authoritative service configuration. Compose
interpolates values from `.env`, creates one `platform` bridge network,
coordinates health/dependencies, and mounts persistent state.

## Service inventory

| Service | Technology / purpose | Compose profile | Persistent state | Key dependency |
| --- | --- | --- | --- | --- |
| `postgres` | PostgreSQL staging and Airflow metadata host | None (default service) | `postgres-data` | None |
| `postgres-init` | Create application databases/roles | None (one-shot) | None | Healthy PostgreSQL |
| `staging-schema-init` | Apply PostgreSQL DDL | None (one-shot) | PostgreSQL volume | `postgres-init` completed |
| `clickhouse` | Analytical warehouse | None (default service) | `clickhouse-data` | None |
| `clickhouse-init` | Apply ClickHouse DDL and initialize users | None (one-shot) | ClickHouse volume | Healthy ClickHouse |
| `airflow-init` | Migrate Airflow metadata/create admin | None (one-shot) | PostgreSQL metadata | `postgres-init` |
| `airflow-api-server` | Airflow UI/API | None (default service) | Logs bind mount / PG metadata | `airflow-init` |
| `airflow-scheduler` | Run finite Airflow DAG tasks, including local Spark warehouse work | None (default service) | Logs bind mount / PG metadata | Airflow/API and schema initializers |
| `airflow-dag-processor` | Parse DAG files | None (default service) | DAG/log bind mounts | `airflow-init` |
| `spark-warehouse` | One-shot manual Spark batch alternative | `batch` | No persistent app state | Healthy PostgreSQL and ClickHouse |
| `kafka` | Single-node KRaft broker/controller | None (default service) | `kafka-data` | None |
| `connect` | Debezium/Kafka Connect | None (default service) | Internal Kafka topics | Healthy Kafka |
| `debezium-register` | One-shot connector registration/check | None (one-shot) | Connector/offsets live in Kafka | Healthy Connect REST |
| `spark-cdc-stream` | Independent Spark streaming app | `streaming` | Checkpoint volume | PostgreSQL, ClickHouse, Kafka, Connect, registration complete |
| `grafana` | ClickHouse-backed business KPI dashboards | None (default service) | `grafana-data` | Healthy ClickHouse |

## Profiles and dependency behavior

Only `spark-warehouse` and `spark-cdc-stream` currently have profiles:
`batch` and `streaming`, respectively. All other services—including Kafka,
Connect, and `debezium-register`—are unprofiled. An unqualified
`docker compose up` includes unprofiled services and can therefore start CDC
infrastructure and attempt connector registration. `docker compose --profile
'*' up` additionally enables both Spark services; do not use either command
as the normal startup path.

`make up-pipeline` means core + Airflow + the Grafana KPI dashboard; it does
**not** start Kafka/Connect. `make dashboards-up` starts core and Grafana only.
`make cdc-up` starts Kafka/Connect and then attempts connector registration.
These Make targets select service names explicitly; the `cdc`, `tools`, and
`dashboards` profile flags in some targets do not currently correspond to
Compose profile assignments. `make streaming-up` builds on connector
registration and starts Spark; do not use it until the source,
consumer-ownership, and checkpoint/offset readiness gates are resolved.
Grafana has no dependency on Prometheus or an exporter stack. Connect's JMX
endpoint remains because Spark uses its snapshot-completion metric as a
bootstrap correctness gate.

The Airflow scheduler image contains Java and PySpark for its warehouse DAG,
which runs without a host Docker socket or another service. The standalone
`spark-warehouse` service remains for manual runs; do not run both paths
concurrently. Snapshot extraction and warehouse publication are separate,
manually triggered Airflow DAGs.

The profile graph is visualized in
[deployment architecture](../02-architecture/diagrams/deployment-architecture.md).
Use the [operations runbook](../10-operations/operational-runbook.md) for
verified commands.
