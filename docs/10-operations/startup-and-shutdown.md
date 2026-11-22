# Startup and shutdown

Use [Quick start](../01-getting-started/quick-start.md) when selecting a workflow. Use this page when starting or stopping services without running a new extract.

| Action | Operational effect |
|---|---|
| `make up-core` | Starts the PostgreSQL and ClickHouse initializer graph and checks completion. |
| `make up-batch` | Brings up the Airflow execution environment and its dependencies. |
| `make dashboards-up` | Starts the analytical server dependencies and Grafana. |
| `make cdc-up` | Starts Kafka/Connect, checks topics and verifies or creates the configured connector. |
| `make streaming-up` | Adds Spark Structured Streaming and Grafana after CDC readiness. |
| `make down` | Stops the Compose project and preserves named volumes. |

For a controlled CDC worker restart use the same saved checkpoint and database; check topic retention first after an extended outage. For a maintenance operation that changes grants or the mounted checkpoint permissions, stop the worker before the change. Do not treat re-running startup as permission to reset the Kafka cluster or its internal topics.

For diagnosis use `make status` and the targeted service logs. Initializers may be exited after success. The Stream's application health comes from verified progress and generation publication. [Compose dependencies](../03-infrastructure/docker-compose.md) · [Recovery runbook](operational-runbook.md)
