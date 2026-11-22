# Compose services and configuration

Docker packages runtimes in images. Compose defines the `platform` network, service discovery, health dependencies and durable volumes. The [Compose file](../../docker-compose.yml) and [environment template](../../.env.example) are the configuration references.

| Runtime | Version | Internal endpoint | Default loopback port |
|---|---|---|---|
| Airflow API/UI | 3.1.5 | `airflow-api-server:8080` | 18080 |
| PostgreSQL | 16 | `postgres:5432` | 15433 |
| ClickHouse HTTP/JDBC and native | 24.8 | `clickhouse:8123`, `clickhouse:9000` | 18123 and 19000 |
| Kafka | 3.9.1 | `kafka:9092` | 19094 |
| Connect and Debezium | 2.7.3 | `connect:8083` | 18083 |
| Spark | 3.5.6 | Airflow task for Batch, `spark-cdc-stream` for CDC | No application port published |
| Grafana | 11.5.2 | `grafana:3000` | 13000 |

Use the Make targets to start the appropriate services and wait for initialization. The `cdc` profile adds Kafka, Connect and registration. The `streaming` profile adds the Spark worker. Kafka runs a single KRaft broker/controller, with distinct advertised addresses for containers and host clients. SQL Server runs externally. The Airflow and Connect services can route to the host gateway through `host.docker.internal` when that matches the source topology. [Compose profiles](https://docs.docker.com/compose/how-tos/profiles/)

| State boundary | What persists |
|---|---|
| PostgreSQL | Staging, extraction metadata and Airflow state |
| ClickHouse | Two databases and their publication and commit ledgers |
| Kafka | Business topics, Connect config/status/offsets, Debezium schema history and broker lineage |
| Spark | Checkpoint volume with query and progress state |
| Grafana | Local application state. Dashboard definitions and datasources come from provisioning. |

Run full bootstrap, warehouse backfills and builds separately on a small host. Watch memory, disk and health through `docker stats` and service logs. ClickHouse's local profile limits query threads and can spill aggregations. For startup ordering, backup and checkpoint ownership, see the [runbook](../07-operations/runbook.md).
