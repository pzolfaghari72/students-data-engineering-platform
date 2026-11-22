# Services and networking

Compose provides DNS on the `platform` bridge. Runtime clients use service names and container-side ports. Host tools use loopback bindings and the published ports. The same word `localhost` refers to different network namespaces inside the host, a container and the shared ClickHouse initialization network.

| Interface | Internal endpoint by default | Host loopback port and variable |
|---|---|---|
| PostgreSQL | `postgres:5432` | `15433` via `POSTGRES_HOST_PORT` |
| ClickHouse HTTP and JDBC | `clickhouse:8123` | `18123` via `CLICKHOUSE_HOST_HTTP_PORT` |
| ClickHouse native, used by Grafana | `clickhouse:9000` | `19000` via `CLICKHOUSE_HOST_NATIVE_PORT` |
| Kafka | `kafka:9092` for clients | `19094` via `KAFKA_HOST_PORT` |
| Connect REST | `connect:8083` | `18083` via `CONNECT_HOST_PORT` |
| Airflow API/UI | `airflow-api-server:8080` | `18080` via `AIRFLOW_HOST_PORT` |
| Grafana UI | `grafana:3000` | `13000` via `GRAFANA_HOST_PORT` |

Kafka clients follow the addresses advertised by the broker after bootstrap. The external advertised host must be reachable from a host client, and the internal listener must stay reachable by Connect and Spark. The broker/controller ports have different roles. Connect's JMX exporter is an internal diagnostic endpoint.

The external SQL Server is reached via the configured `SOURCE_DB_HOST` and port. The Airflow and Connect Compose services can map `host.docker.internal` to the host gateway when that is the correct source route. Browser reachability on the host does not establish connectivity from the containers. [Source connectivity decision](../adr/ADR-005-source-connectivity.md) · [Configuration reference](../01-getting-started/environment-configuration.md)
