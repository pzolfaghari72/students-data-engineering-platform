# Environment Configuration

This page is the authoritative reference for runtime configuration keys.
Values are intentionally not copied here. Store credentials in the ignored,
permission-restricted `.env` file or inject them through the deployment
environment. `.env.example` documents key names and sample/non-secret values;
it is never a runtime file.

## Variable groups

| Variables | Purpose and consumers | Required |
| --- | --- | --- |
| `PROJECT_NAME`, `TIMEZONE`, `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`, `https_proxy` | Compose project identity, service time zone and optional build/network proxy. | Project/time zone have documented defaults; proxy keys are optional. |
| `SOURCE_DB_HOST`, `SOURCE_DB_PORT`, `SOURCE_DB_NAME`, `SOURCE_DB_SCHEMA`, `SOURCE_DB_USER`, `SOURCE_DB_PASSWORD`, `SOURCE_DB_ODBC_DRIVER`, `SOURCE_DB_ENCRYPT`, `SOURCE_DB_TRUST_SERVER_CERTIFICATE`, `SOURCE_DB_CONNECTION_TIMEOUT`, `SOURCE_DB_SNAPSHOT_ISOLATION` | Source SQL Server target, ODBC connection, TLS and consistent batch isolation; Airflow and connector registration. | Required for source ingestion/connector. |
| `POSTGRES_HOST`, `POSTGRES_CONTAINER_PORT`, `POSTGRES_BOOTSTRAP_DB`, `POSTGRES_ADMIN_USER`, `POSTGRES_ADMIN_PASSWORD` | PostgreSQL service, bootstrap role and health check. The internal endpoint is shared by staging and Airflow. | Required by core initialization. |
| `STAGING_DB_NAME`, `STAGING_DB_USER`, `STAGING_DB_PASSWORD`, `STAGING_JDBC_DRIVER` | PostgreSQL staging schemas and Spark JDBC reads. Compose supplies the shared PostgreSQL endpoint. | Required for staging/batch/stream paths. |
| `AIRFLOW_DB_NAME`, `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_UID`, `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`, `AIRFLOW_JWT_SECRET`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API__SECRET_KEY`, `AIRFLOW_API_SERVER_URL` | Airflow metadata, file ownership and secret encryption. Compose supplies the shared PostgreSQL endpoint. | Required when Airflow is enabled; UID has a Compose default. |
| `CLICKHOUSE_HOST`, `CLICKHOUSE_HTTP_PORT`, `CLICKHOUSE_NATIVE_PORT`, `CLICKHOUSE_ADMIN_USER`, `CLICKHOUSE_ADMIN_PASSWORD` | ClickHouse service endpoints and bootstrap admin. | Required for warehouse service. |
| `WAREHOUSE_DB_NAME`, `WAREHOUSE_DB_USER`, `WAREHOUSE_DB_PASSWORD`, `WAREHOUSE_JDBC_DRIVER`, `STUDENT_ID_HMAC_KEY` | Analytical database, Spark writer and pseudonymous student-key hashing. Keep the HMAC key stable to preserve keys across snapshots; rotation changes them. | Required for warehouse loads. |
| `GRAFANA_DB_USER`, `GRAFANA_DB_PASSWORD`, `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | Grafana login and ClickHouse business-KPI datasource. Compose uses the canonical ClickHouse host and native port. | Required when Grafana dashboards are enabled. |
| `KAFKA_HOST`, `KAFKA_INTERNAL_PORT`, `KAFKA_CONTROLLER_PORT`, `KAFKA_EXTERNAL_PORT`, `KAFKA_EXTERNAL_ADVERTISED_HOST`, `KAFKA_NUM_PARTITIONS`, `KAFKA_TOPIC_RETENTION_MS`, `KAFKA_HEAP_OPTS` | Kafka KRaft listeners, advertised host, defaults and heap. Compose builds the bootstrap endpoint from the host and internal port. | Required for CDC. |
| `CONNECT_HOST`, `CONNECT_INTERNAL_PORT`, `CONNECT_METRICS_PORT`, `CONNECT_HEAP_OPTS`, `CONNECTOR_HEALTH_TIMEOUT_SECONDS` | Kafka Connect REST endpoint, JVM memory and registration wait. The JMX endpoint is used internally by Spark to detect Debezium snapshot completion; it is not scraped for monitoring. | Required for CDC/Connect. |
| `DEBEZIUM_CONNECTOR_NAME`, `DEBEZIUM_TOPIC_PREFIX`, `DEBEZIUM_SCHEMA_HISTORY_TOPIC`, `DEBEZIUM_SNAPSHOT_MODE` | Connector identity, topics and initial snapshot. | Required for Debezium. Snapshot mode is constrained to `initial` by the registration helper. |
| `CDC_MAX_RECORDS`, `CDC_MAX_SECONDS` | Bound the legacy Airflow CDC consumer work per run. | Required by the CDC DAG. |
| `STREAMING_APP_NAME`, `STREAMING_QUERY_NAME`, `STREAMING_CHECKPOINT_DIR`, `STREAMING_MAX_OFFSETS_PER_TRIGGER`, `STREAMING_KAFKA_GROUP_ID`, `WAREHOUSE_WRITE_BATCH_SIZE` | Spark streaming application, persistent checkpoint, trigger bounds, consumer naming and warehouse write size. | Required by the streaming service. |

Host-published ports are fixed in `docker-compose.yml`, not configured through
`.env`; edit the Compose port mappings to change them.

## Container-only aliases and discrepancies

Compose derives container aliases for staging and Airflow PostgreSQL access
from `POSTGRES_HOST` and `POSTGRES_CONTAINER_PORT`, and Grafana's ClickHouse
host/native port from `CLICKHOUSE_HOST` and `CLICKHOUSE_NATIVE_PORT`. It also
builds the Kafka bootstrap, Kafka Connect REST and Debezium snapshot-metric URLs
from their canonical hosts and ports. The Debezium endpoint is required for
Spark's snapshot-completion gate; there is no Prometheus scrape service.
These are container settings, not additional host-side `.env` values. Spark's
`WAREHOUSE_HOST` and `WAREHOUSE_HTTP_PORT` are similarly derived from
`CLICKHOUSE_HOST` and `CLICKHOUSE_HTTP_PORT`.
`AIRFLOW__CORE__PARALLELISM` and
`AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` are optional Compose tuning
overrides with defaults but are not listed in `.env.example`. `AIRFLOW_VERSION`
and image/plugin version constants are build-time metadata, not `.env` keys.

Grafana is configured only with the ClickHouse warehouse datasource. Its UI
port is host-published; the ClickHouse datasource uses the internal service
endpoint. Renaming a service or changing an internal port requires updating
the corresponding Compose/provisioning configuration.

## Handling secrets

Never paste values into documentation or commands that may enter shell history.
Use generated secrets from `make init`; back up `.env` securely because
Airflow's encrypted metadata depends on its Fernet key. Replace placeholders
before enabling any external connection. Do not silently substitute
environment-specific defaults in application code.
