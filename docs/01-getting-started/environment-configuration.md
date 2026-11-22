# Environment configuration

[.env.example](../../.env.example) is the reference for variable names, template values and defaults. `.env` is the private local configuration. Keep it ignored and use the Compose service DNS names for in-network connections.

| Group | Key examples | Role |
|---|---|---|
| Project | `PROJECT_NAME`, `TIMEZONE` | Compose namespace and runtime time zone. A different project name normally selects different named volumes. |
| Source | `SOURCE_DB_HOST`, `SOURCE_DB_PORT`, `SOURCE_DB_NAME`, `SOURCE_DB_SCHEMA`, `SOURCE_DB_ENCRYPT`, `SOURCE_DB_TRUST_SERVER_CERTIFICATE` | Source endpoint and TLS. Use trusted certificates for shared environments. |
| Source identities | `SOURCE_DB_USER`, `SOURCE_DB_PASSWORD`, `SOURCE_CDC_USER`, `SOURCE_CDC_PASSWORD` | The optional CDC pair can use a separately authorized reader. Empty CDC settings fall back to the source credentials. |
| Airflow and batch | `AIRFLOW_UID`, `AIRFLOW_API_SERVER_URL`, `BATCH_SNAPSHOT_SCHEDULE`, `SPARK_BATCH_DRIVER_MEMORY`, `STUDENT_ID_HMAC_KEY` | Local volume access, task API, optional snapshot cadence, Spark resources and stable pseudonymous keys. |
| Databases | `STAGING_DB_NAME`, `AIRFLOW_DB_NAME`, `WAREHOUSE_DB_NAME`, `CDC_DB_NAME` | Data boundary and authentication. Keep the two ClickHouse names and writer accounts distinct. |
| Kafka and Connect | `KAFKA_CLUSTER_ID`, `KAFKA_TOPIC_RETENTION_MS`, `KAFKA_NUM_PARTITIONS`, `DEBEZIUM_CONNECTOR_NAME`, `DEBEZIUM_TOPIC_PREFIX` | Broker, topic and connector identity. These values matter to retained state. |
| Streaming | `STREAMING_QUERY_NAME`, `STREAMING_GENERATION_ID`, `CDC_CHECKPOINT_DIR`, `STREAMING_MAX_OFFSETS_PER_TRIGGER`, `STREAMING_TRIGGER_INTERVAL` | Preserve the first three settings for routine restart. Trigger and maximum offsets set processing cadence and workload size. |
| Serving | `GRAFANA_DB_USER`, `GRAFANA_HOST_PORT`, `CLICKHOUSE_DASHBOARD_GROUP_BY_BYTES` | Read-only dashboard account, host binding and external aggregation threshold. |

Run the mode-specific validator with the host virtual environment.

```bash
.venv/bin/python scripts/validate_environment.py --mode batch
.venv/bin/python scripts/validate_environment.py --mode cdc
```

Compose derives settings such as JDBC URLs, the Connect REST URL and the Spark checkpoint location passed inside the container. The host and container ports can differ. A literal password in a URI can require URL encoding for the Airflow database connection. Keep generated secrets stable across container recreation and avoid sharing raw Compose output or connection strings. [Networking and port map](../03-infrastructure/services-and-networking.md)
