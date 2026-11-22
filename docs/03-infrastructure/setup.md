# Local environment setup

Use Docker with Compose v2, GNU Make, Python 3.11 or 3.12 with `venv`, and a reachable SQL Server. Allow access to image registries, package indexes and Maven for image builds and Spark dependencies. On WSL2, place the checkout on the Linux filesystem. The source endpoint must be reachable from Airflow and Connect inside Docker.

## Prepare a fresh checkout

```bash
python3 -m venv .venv
make install
test -f .env || cp .env.example .env
```

Update the source settings in the private `.env` before startup. `make up` calls initialization, which creates the local mount directories and fills supported `GENERATE_*` placeholders. `make init` runs that preparation independently. `make compose-config` checks interpolation without printing the resolved configuration.

| Settings | When to review them |
|---|---|
| `SOURCE_DB_*` | Set host, port, database, schema, approved reader and TLS options. Both Airflow and Debezium use this reader. Keep certificate verification enabled for a trusted source. |
| `POSTGRES_*`, `STAGING_DB_NAME`, `AIRFLOW_DB_NAME` | Local database endpoints and names. Airflow and staging use the PostgreSQL admin. |
| `AIRFLOW_*`, `BATCH_SNAPSHOT_SCHEDULE` | Airflow UI login and internal execution API. Leave the schedule blank for manual full snapshots. |
| `CLICKHOUSE_*`, `WAREHOUSE_DB_NAME`, `CDC_DB_NAME` | Local ClickHouse endpoints, admin and two distinct publication databases. Spark and the Grafana datasources use this admin. |
| `KAFKA_*`, `CONNECT_*`, `DEBEZIUM_*` | Broker endpoints, topic and connector identities, snapshot mode and retention. Keep identities paired with the persisted Kafka and Connect state. |
| `SPARK_*`, `STREAMING_*`, `CDC_CHECKPOINT_DIR` | Local execution and microbatch tuning. Keep the generation and checkpoint paired with the Kafka topics and ClickHouse ledger. |
| `GRAFANA_*`, `STUDENT_ID_HMAC_KEY` | Grafana UI login and stable pseudonymous Batch identifiers. Preserve the HMAC key and Airflow keys with their data. |

The template groups every setting by service and provides local defaults. Keep the real `.env` and resolved Compose output private. [Environment template](../../.env.example)

## Source readiness

Ask the source owner to verify the expected tables and reader access. Batch requires SQL Server snapshot isolation. CDC also requires the 15 capture instances, working Agent capture and cleanup jobs, and sufficient retention. The source readiness script reads configuration and permissions. Source enablement is a separate DBA operation. [Source prerequisites](../05-data-platform/source-and-staging.md#cdc-capture-prerequisites)

Follow the README to start the services and trigger the first Batch snapshot. The runbook covers partial startup, retries and restarts. [Quick start](../../README.md#quick-start) · [Runbook](../07-operations/runbook.md)
