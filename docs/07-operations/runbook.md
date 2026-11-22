# Operational runbook

## Start and operate the stack

Run `make install` once, then `make init`. Keep `.env` private. The template documents host bindings, internal endpoints, roles and application settings. Normal local host mappings bind to loopback. Supply SQL Server endpoint and credentials in `.env`. Keep source TLS verification enabled with a valid certificate, except for a separately controlled local fixture. Set the connector-specific reader variables if the approved CDC login differs from the batch login.

| Command | Effect |
|---|---|
| `make up-core` | Start database services and initialize schemas and grants |
| `make up-batch` | Start databases and Airflow |
| `make up` | Start batch and Grafana |
| `make dashboards-up` | Start ClickHouse and Grafana |
| `make cdc-up` | Start the broker and Connect, verify topic policies, and register or check the connector |
| `make streaming-up` | Complete CDC startup and start Spark plus Grafana |
| `make status` | Show state for all profiles |
| `make logs` | Tail service logs |
| `make down` | Stop all platform containers while retaining named volumes |

### Batch

Unpause `sqlserver_to_postgres_staging` and `postgres_to_clickhouse_warehouse` in Airflow. Trigger the snapshot DAG for a full extract. Track the staging run ID through the warehouse DAG and `batch_publication_status`. The extract DAG finishing means it staged the snapshot and requested processing. Use the warehouse DAG and publication view to confirm it is available to Grafana.

An extraction failure requires a new snapshot DAG run. A warehouse failure can be retried against the same validated staging run with `make warehouse-load RUN_ID=123`. An interrupted warehouse attempt can leave unreferenced rows in ClickHouse. Keep recent publications and any staging data needed for recovery. Before pruning, account for active attempts, the current and previous publications, and required rollback evidence.

Set `BATCH_SNAPSHOT_SCHEDULE` only when there is a reason to collect recurring full snapshots. The warehouse DAG is always event-triggered or manually started with an explicit run ID.

### CDC readiness

Have the SQL Server DBA verify snapshot isolation, the capture-job and cleanup-job status, retention, reader permissions and the expected 15 table mappings. Run the [read-only readiness script](../../db/source_assessment/sqlserver_cdc_prerequisites.sql) in the configured database with `sqlcmd -b -d <database> -v SourceSchema=<schema> ConnectorLogin=<login> -i <script>`. The [enablement script](../../db/staging_postgres/enable_source_cdc.sql) accepts `SourceDatabase` and `SourceSchema` and is for the authorized DBA on a designated source. The runtime accounts only need the approved read and metadata privileges.

Run `make streaming-up` after readiness. The data topics use the configured partition count and delete-only retention. Increase retention before a planned outage or a replay that could exceed it. Treat Connect storage and schema-history topics as durable operational state. The worker uses the environment config provider for newly registered connector credentials. Existing connector settings are preserved. Migrate a previously stored password in a reviewed maintenance window.

### CDC monitoring and recovery

Use **Live Student Analytics — CDC** for generation state, verified bootstrap progress, and sampled lag. The Connect REST status and Spark logs distinguish an idle source from a stopped query. Lag and live processing delay are sampled at successful microbatches. A bootstrap can finish after the broker already has some live records. A generation stays hidden until ClickHouse has verified the complete saved cut.

On a small host, avoid running full bootstrap, batch backfills and load tests concurrently. The dashboard reader and CDC writer limit query threads and can spill large aggregations to disk at the threshold configured by `CLICKHOUSE_DASHBOARD_GROUP_BY_BYTES`.

- **Ordinary restart.** Restart the same Spark service with the same generation name, topic IDs, checkpoint directory and ClickHouse database. Spark can retry an uncommitted callback automatically. An existing sealed batch is verified on replay. Avoid resetting a checkpoint to clear a routine error.
- **Retention or restore mismatch.** Compare Kafka beginning and end offsets, Spark checkpoint identity, and the ClickHouse commit ledger. Restore compatible state if available. Otherwise use a new connector/topic/checkpoint lineage and take a fresh baseline. Keep the last good published generation until the replacement passes reconciliation.
- **Schema evolution.** A compatible payload field addition remains in the raw envelope. Validate changes to the source key, event structure or KPI fields before rollout. Coordinate unsupported changes with the DBA using SQL Server’s capture-instance transition and preserve schema history.
- **Upgrade from the PostgreSQL-backed stream.** Capture the previous connector, source topic IDs and offsets, and the old generation’s health. Stop the old Spark writer in the maintenance window before applying the narrowed legacy warehouse grants. Replay verified Kafka offsets into the dedicated CDC database using a new checkpoint path. If the full baseline is unavailable, create a new connector name, topic prefix and schema-history topic with an approved initial snapshot. Promote Grafana with the new stack after the generation is verified. Keep the legacy checkpoint, PostgreSQL CDC objects, old ClickHouse events and Kafka lineage until dependency and rollback checks permit removal.

On upgraded installations, check that the Spark user can write to the root of the existing checkpoint volume. If an older volume is root-owned, correct only the mount directory while the worker is stopped.

```bash
docker compose --profile cdc --profile streaming run -T --rm --no-deps \
  --user root --entrypoint chown spark-cdc-stream spark:spark /opt/spark/checkpoints
```

## Reconciliation and local persistence

`make reconcile` compares current application, offering and enrollment counts from the two data products and prints the reference IDs, timestamps and observed lag. Query it while the source is quiescent or interpret changes after the batch snapshot before calling a difference a defect.

Named volumes preserve data across restarts on one host. For an upgrade or host-loss rehearsal, back up PostgreSQL and ClickHouse and preserve Kafka’s data and the compatible Spark checkpoint together. Verify restores in an isolated environment. Reserve volume removal and Kafka topic deletion for confirmed disposable resources.

See [environment setup](../03-infrastructure/setup.md), [service state and ports](../03-infrastructure/services.md), [batch failure handling](../04-data-pipelines/batch.md#operation-and-failure-handling), [CDC replay](../04-data-pipelines/cdc.md#verified-writes-deduplication-and-replay), and the [validation record](../06-observability-and-quality/testing-and-validation.md).
