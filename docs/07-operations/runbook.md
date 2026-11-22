# Operational runbook

## Startup, restart and shutdown

Complete the source-owner checks and private configuration before first startup. `make up` initializes databases and Airflow, starts Kafka and Connect, verifies topics and registration, then starts Spark streaming and Grafana. A fresh connector can begin its initial SQL Server snapshot during startup. The first Batch extract is an explicit Airflow action. [Quick start](../../README.md#first-run) · [Local setup](../03-infrastructure/setup.md)

| Command | Effect |
|---|---|
| `make status` | Service state, including the CDC and streaming profiles |
| `make logs` | Tails logs across the profiles. Use `docker compose logs <service>` to focus on a service. |
| `make up-core` | PostgreSQL and ClickHouse with schema initialization |
| `make up-batch` | Database and Airflow services for Batch |
| `make up-pipeline` | Batch services and Grafana |
| `make cdc-up` | Kafka and Connect, topic checks and connector registration or verification |
| `make streaming-up` | CDC services, Spark streaming and Grafana |
| `make dashboards-up` | ClickHouse and Grafana |
| `make down` | Stops and removes project containers while preserving named volumes |

Use `make up` again for a routine restart. Preserve the connector name, Kafka topics, generation ID, checkpoint and databases. With a blank Batch schedule, deploys and restarts leave the choice to start a new extraction with the operator.

## Run or retry Batch

Unpause both Batch DAGs and trigger `sqlserver_to_postgres_staging`. Track the validated staging run ID through `postgres_to_clickhouse_warehouse` and `batch_publication_status`. Staging success means the extract finished and the handoff was requested. The final warehouse marker makes the snapshot available to Grafana.

A failed extraction requires a fresh staging run. A failed warehouse load can retry the same successful staging input with `make warehouse-load RUN_ID=123`, using its actual ID. During upgrades, apply the existing staging initializer if the DAG reports outdated metadata. Keep the raw batches needed by current and recent publications. [Batch failure handling](../04-data-pipelines/batch.md#operation-and-failure-handling)

## Monitor and recover CDC

The DBA verifies capture and cleanup jobs, retention and permissions. The read-only readiness SQL takes the source schema and connector login as parameters. Run it in the source database with `sqlcmd -b -d <database> -v SourceSchema=<schema> ConnectorLogin=<login> -i <script>`. Run any source enablement only with the source owner's authorization. [Readiness SQL](../../db/source_assessment/sqlserver_cdc_prerequisites.sql) · [Source requirements](../05-data-platform/source-and-staging.md#cdc-capture-prerequisites)

Spark can stay in `BOOTSTRAPPING` after the Connect task reports `RUNNING`. Check verified offsets and the ClickHouse publication. Spark can retry an uncommitted callback and revalidate an existing seal. Schedule resource-heavy Batch and replay work separately if the host is constrained.

- **Retention or lineage mismatch.** Compare the Kafka beginning and end offsets, saved checkpoint identity and ClickHouse commit ledger. Restore compatible state or plan a new isolated connector, topic namespace, checkpoint and baseline with the source owner.
- **Schema evolution.** Validate changes to source keys, operation envelopes and KPI fields. Coordinate capture-instance transitions with the DBA and preserve Debezium schema history.
- **Upgrade from the retired PostgreSQL CDC path.** Record the previous connector, topic IDs, offsets and generation health. Stop the old writer in an approved maintenance window. Reuse retained Kafka offsets only if they cover the full baseline and use a separate checkpoint for the new CDC database. Keep rollback state until the new generation has passed reconciliation.

An upgraded Spark volume can retain ownership from an older image. If the checkpoint root is root-owned, correct the mount directory during maintenance with the worker stopped. [CDC replay contract](../04-data-pipelines/cdc.md#verified-writes-deduplication-and-replay)

```bash
docker compose --profile cdc --profile streaming run -T --rm --no-deps \
  --user root --entrypoint chown spark-cdc-stream spark:spark /opt/spark/checkpoints
```

## Troubleshooting

| Symptom | First check |
|---|---|
| Source login or TLS failure | Verify the endpoint, approved reader and certificate trust from the affected container. |
| Batch finishes staging with no dashboard update | Check the triggered warehouse task and selected publication ID. |
| CDC totals are empty | Check whether a generation is active, then inspect bootstrap progress and the Connect task. |
| CDC metrics appear stale | Compare the latest Spark seal, sampled Kafka offsets, Connect status and source activity. |
| Grafana query error | Check the provisioned datasource UID and its ClickHouse database and access. |
| `make reconcile` returns no rows | Check that both products have published. |

### Read-only diagnostics

Run the first query in PostgreSQL staging and replace `123` with the selected staging run ID.

```sql
SELECT source_table, status, source_rows_expected, extracted_rows, inserted_rows
FROM metadata.extraction_batches
WHERE run_id = 123
ORDER BY source_table;
```

Run the publication query in the ClickHouse Batch database and the progress query in the CDC database.

```sql
SELECT source_run_id, snapshot_started_at, completed_at,
       fact_application_rows, fact_course_offering_rows, fact_enrollment_rows
FROM batch_publication_status;

SELECT generation_id, active_generation, status, verified_bootstrap_records,
       bootstrap_records, last_verified_at, offsets_behind_at_last_batch
FROM cdc_pipeline_health;
```

For a specific authorized source check, query the needed field from `cdc_current_state`. Substitute the approved key and column.

```sql
SELECT source_timestamp, JSONExtractRaw(row_image, 'StudentStatus') AS current_value
FROM cdc_current_state
WHERE source_table = 'Students' AND JSONExtractUInt(source_key, 'StudentId') = 12345;
```

Use `make reconcile` at a quiescent source boundary when possible. Record both publications and interpret the differences with source changes in mind. [Reconciliation guide](../04-data-pipelines/reconciliation.md)

## Backup and restore

Named volumes preserve data on one host. Record the selected Batch run, active CDC generation and latest verified offsets before maintenance. Preserve the corresponding private configuration and test restores in an isolated environment.

| State | Restore check |
|---|---|
| PostgreSQL staging and Airflow metadata | The selected run, raw batches and Airflow task history still resolve. |
| ClickHouse Batch | The publication selects the expected snapshot and counts. |
| Kafka and Connect, Spark checkpoint and ClickHouse CDC | Topic identities, retained offset ranges, Spark query ID and verified sink coverage agree. Restore these together. |
| Grafana and provisioning | Both datasource UIDs resolve and both dashboards can query their views. |
