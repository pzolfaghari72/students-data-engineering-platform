# Testing and acceptance

| Check | How to run | Expected result |
|---|---|---|
| Unit cases | `make test` | Batch completion and run identity, privacy keys, connector lineage, CDC normalization, offset gaps, failed writes and replay pass. |
| Deployment syntax | `make compose-config` and `python scripts/validate_environment.py --mode all` | Compose interpolation and the private configuration validate without printing values. |
| Airflow | `docker compose exec -T airflow-scheduler airflow dags list-import-errors --local` | No import errors and only the two supported batch DAGs appear in the current source. |
| ClickHouse SQL fixture | Run the isolated SQL command below | No fixture assertion throws. The fixture verifies batch publication order and CDC hidden writes, baseline, latest state, deletes, and replay. |
| Spark batch fixture | [batch_publication.py](../../tests/integration/batch_publication.py) | Spark processes the requested staging run while a newer one exists, reuses the published result on retry, and publishes a valid empty run. |
| Spark replay fixture | [streaming_replay.py](../../tests/integration/streaming_replay.py) | Spark writes synthetic Kafka records into an empty scratch CDC database and resumes the same checkpoint after a failure between the ClickHouse seal and the Spark commit. |

Run the SQL fixture with the ClickHouse binary from the pinned container. It uses temporary local databases.

```bash
set -o pipefail
{
  printf 'CREATE DATABASE cdc_fixture; USE cdc_fixture;\n'
  cat db/warehouse_clickhouse/cdc_warehouse_ddl.sql
  printf 'CREATE DATABASE batch_fixture; USE batch_fixture;\n'
  cat db/warehouse_clickhouse/warehouse_ddl.sql tests/integration/clickhouse_publication.sql
} | docker compose exec -T clickhouse clickhouse local --max_threads 2 \
    --background_pool_size 16 --background_schedule_pool_size 16 --multiquery
```

The Spark fixture expects an empty `students_fixture_*` ClickHouse database with CDC DDL and a restricted scratch writer, Kafka credentials for a disposable topic prefix, and the pinned Spark Kafka package. Run it with the existing Spark image and the repository Spark app on `PYTHONPATH`. Set `CDC_DB_NAME`, `CDC_DB_USER`, `CDC_DB_PASSWORD`, `WAREHOUSE_HOST`, `WAREHOUSE_HTTP_PORT` and `KAFKA_BOOTSTRAP_SERVERS` for the fixture. Its Kafka topics and checkpoint are temporary. Drop only the scratch ClickHouse database and writer that were created for the test.

For the batch fixture, apply the PostgreSQL staging DDL and [fixture rows](../../tests/integration/batch_staging_fixture.sql) to a disposable PostgreSQL database. Apply batch DDL to a disposable ClickHouse database. Set the warehouse and staging variables from the Spark batch task, then run `batch_publication.py` with the repository Spark app on `PYTHONPATH` and the pinned PostgreSQL and ClickHouse JDBC packages.

## External source and UI acceptance

1. On an authorized SQL Server fixture, change related rows during an extraction and confirm all staged tables reflect one consistent snapshot. Confirm all 15 source and staging counts, the run ID, warehouse read-back, and dashboard results. Interrupt a staging or warehouse attempt and verify the prior publication stays readable. Repeat the warehouse run using the same staging ID.
2. Run Debezium against an authorized synthetic source. Verify an initial snapshot, then insert, update, delete and reinsert a key and compare its Kafka position and ClickHouse current state. Test late consumer startup and a source/Kafka retention gap. Verify Kafka offsets and ClickHouse progress before and after a checkpoint-preserving restart.
3. In Grafana, verify both datasources and the live CDC and batch dashboards. Verify the dashboard account cannot read raw event images. Use `make reconcile` and record both reference boundaries and observed differences. Report actual results in [implementation status](../12-implementation/implementation-status.md).

Use synthetic data for mutation, replay and failure tests. The external source and the live legacy volumes can be inspected read-only while migration is pending.
