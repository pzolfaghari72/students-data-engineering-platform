# Testing and validation

## Local checks

| Check | Command or evidence |
|---|---|
| Python logic | `make test` covers staging completion, COPY text encoding, key pseudonymization, connector registration, event normalization, offset gaps and replay. |
| Configuration | `make compose-config` checks both Compose profiles. `.venv/bin/python scripts/validate_environment.py --mode all` checks the private settings without printing their values. |
| Airflow parsing | `docker compose exec -T airflow-scheduler airflow dags list-import-errors --output json` reads persisted import errors. Add `--local` to parse the mounted files. |
| Published data | Use `make reconcile` after both products publish. Read `batch_publication_status` and `cdc_pipeline_health` to verify the selected run and generation. |
| Grafana | Verify both datasource health checks and run the panel queries. CDC operation panels need an active generation with live events to return event rows. |

## Integration fixtures

Use disposable resources for the integration tests. The SQL fixture below uses the ClickHouse local process and its temporary scratch databases.

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

- **Batch fixture.** Apply the staging DDL and fixture rows to a disposable PostgreSQL database, and the warehouse DDL to a disposable ClickHouse database. Supply the job's staging and warehouse environment variables. Run the fixture with the Batch Spark runtime and the pinned PostgreSQL and ClickHouse JDBC libraries. It checks exact run selection, retry and a valid empty snapshot. [Fixture rows](../../tests/integration/batch_staging_fixture.sql) · [Batch fixture](../../tests/integration/batch_publication.py)
- **CDC replay fixture.** Apply the CDC DDL to an empty `students_fixture_*` ClickHouse database. Supply `CDC_DB_NAME`, `CDC_DB_USER`, `CDC_DB_PASSWORD`, `WAREHOUSE_HOST`, `WAREHOUSE_HTTP_PORT` and `KAFKA_BOOTSTRAP_SERVERS`. Use the streaming runtime with its pinned Kafka package. The fixture creates temporary topics and a checkpoint, simulates the Connect completion response, then interrupts processing after a seal to test replay. [Streaming fixture](../../tests/integration/streaming_replay.py)

## Implementation status

The following observations refer to the local deployment on **22 November 2026**.

| Check | Observed result |
|---|---|
| Unit and configuration checks | 28 unit tests passed. Both Compose profiles and the private `all` mode validated. |
| Airflow and fresh Batch publication | Both DAG runs succeeded. The extract reconciled 15 of 15 tables in staging run `1`, and the triggered warehouse run `staging__1` published it. The published counts were 600,000 applications, 59,279 offerings and 3,724,654 enrollment attempts. |
| Source readiness | The configured reader previously verified SQL Server snapshot isolation and all 15 CDC capture instances. Agent job and retention details require DBA access. |
| Current CDC | Connect and its task reported `RUNNING`. The new generation was still `BOOTSTRAPPING` during the read-only check, with 1,977,545 of 4,623,487 bootstrap records verified. |
| Updated Grafana CDC dashboard | Both datasources passed health checks. All seven queries in the updated CDC dashboard executed, including count and trend queries at 15-minute and 24-hour ranges. Event panels returned an empty result during bootstrap. |

### Remaining end-to-end checks

After the new CDC generation activates, compare both published boundaries with `make reconcile`. Use an authorized SQL Server fixture to perform insert, update, delete and reinsertion. Verify each change in Kafka, ClickHouse current state and Grafana, including event counts and delete visibility. Use the fixture environment to exercise checkpoint-preserving restart and retention failures.

## Live CDC cutover

In the earlier 22 November deployment, Spark replayed all 4,623,487 retained records, resumed after a ClickHouse restart and published the CDC generation. At that boundary, Batch run `2` and CDC agreed on applications, offerings and enrollment attempts. All 21 queries in the dashboard set of that deployment returned data after the reader grant repair. The accompanying screenshot predates the current event panels. [Grafana capture](../assets/screenshots/grafana-live-cdc-publication.png) · [Grant repair](grafana.md#batch-reader-grant-repair)

Earlier integration runs separately covered pinned Batch input and empty snapshots, ClickHouse publication assertions, and recovery after an injected failure between the Spark callback seal and checkpoint commit. The CDC fixture produced five logical events from seven transport records. That fixture used a simulated Connect offset response. An earlier full-workload test also encountered an out-of-memory restart. The empty Batch case passed when run separately.

The prior validation notes remain available at `55843c3:docs/assets/validation-2026-11-21.md`.
