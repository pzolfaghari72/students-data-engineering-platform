# Testing, quality and implementation evidence

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
3. In Grafana, verify both datasources and the live CDC and batch dashboards. Verify the dashboard account cannot read raw event images. Use `make reconcile` and record both reference boundaries and observed differences. Report actual results in [implementation status](#live-cdc-cutover).

Use synthetic data for mutation, replay and failure tests. The external source and the live legacy volumes can be inspected read-only while migration is pending.

## Implementation status

**Evidence date — 22 November 2026**

The repository implements the independent batch and live CDC paths described in the [architecture](../02-architecture/overview.md). The running installation publishes live CDC views from the dedicated ClickHouse database. The legacy generation and checkpoint are retained for rollback.

| Validation | Result |
|---|---|
| Host unit and static checks | 25 unit tests passed. Compose interpolation, environment modes, installed Python dependency consistency, syntax and dashboard datasource references were checked. |
| Airflow | The two supported batch DAGs parsed with zero import errors in the running Airflow image. |
| Source readiness | The configured SQL Server reader connected, reported snapshot isolation `ON` and database CDC enabled, and saw 15 capture instances. Agent and job-retention queries returned SQLSTATE `42000` for this reader and require DBA evidence. |
| Spark Batch | A synthetic staging run published through pinned Spark and the real PostgreSQL and ClickHouse JDBC libraries. It selected run 1 while run 2 existed, validated persisted results and skipped a repeated publication. A separate run published the all-empty fixture and verified its zero counts. |
| ClickHouse SQL | Synthetic batch and CDC publication assertions passed in ClickHouse 24.8. They cover no visibility before publication, source-run precedence, verified CDC batches, baseline filtering, logical replay, deletes and current state. |
| Spark CDC | The disposable Kafka → Spark → ClickHouse fixture passed. It injected a failure after the ClickHouse microbatch was sealed, resumed the same checkpoint, and produced five logical events from seven Kafka records with the expected delete, reinsert and offering update. Its Connect offset response was a fixture. |
| Grafana and reconciliation | Both datasources and dashboards provisioned in an isolated Grafana process. Its ClickHouse plugin queried the live CDC KPI definition using fixture data. The restricted reader could not select raw event images. The cross-database reconciliation query also executed. |
| Legacy baseline check | The existing connector and task were running. Kafka reported 15 partitions and 4,623,487 retained records from offset zero. Legacy batch and CDC snapshot counts matched for 600,000 applications, 59,279 offerings and 3,724,654 enrollments. The comparison covers counts at those historical boundaries. |
| Live CDC cutover | Spark replayed and verified all 4,623,487 retained records. After a ClickHouse restart during final validation, Spark resumed from the existing checkpoint and published the generation as `ACTIVE` using bounded ClickHouse query settings. |

A combined test attempt reached batch publication and then an OOM restart interrupted its empty-run check. ClickHouse recovered to healthy, and the empty case passed when tested alone. The deployment no longer uses the separate Spark batch Compose service. Airflow launches the batch job.

## Live CDC cutover

The local read-only check on 22 November 2026 found generation `students-cdc-v1` active. ClickHouse reported 4,623,487 of 4,623,487 verified bootstrap records and zero sampled Kafka offsets behind at the last sealed batch. Both Grafana datasource health endpoints returned HTTP 200.

| Shared-grain measure | Published batch run 2 | Active CDC |
|---|---:|---:|
| Applications | 600,000 | 600,000 |
| Course offerings | 59,279 | 59,279 |
| Enrollment attempts | 3,724,654 | 3,724,654 |

These are observations at the queried publication boundaries. The batch marker confirms a published run, and the current DAG handoff still requires the source-specific acceptance below.

## Live acceptance remaining

A fresh full extract from the external source through the new Airflow DAG chain and controlled source mutations through Debezium remain pending. The external source was accessed read-only. The existing connector and Kafka topics remain in use. The legacy warehouse objects, PostgreSQL CDC tables and Spark checkpoint are retained for rollback. Follow the [cutover runbook](../07-operations/runbook.md) before retiring that state.

The [21 November record](../assets/validation-2026-11-21.md) describes the earlier deployment.
