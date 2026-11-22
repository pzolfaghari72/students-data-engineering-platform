# Validation Update - 2026-11-21

**Scope:** Read-only local checks and subsequent user-supplied terminal outputs on 2026-11-21, plus a locally applied warehouse code safety patch. This page records evidence as of the supplied outputs, not a live measurement. It describes the legacy deployment. See [implementation status](../12-implementation/implementation-status.md) for the current code and later test evidence.

## Combined health report and stream progress

A local `check_students_platform.py` report recorded at 2026-11-21 18:23:57 (local project timezone) showed:

| Check | Observed result | Limit |
| --- | --- | --- |
| Docker | PostgreSQL, ClickHouse, Kafka, Connect, Airflow, Grafana and `spark-cdc-stream` running; designated health checks passing | Report describes one point in time |
| Airflow DAG parsing | Three DAGs, zero **local** import errors; Airflow CDC DAG paused | Execution of new warehouse task not established |
| Spark stream | `students-cdc` `BOOTSTRAPPING`; boundary captured `t`; complete `f`; 839,094 / 4,623,487 consumed (**18.15%**) | Before later completion check |
| PostgreSQL / ClickHouse CDC | PG total **878,745**, CH total **839,094** | Counts at different instants cannot prove missing records |
| Main table mismatch | `CourseOfferings`: PG 50,112; CH 10,461 in report (other 14 matched at that instant) | PG already held offsets ahead of active Spark stream; generation/lineage must be interpreted |
| ClickHouse event identities | No duplicate `(generation_id, topic, partition, offset)` identities detected | Only for the data queried then; future replay may differ |
| ClickHouse published batch views | Enrollments 3,724,654; offerings 59,279; applications 600,000; applicants 600,000 | Batch snapshot, not live CDC-backed facts |
| Grafana ClickHouse datasource | Working / OK | Not proof of every visual calculation |
| Spark logs | No obvious errors in the last 120 log lines | Not a complete log review |

## Subsequent snapshot-boundary milestone

Later the same date, the user queried PostgreSQL `metadata.streaming_generations` and obtained:

```text
query_name   status         complete   progress_percent
students-cdc BOOTSTRAPPING  t          100
```

This establishes that **all captured initial-snapshot Kafka end offsets were consumed** and `snapshot_events_complete=t` at that time. It does **not** establish PostgreSQL status `ACTIVE` or a ClickHouse row in `warehouse_stream_generations` / `active_stream_generations`; the next verification result was not supplied. The original 18.15% report is a historical earlier measurement, not a contradiction.

## Warehouse code safety patch and runtime check

The user ran `apply_warehouse_safety.py`, which reported updated local files:

- `airflow/dags/postgres_to_clickhouse_warehouse.py`: added `verify_spark_runtime` before `load_clickhouse_warehouse` and kept `schedule=None`.
- `spark/load_warehouse.py`: added a guard to skip an already published **full** staging `source_run_id`.

Afterwards, `airflow dags list-import-errors --local` returned `No data found`. In `airflow-scheduler`, `/usr/bin/java` reported **OpenJDK 17.0.21**, `spark-submit` reported **Spark 3.5.6**, and the warehouse loader/key files were present.

**Not yet proven:** a successful post-patch Airflow warehouse task, an implemented CDC-to-analytical-star-schema incremental SCD2 loader, active CDC generation publication, complete source insert/update/delete behavior, or replay correctness after partial dual-sink writes. The local patch might not be committed to the GitHub repository.

## Recommended next verification (read only)

```bash
docker compose exec -T postgres sh -c '
psql -U "$POSTGRES_USER" -d students_staging -P pager=off -c "
SELECT query_name, status, snapshot_boundary_captured,
       snapshot_events_complete, last_event_at
FROM metadata.streaming_generations;
"'

docker compose exec -T clickhouse sh -c '
clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --database students_warehouse --query "
SELECT
  (SELECT count() FROM warehouse_stream_generations) AS published_generations,
  (SELECT count() FROM active_stream_generations) AS active_generations
"'
```

Avoid running the legacy Airflow CDC consumer alongside Spark, re-registering the connector, launching an overlapping warehouse publisher, or clearing Kafka/Spark checkpoint/database data just to make historical reports appear consistent.
