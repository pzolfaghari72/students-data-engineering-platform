# Streaming Execution

The Spark CDC consumer runs as an **independent long-running service** and persists a checkpoint. It reads Kafka Debezium topics and writes PostgreSQL CDC history/generation metadata plus ClickHouse CDC events. It does not build the current analytical star schema.

## Status / operating decision (2026-11-21)

- Kafka, Connect, PostgreSQL, ClickHouse and Spark CDC were running in the combined local health report.
- Spark had processed source events from all 15 Debezium topics with increasing offsets and no obvious recent errors.
- The subsequent PostgreSQL metadata query showed **100%** of the initial snapshot boundary consumed and `snapshot_events_complete=t`.
- The same row still showed **`status=BOOTSTRAPPING`**. Final ClickHouse generation publication and PostgreSQL `ACTIVE` transition have **not** been shown in the supplied output.
- The legacy Airflow CDC DAG is **paused**; do not enable it alongside Spark.

The [November 16 CDC readiness report](../13-validation/cdc-readiness-report.md) documents earlier blockers when no connector/source topics were present. Those observations are historical and should not be applied as a command to stop or re-register the currently running stream.

## Safe read-only completion check

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

Even if both publication checks succeed, test actual source insert/update/delete processing and Kafka-position reconciliation before declaring complete CDC correctness. Comparing raw event counts at different moments is not sufficient; source and warehouse CDC can have different historical generations.

## Recovery rules

- Preserve the Spark checkpoint volume, Kafka source/connector offsets, PostgreSQL history and ClickHouse event rows.
- Never run Airflow CDC concurrently with Spark for the same intended stream.
- If Spark stops, inspect logs and compare Kafka beginning/end offsets to saved checkpoint/PG generation state before making a change.
- Do not rerun `make streaming-up`, register the connector, or reset anything simply because a status remains `BOOTSTRAPPING` after boundary consumption.
- Physical ClickHouse CDC rows are append-only and may contain replay duplicates after partial writes; verify unique event identities and the query projections.

See [Checkpoint Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md), [SCD Type 2](../07-data-warehouse/scd-type-2.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
