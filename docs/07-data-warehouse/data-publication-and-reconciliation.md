# Data Publication and Reconciliation

The batch warehouse and CDC event warehouse have **separate publication markers** and different freshness semantics.

## Batch model publication

1. Select latest successful SQL Server full staging run and all required table batches.
2. With Spark, transform nine dimensions and three facts; validate keys and grain preservation.
3. Append physical rows under a new UUID `snapshot_id` to `dim_*_snapshots` and `fact_*_snapshots`.
4. Append **`warehouse_model_loads`** last, including `source_run_id`, row counts and completion time.
5. Expose only the newest published `snapshot_id` through ClickHouse `dim_*` / `fact_*` views.

This avoids exposing partially written snapshots but leaves unpublished rows behind on a failed attempt. Do not count every physical snapshot row as a current KPI. The local safety patch skips an already published **full** source run, but is not a multi-process lock and is not an incremental checkpoint.

## Streaming generation publication

Spark Structured Streaming writes CDC events to PostgreSQL and ClickHouse, tracks Kafka partition offsets, and captures a Debezium snapshot boundary. The intended publication flow is: finish that boundary -> insert a ClickHouse `warehouse_stream_generations` record -> mark PostgreSQL generation active. `active_stream_generations` selects a published generation.

**Last observed checkpoint:** 2026-11-21, `snapshot_events_complete=t`, offsets 100%, **status still `BOOTSTRAPPING`**. The ClickHouse publication state was not shown after this check. Do not claim the generation active or the CDC projections fully reconciled until verified. This is different from the batch `warehouse_model_loads` marker.

## Reconciliation checklist

- Compare **the same source run / same Kafka generation** rather than raw counts at unrelated moments.
- For full loads, check staging successful batches against the published marker's row counts and views.
- For CDC, verify one active generation and Kafka position coverage per table, then check current-state / delete semantics.
- Test insert, update, delete and restart/replay behavior before claiming live correctness.
- Preserve historical source records, offsets, topic lineage, and Spark checkpoints. Never truncate or reset them as a quick fix.

See [CDC Design](./cdc-warehouse-design.md), [Checkpoint Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
