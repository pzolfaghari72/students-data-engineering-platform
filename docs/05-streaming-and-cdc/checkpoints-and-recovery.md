# Checkpoints and Recovery

Spark checkpoints persist query progress and state on
`spark-streaming-checkpoints`, mounted at the configured checkpoint path.
They are not interchangeable with PostgreSQL's saved consumer offsets or
Debezium Connect's source offsets.

## Recovery behavior

- Spark `startingOffsets=earliest` is used only when there is no usable Spark
  checkpoint. The app checks Kafka beginning offsets before a fresh start and
  refuses a fresh checkpoint if retained offsets are already nonzero.
- `failOnDataLoss=true` fails when Kafka data required by checkpoint progress
  is unavailable.
- PostgreSQL history is unique on `(kafka_topic, kafka_partition,
  kafka_offset)` and generation offset updates are committed with metadata.
- ClickHouse writes use append-only `MergeTree`; a retry after PostgreSQL
  commits but before ClickHouse succeeds may append duplicate physical rows.
- A Spark batch is checkpointed only after `foreachBatch` returns. This does
  not make the PostgreSQL and ClickHouse writes one transaction.

Do not delete the checkpoint, manually advance offsets, or shorten Kafka
retention to “unstick” a consumer. First establish which consumer owns the
stream, compare Kafka partitions/retention with PostgreSQL and checkpoint
state, then follow an approved replay/reconciliation plan. Current state
requires lineage reconciliation before bootstrap; see the
[readiness report](../13-validation/cdc-readiness-report.md).
