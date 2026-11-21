# Idempotency and Retries

Idempotency differs by path and sink. There is no single transaction covering
SQL Server, Kafka, PostgreSQL, ClickHouse and Spark checkpoints.

| Path | Retry protection | Remaining risk |
| --- | --- | --- |
| Raw batch | Batch IDs scope rows; inserts are append-only. | A failed table may already have committed chunks; rerunning the whole DAG appends another full snapshot. |
| Airflow CDC → PostgreSQL | Unique Kafka topic/partition/offset; event insertion and next-offset updates share a PostgreSQL transaction. | Kafka topic lineage/retention must still match saved offsets. |
| Spark CDC → PostgreSQL | Same unique event identity; PostgreSQL generation offsets are updated transactionally. | PostgreSQL commit can succeed while ClickHouse fails. |
| Spark CDC → ClickHouse | Event rows carry generation and Kafka position; views use source ordering for current state. | Physical `MergeTree` appends are not unique; retries may produce duplicate rows. |
| Spark batch → ClickHouse | New snapshot UUID per attempt; all dimensions and facts are validated before writes; `warehouse_model_loads_v2` is written last; views select only completed model snapshots. | Partial rows remain physically and there is no retention cleanup. Retries can leave additional unpublished rows. |

Never claim exactly-once end-to-end delivery. For a retry, first identify the
owning consumer, compare source topic positions with checkpoints and PostgreSQL
metadata, and preserve the original event lineage. Operational rules are in
[recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).
