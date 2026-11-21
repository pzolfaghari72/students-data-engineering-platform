# Idempotency and Retries

There is no single atomic transaction spanning SQL Server, Kafka, PostgreSQL, ClickHouse and Spark checkpoints. The protection depends on each path.

| Path | Protection | Remaining risk |
| --- | --- | --- |
| Full staging snapshot | Rows are tagged with batch IDs and metadata; failed batches are excluded from published full-run selection | Failed table extracts may leave partial committed chunks; a new full run adds new batches |
| Legacy Airflow CDC -> PostgreSQL | Unique Kafka topic/partition/offset; insert and offset commit share a PostgreSQL transaction | Topic/offset lineage must be preserved; keep this consumer paused while Spark owns CDC |
| Spark CDC -> PostgreSQL | Unique Kafka event identities and generation metadata | Failure after PostgreSQL commit can cause replay into ClickHouse |
| Spark CDC -> ClickHouse | Events carry generation and Kafka positions; views select latest state | Append-only `MergeTree` does not physically enforce position uniqueness; retries can duplicate rows |
| Spark batch -> ClickHouse | Validations before writes; new `snapshot_id`; `warehouse_model_loads` last | Failed attempts can leave unpublished rows; concurrent publishers are not transactionally serialized |
| Local full-run publication guard | Check existing `warehouse_model_loads.source_run_id` and skip if published | Protects common sequential reruns; not an atomic lock, not incremental CDC/SCD2 |

A successful microbatch does not prove two-sink exactly-once behavior. Do not reset Kafka or Spark offsets to bypass a failure. Inspect event identities and generation metadata before recovery; see [Checkpoint Recovery](../05-streaming-and-cdc/checkpoints-and-recovery.md).
