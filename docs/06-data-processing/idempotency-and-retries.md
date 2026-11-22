# Idempotency and retries

An idempotent retry has the same published effect as one successful execution. Physical writes may still repeat, so reliable retry rules must name the input identity and visibility boundary.

| Operation | Identity | Retry rule |
|---|---|---|
| Full source extraction | Airflow DAG run and allocated staging `run_id` | Start a new full run after a failed extraction. Existing batch records prevent re-extraction into the same run. |
| Warehouse publication | Explicit `source_run_id` and per-attempt `snapshot_id` | The supported DAG serializes warehouse runs. A published source run is skipped. An unpublished partial attempt stays out of serving views. |
| Streaming transport | Generation plus topic, partition and offset | ClickHouse verifies stored positions and payload fingerprints. Existing seals are validated on replay. |
| Logical change | Generation, table, key, operation and source version | Query-time grouping collapses repeated source events. Current state selects the most recent qualifying version. |

For example, a sink failure after writing facts but before adding `warehouse_model_loads` leaves candidate rows in the physical tables. Retry the same successful staging run and validate the replacement's publication. For streaming preserve the paired checkpoint even if a previous callback wrote the last seal. [Batch recovery](../10-operations/batch-execution.md) · [CDC replay](../05-streaming-and-cdc/checkpoints-and-recovery.md)
