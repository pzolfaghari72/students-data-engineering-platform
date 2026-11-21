# ADR-008: ClickHouse Snapshot and CDC Current-State Semantics

## Status

**Batch warehouse publication implemented and populated.** The independent CDC event store is populated, but published/active stream generation and current-state / Student SCD2 projection correctness still require final confirmation as of the latest supplied 2026-11-21 checks.

## Context

The batch warehouse must avoid exposing incomplete full-snapshot generations. The CDC event model must support source updates and deletes without assuming transactional in-place ClickHouse mutations.

## Decision

For **batch**, append each validated attempt under `snapshot_id` to current `dim_*_snapshots` and `fact_*_snapshots` physical tables, then insert **`warehouse_model_loads`** last. The published business views filter to the latest completed snapshot. Obsolete documentation names such as `warehouse_model_loads_v2` and `*_v2_snapshots` are not the current schema. A locally applied 2026-11-21 guard skips an already published full source run, but does not create an incremental loader.

For **CDC**, append event records to `cdc_events` and use generation-gated query-time current-state / Student SCD2 views ordered by source version and Kafka position. This model remains separate from batch analytical facts.

## Alternatives considered

- In-place batch row mutation: not the selected safe full-snapshot publication approach.
- `ReplacingMergeTree` alone: background merges are asynchronous and are not an immediate query-time deduplication guarantee.
- Joining CDC directly into batch views without a controlled handoff: deferred until watermark, generation, delete, replay and SCD2 semantics are designed and tested.

## Consequences

Unpublished physical snapshot rows can remain after failed writes. Published batch views remain stable until a completion marker is written. Physical CDC event rows may duplicate after cross-sink retries. Counting all historical physical rows is not equivalent to querying the latest business view. The 2026-11-21 CDC snapshot boundary was consumed, but stream generation activation was not yet proven in the supplied output.

## Implementation references

`db/warehouse_clickhouse/warehouse_ddl.sql`, `db/warehouse_clickhouse/cdc_warehouse_ddl.sql`, `spark/load_warehouse.py`, `spark/stream_cdc.py`, [Publication and Reconciliation](../07-data-warehouse/data-publication-and-reconciliation.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
