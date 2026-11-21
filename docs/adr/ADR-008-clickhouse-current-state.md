# ADR-008: ClickHouse Snapshot and Current-State Semantics

## Status

Batch publication is implemented for the v2 dimensional model. CDC current
state remains a separate, unvalidated projection.

## Context

The batch warehouse must not expose partially written generations; CDC
updates and deletes need append-compatible handling in ClickHouse.

## Decision

For batch, append each attempt under a new `snapshot_id` to versioned v2
tables. Validate the nine dimensions and three fact row counts, then write
`warehouse_model_loads_v2` last. Current-facing model views select the latest
completed v2 generation. Keep the original physical warehouse tables and
`warehouse_loads` marker for rollback.

For CDC, keep the append-only event table and derive current state at query
time with source-version metadata, filtering deleted keys.

## Alternatives considered

- In-place row mutation: not the snapshot publication model.
- `ReplacingMergeTree` alone: background merges are asynchronous and do not
  guarantee immediate deduplication in every query.
- Merging batch and CDC writes into one current table: deferred until
  bootstrap/reconciliation and retry semantics are validated.

## Consequences

Batch views do not switch until all rows have been written and the publication
marker is inserted. Abandoned physical rows remain; there is no cleanup
process. CDC rows can duplicate physically on replay, so query logic must
account for source ordering and deletion markers.

## Implementation references

`db/warehouse_clickhouse/warehouse_ddl.sql`,
`db/warehouse_clickhouse/cdc_warehouse_ddl.sql`, `spark/load_warehouse.py`,
`spark/stream_cdc.py`, and
[publication/reconciliation](../07-data-warehouse/data-publication-and-reconciliation.md).
