# CDC warehouse design

The CDC database is a separate ClickHouse publication product. Its append-oriented tables preserve verified transport history and let readers reconstruct the active generation's current state.

| Layer | Objects and grain |
|---|---|
| Lineage | `cdc_generations` records the generation, Kafka lineage fingerprint, baseline evidence and saved end offsets. |
| Transport | `cdc_events` records normalized before/after images at Kafka positions, scoped to a generation and Spark batch. |
| Seal | `cdc_batch_commits` stores checkpoint identity, record digest, offsets, record count and verification time. |
| Publication | `cdc_publications` records baseline LSN and activation time. `cdc_active_generation` resolves the newest publication. |
| Visible records | `cdc_committed_records` limits data to sealed batches. `cdc_serving_records` filters to the active lineage and baseline. |
| Analytical interfaces | `cdc_logical_events` deduplicates logical changes; `cdc_current_state` returns surviving latest rows. `cdc_business_kpis` and `cdc_pipeline_health` are the approved Grafana interfaces. |

Current-state ordering uses `argMax` with the source version and transport values as tie-breakers. The current row disappears when the winning operation is delete. Earlier incomplete snapshot cohorts and tombstones stay outside the serving view. The logical event view preserves qualifying deletes and groups re-emitted changes.

MergeTree suits this workload because immutable transport rows and explicit checkpoints make failure behavior inspectable. The latest-image and metric projections run at query time. They depend on working generation filtering and enough local I/O and memory for grouping. The writer and dashboard roles bound threads and can spill to disk. [CDC DDL](../../db/warehouse_clickhouse/cdc_warehouse_ddl.sql) · [Event identity](../05-streaming-and-cdc/event-processing.md) · [Current-state decision](../adr/ADR-008-clickhouse-current-state.md)
