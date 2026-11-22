# Reconciliation and publication boundaries

Run `make reconcile` after both data products publish. The [SQL query](../../db/warehouse_clickhouse/reconcile.sql) compares applications, course offerings and enrollment attempts at their shared business grains. It reports the batch source run and completion time, CDC generation and activation time, sampled progress when relevant, and signed differences as CDC minus batch. An empty result can indicate a missing publication.

| Boundary | Interpretation |
|---|---|
| Batch | Source rows seen in the snapshot transaction for one staged run, published after candidate warehouse validation |
| CDC | Current nondeleted source rows through sealed progress for the active generation |
| Exact business comparison | A known quiescent point at which the relevant source changes are covered by CDC and represented in batch |

For example, a batch snapshot has 100 offerings and the active CDC generation has 101. A source insert after the batch snapshot explains a +1. If the cut is expected to align, compare the source counts and per-table staging manifest, CDC progress and recent relevant operations, then verify the agreed business grain and filter scope. Equal totals establish count parity for the observed cuts. Row-level equality needs a separate comparison.

Reconciliation is read-only and outside either pipeline’s publication transaction. The batch marker selects a complete dimensional snapshot, and the CDC marker activates a verified generation whose later sealed updates become visible. Readers continue to see the preceding valid publication during a failed candidate. [Batch](batch.md) · [CDC](cdc.md) · [Validation evidence](../06-observability-and-quality/testing-and-validation.md)
