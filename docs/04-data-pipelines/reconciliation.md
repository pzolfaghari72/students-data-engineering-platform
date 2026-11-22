# Reconciliation and publication boundaries

Run `make reconcile` after both products publish. The query compares applications, course offerings and enrollment attempts at a shared business grain. It reports the Batch staging run and snapshot times, the active CDC generation and sampled progress, and signed differences as **CDC minus Batch**. An empty result can indicate a missing publication. [Reconciliation SQL](../../db/warehouse_clickhouse/reconcile.sql)

| Boundary | Interpretation |
|---|---|
| Batch | Source rows seen in the snapshot transaction for the selected staging run |
| CDC | Current nondeleted rows through sealed progress for the active generation |
| Comparable cut | A known quiescent point at which the relevant source changes are covered by both products |

If Batch contains 100 offerings and CDC contains 101, an insert after the Batch snapshot can explain the difference. Check the Batch snapshot start, CDC progress and recent operations before treating a difference as a defect. Equal counts establish parity at the observed grain. Row-level equality requires a separate check.

Reconciliation reads published views. Batch and CDC maintain independent publication and recovery boundaries. [Batch pipeline](batch.md) · [CDC pipeline](cdc.md)
