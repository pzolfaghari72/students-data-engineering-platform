# Publication and reconciliation

Publication is the point at which a validated data product becomes eligible for readers. It provides a stable boundary when processing touches multiple tables or two systems with independent commits.

| Batch | CDC |
|---|---|
| Write candidate dimensions and facts under a fresh `snapshot_id`. Validate rows, exact distinct keys and selected totals in ClickHouse. Append `warehouse_model_loads` last. Serving ranks run ID, completion time and snapshot ID. | Write, verify and seal individual Spark microbatches. Publish the generation after validating coverage through the saved Kafka cut and the completed source baseline. Serving sees subsequent seals in the active generation. |

A prior batch publication stays visible when a newer candidate fails. A previous CDC generation can remain active while a new one bootstraps. Neither marker implies a distributed transaction with its source.

Run `make reconcile` with both models published. The [comparison query](../../db/warehouse_clickhouse/reconcile.sql) returns batch run, candidate timing, CDC generation, activation timing, sampled progress and counts for applications, course offerings and enrollments. The signed delta is CDC minus batch. The CDC health columns correspond to the latest observed generation when it matches the active aggregate. An empty result can mean one side has no publication yet.

For example, a batch snapshot contains 100 offerings and the active generation contains 101. If SQL Server added one offering after the batch input boundary, +1 is expected. If both represent a known quiescent point and the declared grains match, compare batch source counts and CDC changes for the affected table at those boundaries. Matching totals are useful evidence of parity, not a proof that row values or keys match. [Batch execution](../10-operations/batch-execution.md) · [Business KPI scopes](../08-analytics-and-monitoring/business-kpis.md)
