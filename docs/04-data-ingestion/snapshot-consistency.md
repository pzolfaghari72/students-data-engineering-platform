# Snapshot Consistency

Independent table extracts at different times can produce mutually inconsistent source states. The full SQL Server staging path addresses this by reading the 15 assessed tables through a **single SQL Server SNAPSHOT isolation transaction**.

## Batch extractor behavior

`load_consistent_snapshot()` opens one ODBC connection with autocommit disabled, sets `TRANSACTION ISOLATION LEVEL SNAPSHOT`, reads the assessed tables sequentially on the same connection, then finishes the source transaction. PostgreSQL row-batch writes and metadata commits are separate; the **SQL Server read transaction** provides a common source view.

The source database must have `ALLOW_SNAPSHOT_ISOLATION=ON` for this approach. A dated **2026-11-16** readiness report recorded it `OFF`. Later successful full staging and warehouse data exists, but the newer checks did not explicitly requery this SQL Server setting. Confirm the setting with the source DBA before another full extraction instead of asserting its current value from the old report. Read-committed snapshot isolation alone gives statement-level views, not the same multi-table transaction snapshot.

A long full-table snapshot may retain row versions and increase source `tempdb` load; coordinate such work with the SQL Server owner. Do not launch repeated full loads just to refresh a Grafana dashboard.

## Separate Debezium initial snapshot

Debezium `snapshot.mode=initial` is a **different** snapshot boundary. It writes Kafka topic events and then reads CDC changes; it does not share the Airflow transaction or populate PostgreSQL `raw`. Spark's snapshot-completion gate captures Kafka end offsets across topics. Kafka offsets prove **consumer positions**, not a single SQL Server transaction LSN across every topic.

The 2026-11-21 user query reported `snapshot_events_complete=t` and 100% of captured offsets, while the generation status remained `BOOTSTRAPPING`. That resolves the old statement that there were no source topics at that time, but it does **not** establish final streaming publication or a globally consistent handoff between batch and CDC analytical models.

See [CDC Validation Update](../13-validation/validation-update-2026-11-21.md), [Batch Execution](../10-operations/batch-execution.md), and [Streaming Execution](../10-operations/streaming-execution.md).
