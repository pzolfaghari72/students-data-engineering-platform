# Snapshot Consistency

Independent table extracts at different times can combine incompatible
source states. The current batch loader addresses this in code by using one
SQL Server `SNAPSHOT` isolation transaction for all 15 tables.

## What the code does

`load_consistent_snapshot()` opens one `pyodbc` connection with autocommit
disabled, executes `SET TRANSACTION ISOLATION LEVEL SNAPSHOT`, reads each
table sequentially on that connection, then commits once all are complete.
PostgreSQL batch commits are independent; only the SQL Server read transaction
provides the shared source version.

SQL Server must have `ALLOW_SNAPSHOT_ISOLATION` enabled for the `Students`
database. The setting is currently reported OFF. This must be changed and
verified by an authorized DBA before the new extraction path is run. Long
snapshot transactions retain row versions and can increase `tempdb` pressure;
coordinate with the DBA and schedule carefully.
Read-committed snapshot isolation (RCSI) alone is not equivalent: RCSI gives a
statement-level view, while this multi-table extract needs one transaction
snapshot. The first source read establishes the transaction's view.

## Distinguish the Debezium snapshot

Debezium's `snapshot.mode=initial` bootstraps the connector's captured tables
and then transitions into CDC. It does not share the Airflow transaction,
does not populate PostgreSQL `raw`, and is not by itself a cross-path
point-in-time guarantee. Spark's configured boundary logic observes a
Debezium completion metric and records Kafka end offsets; Kafka offsets are
consumer positions, not SQL Server transaction isolation boundaries.

No source-to-CDC reconciliation or common LSN handoff is verified yet. The
existing PostgreSQL event history and offset records do not match topics in
the currently observed Kafka cluster. See the
[readiness report](../13-validation/cdc-readiness-report.md).
