# Snapshot consistency

Suppose a student and an enrollment are inserted between two table reads. Reads at different committed points can disagree about whether both exist. SQL Server's row-versioned `SNAPSHOT` transaction makes the extractor's count and fetch for each table use one consistent source view across all 15 tables.

The loader opens a single connection with autocommit off, sets `SNAPSHOT`, then reads the configured tables. SQL Server must allow snapshot isolation. `READ_COMMITTED_SNAPSHOT` alone does not supply the explicit transaction mode the loader requests. The DBA should consider version-store impact and the duration of a large full extract.

PostgreSQL chunk commits keep intermediate transactions practical. They create a different visibility boundary. Rows can remain after a late failure, so Airflow requires the exact extraction manifest and reconciled metadata before the run qualifies for Spark. A new extraction creates a new run.

Debezium also has an initial snapshot, but the connector coordinates it with its own persisted CDC position. It does not share the Airflow connection or its snapshot instant. This difference matters when comparing batch totals with current CDC views. [Capture and bootstrap](../05-streaming-and-cdc/sql-server-cdc.md) · [Reconciliation](../07-data-warehouse/data-publication-and-reconciliation.md)
