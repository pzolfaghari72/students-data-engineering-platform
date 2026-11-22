# SQL Server CDC

SQL Server CDC records changes from the transaction log into capture tables asynchronously. The capture instance identifies the source table's capture definition and its tracked schema. Commit and change log sequence numbers (LSNs) order source changes. A cleanup process advances the low-water mark of change history. A consumer behind that mark needs compatible recovery or a new baseline.

For this external SQL Server, an authorized DBA checks database and table CDC, all 15 capture instances, Agent and job status, cleanup retention, and reader permissions. The [read-only readiness SQL](../../db/source_assessment/sqlserver_cdc_prerequisites.sql) accepts schema and connector login parameters. The separately provided [enablement SQL](../../db/staging_postgres/enable_source_cdc.sql) is for a designated source and an authorized DBA.

Debezium bootstraps a source with an initial snapshot, then follows committed changes. The connector has its own offsets and isolation setting. New registration defaults to `snapshot` isolation via the project configuration. Existing connector settings are checked and preserved. SQL Server snapshot-isolation readiness and CDC capture readiness are separate checks.

Capture instances and their schema can change as a source evolves. Coordinate changes to primary keys, captured columns or incompatible types with the DBA and test the old-to-new boundary. Kafka retention cannot replace source change-table history that Debezium never captured.

[Microsoft SQL Server CDC](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server) · [Debezium settings](debezium.md) · [Observed source permissions](../12-implementation/implementation-status.md)
