# Backup and recovery

Docker volumes make container replacement routine on one host. Keep separate, restorable backups for a host-loss scenario or an upgrade that changes published data.

| Boundary | Preserve together | Verify after an isolated restore |
|---|---|---|
| Staging and Airflow | PostgreSQL databases, role/credential plan, stage runs and referenced batches | An approved run still resolves all batch metadata and rows; Airflow metadata can be read with compatible configuration. |
| Batch warehouse | ClickHouse physical candidate tables, views and publication markers | Latest publication points to consistent data. Validate counts and distinct fact keys. |
| CDC | Kafka data and internal topics, Connect offsets and schema history, Spark checkpoint and ClickHouse generation/seals/data | Query ID, topic IDs, offsets and sink coverage agree. Use controlled CDC replay fixtures before promoting. |
| Grafana | Application state, repo provisioning and compatible credentials | Stable datasource UIDs, health and dashboard queries resolve. |

Capture the active generation and latest verified offsets before backup or migration. A restored sink behind Spark may require a compatible older checkpoint and retained input. Do not remove offsets or repoint topics to bypass that safeguard. If retained history cannot support compatible recovery, plan a fresh baseline with explicit lineage. [Volume map](../03-infrastructure/volumes-and-persistence.md) · [Recovery checks](streaming-execution.md)
