# Backup and Recovery

The repository defines persistence volumes and some replay metadata, but it
does not configure an automated backup, restore test, or disaster-recovery
workflow.

| Data | Current persistence | Recovery boundary |
| --- | --- | --- |
| PostgreSQL raw, metadata, history and SCD2 | `postgres-data` volume | Volume persistence is not a backup. A database export/restore procedure must be separately tested. |
| ClickHouse warehouse and CDC tables | `clickhouse-data` volume | No scheduled backup/export or restore validation is configured. |
| Kafka and Connect offsets/topics | `kafka-data` volume | Losing this volume can destroy connector lineage; do not reset it to repair consumer state. |
| Spark progress | `spark-streaming-checkpoints` volume | Keep with the corresponding query and Kafka lineage. Deleting it changes replay behavior. |
| Grafana | `grafana-data` volume plus provisioned files | Dashboard JSON/data-source definitions are source-controlled; local UI state is persistent. |

Before any upgrade, host cleanup or recovery experiment, capture a backup
using an approved database-specific method and verify a restore in an isolated
environment. No untested backup command is supplied as a recovery guarantee.
Avoid destructive Compose volume operations; see
[Volumes and Persistence](../03-infrastructure/volumes-and-persistence.md).

CDC recovery requires preserving Kafka topics, connector offsets, PostgreSQL
event history and Spark checkpoints as a compatible set. Current mismatch
between PostgreSQL's saved history and the current Kafka source topics is
documented in the [readiness report](../13-validation/cdc-readiness-report.md).
