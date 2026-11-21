# Volumes and Persistence

Named volumes preserve service state when containers are recreated. Bind
mounts expose repository configuration/code and should be treated as
authoritative source files.

| Volume / mount | Data | Lifecycle / caution |
| --- | --- | --- |
| `postgres-data` | Staging schemas/raw rows, metadata, Airflow metadata DB | Persistent source and control state. |
| `clickhouse-data` | Warehouse snapshots, CDC rows and ClickHouse users | Persistent analytical data. |
| `kafka-data` | KRaft metadata, Connect internal topics and source topics | Includes connector configuration and offsets; do not reset casually. |
| `spark-streaming-checkpoints` | Spark Structured Streaming checkpoint progress | Required for restart recovery; do not delete as troubleshooting. |
| `grafana-data` | Grafana database and local state | Provisioned dashboards/data sources remain in read-only bind mounts. |
| `./airflow/dags`, `./spark`, `./grafana/...` | Application code, dashboard JSON and config | Repository changes affect mounted services; several mounts are read-only. |

`docker compose down` stops/removes Compose containers and network but retains
named volumes. `docker compose down -v` removes named volumes and can destroy
the only local copies of databases/checkpoints. The latter is not a routine
recovery action. Backup procedures and the limits of the current backup design
are in [Backup and Recovery](../10-operations/backup-and-recovery.md).
