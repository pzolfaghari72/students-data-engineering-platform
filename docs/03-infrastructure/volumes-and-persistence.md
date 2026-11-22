# Volumes and persistence

Containers can be replaced while named volumes retain their data. A bind mount uses the host file as its source of truth. The checked-in Compose definitions bind application and provisioning files, and in several cases mount them read-only.

| Name or location | State | Recovery scope |
|---|---|---|
| `postgres-data` | Raw staging, pipeline metadata and separate Airflow metadata database | Preserve the database identities, runs and referenced raw batches. |
| `clickhouse-data` | Batch candidate rows and manifests, CDC data and seals, users and roles | Preserve publication metadata with the corresponding physical data. |
| `kafka-data` | KRaft metadata, data topics, Connect storage and Debezium schema history | Preserve topic IDs, positions and connector lineage. |
| `spark-streaming-checkpoints` | Query identity, offsets, commits and single-writer lock path | Preserve the checkpoint with the compatible generation, broker and sink. |
| `grafana-data` | Grafana application state | Preserve alongside versioned dashboard JSON and datasource provisioning. |
| `airflow/logs` and repository bind mounts | Task logs, Airflow DAGs, Spark apps and provisioned configuration | Check ownership and ensure the container sees the intended repository revision. |

Changing the Compose project name can point at different named volumes. `make down` preserves named volumes. Volume persistence is only local to the Docker environment. For host loss and upgrade rehearsals follow [Backup and recovery](../10-operations/backup-and-recovery.md).
