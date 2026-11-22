# Docker Compose

Compose declares services, startup dependencies, network identities and persistent storage in one file. A profile makes a service opt-in. A dependency can wait for a process, a health check, or successful completion of a one-shot job. A healthy server still needs the right schema or application publication before readers have usable data.

| Service or group | Mode | Responsibility |
|---|---|---|
| `postgres`, `postgres-init`, `staging-schema-init` | Core/batch | Local staging and Airflow metadata, role/database setup, and typed staging tables |
| `clickhouse`, `clickhouse-init` | Shared | Separate batch/CDC databases, DDL, writer and dashboard permissions |
| `airflow-init`, `airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor` | Batch | Metadata migration, UI/API, execution and DAG parsing |
| `kafka`, `connect`, `debezium-register` | `cdc` profile | Retained broker log, Connect worker, create-or-check SQL Server connector |
| `spark-cdc-stream` | `streaming` profile | One checkpointed Spark stream |
| `grafana` | Shared | Provisioned ClickHouse dashboards and datasources |

`make up` selects the batch and dashboard path. `make cdc-up` prepares Kafka, Connect and registration. `make streaming-up` starts streaming with both profiles and Grafana. Spark Batch runs from the Airflow warehouse task. The earlier separate `spark-warehouse` service was retired.

Use `docker compose config --quiet` or `make compose-config` to validate interpolation. Use `make status` to inspect the live project. One-shot initializer exit code zero is a successful terminal state. Inspect one-shot logs before repeating a failed initialization. [Compose profile behavior](https://docs.docker.com/compose/how-tos/profiles/) · [Deploy diagram](../02-architecture/diagrams/deployment-architecture.md) · [Starting and stopping](../10-operations/startup-and-shutdown.md)
