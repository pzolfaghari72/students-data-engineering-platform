# Startup and Shutdown

The Makefile is the canonical command interface. The following are **startup** commands, not routine health checks; they can recreate or change one-shot services if repeated.

| Need | Entry point | Starts / changes |
| --- | --- | --- |
| Core databases / schemas | `make up-core` | PostgreSQL, ClickHouse and initialization steps |
| Batch DAG execution | `make up-batch` | Core + Airflow API/scheduler/DAG processor |
| Base batch + business dashboards | `make up-pipeline` / `make up` | Core + Airflow + Grafana; does not request Kafka/Connect |
| Business dashboards | `make dashboards-up` | Core + Grafana (no infrastructure monitoring stack) |
| New CDC setup | `make cdc-up` | Kafka/Connect and create-only connector registration attempt |
| New streaming setup | `make streaming-up` | Core, CDC registration, Spark CDC and Grafana |

**Already-running installation (2026-11-21):** Kafka, Connect and Spark CDC were running and the captured initial snapshot offsets later reached 100%. Do **not** rerun `make cdc-up` or `make streaming-up` merely to verify that state. Inspect `make status` and the read-only checks in [Streaming Execution](./streaming-execution.md). Keep the alternate Airflow CDC DAG paused while Spark owns CDC.

`make down` stops/removes containers and the network but retains named volumes; it **interrupts an active stream** and should be used deliberately. Never use `docker compose down -v` for normal cleanup. Persisted volumes are not backups.

See [Operational Runbook](./operational-runbook.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
