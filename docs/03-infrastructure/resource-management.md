# Resource Management

Compose limits selected high-footprint services, but not every service. A
16 GB Windows/WSL2 host must leave room for the host OS, Docker VM and SQL
Server outside this Compose project.

| Service group | Configured limit | Guidance |
| --- | --- | --- |
| Kafka | 1 GiB, 1 CPU; heap from `KAFKA_HEAP_OPTS` | Needed only for CDC/streaming. |
| Kafka Connect | 1.5 GiB, 1 CPU; heap from `CONNECT_HEAP_OPTS` | Needed only for Debezium. |
| Spark batch | 1.5 GiB, 1.5 CPU; 768 MiB driver, `local[2]` | One-shot; do not overlap loads. |
| Airflow scheduler batch task | 2 GiB, 2 CPU; local Spark uses a 768 MiB driver, `local[2]` | The scheduler runs the warehouse Spark task; do not run the standalone batch job at the same time. |
| Spark streaming | 1.5 GiB, 1.5 CPU; 768 MiB driver, `local[2]` | Long-running and competes with Kafka/Connect/DBs. |
| Grafana | 512 MiB, 1 CPU | Optional; serves ClickHouse-backed business dashboards only. |
| PostgreSQL / ClickHouse / Airflow API/DAG processor | No Compose memory limit defined | Observe actual host pressure; run only needed profiles. |

Preferred sequence: core databases → one batch job → stop/idle unused batch
services. Start Grafana only when the KPI dashboard is needed. For larger full snapshots,
watch Docker/WSL memory and SQL Server pressure; more parallelism is not
automatically faster or safer. Do not raise limits until measuring a real
bottleneck.

These are configured caps rather than measured resource guarantees. See
[Compose](./docker-compose.md) for the service inventory and
[operations](../10-operations/operational-runbook.md) for safe status checks.
