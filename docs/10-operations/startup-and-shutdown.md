# Startup and Shutdown

Start only the services required for the activity. The Makefile is the
authoritative command interface; details and CDC prerequisites are in the
[operational runbook](./operational-runbook.md).

| Need | Entry point | Starts |
| --- | --- | --- |
| Databases / schema | `make up-core` | PostgreSQL, ClickHouse and one-shot initializers |
| Batch DAG execution | `make up-batch` | Core plus Airflow API, scheduler and DAG processor |
| Base pipeline + business dashboards | `make up-pipeline` | Core, Airflow and Grafana; not Kafka/Connect |
| Business dashboards | `make dashboards-up` | Core plus Grafana; no infrastructure metrics stack |
| CDC | `make cdc-up` | Kafka and Connect, then attempts connector registration |
| Streaming | `make streaming-up` | Core, CDC registration, Spark streaming and Grafana dashboards; blocked until readiness gates pass |

Use `make status` to inspect services and `make down` to stop the project
without deleting named volumes. Do not use volume deletion as routine
shutdown. `make up` aliases `make up-pipeline`; it does not mean every
optional profile.
