# Operational Runbook

Run commands from the repository root with a valid local `.env`. This page is
the canonical command reference; component pages link here rather than
repeating commands.

## Make targets

| Command | Effect | Prerequisite / caution |
| --- | --- | --- |
| `make init` | Validate local setup, preserve/create `.env`, generate missing secrets, prepare directories, parse Compose. | Does not start data pipelines. |
| `make install` | Install `requirements.txt` into the selected host Python. | Use an isolated virtual environment. |
| `make compose-config` | Validate rendered Compose configuration. | Does not start containers. |
| `make up-core` | Start PostgreSQL/ClickHouse and initialize roles/schemas. | Retains existing volume data. |
| `make up-batch` | Start core plus Airflow batch services. | Source extraction needs approved SQL Server access. |
| `make up-pipeline` / `make up` | Start core, Airflow and Grafana business dashboards. | Does not start Kafka/Connect despite the broad alias name. |
| `make dashboards-up` | Start core plus Grafana's ClickHouse-backed KPI dashboard. | Does not start infrastructure monitoring. |
| `make cdc-up` | Start Kafka/Connect then invoke create-only connector registration. | **Blocked by current readiness findings; do not run yet.** |
| `make streaming-up` | Start core, invoke CDC registration, then start the Spark stream and Grafana dashboards. | **Blocked; do not run until streaming gates pass.** |
| `make warehouse-load` | Apply warehouse DDL and run one Spark batch in the standalone Compose job. | Optional manual alternative; do not overlap with the Airflow warehouse task. |
| `make staging-init`, `make warehouse-init`, `make cdc-warehouse-init` | Apply idempotent schema scripts. | Schema changes need review; DDL does not migrate every existing table. |
| `make status` | Show Compose service state. | Safe read-only inspection. |
| `make logs` | Follow recent Compose logs. | Avoid sharing logs containing sensitive source details. |
| `make test` | Run unit tests. | No database/service required. |
| `make down` | Stop/remove Compose containers/network. | Named volumes are retained. |

## Diagnostics

Use `docker compose ps`, service-specific `docker compose logs --tail=100
<service>`, and `docker stats --no-stream`. For Kafka/Connect details, check
the [CDC operations page](./streaming-execution.md); for data inspection use
the [staging](../04-data-ingestion/postgresql-staging.md) and
[warehouse](../07-data-warehouse/fact-and-dimension-tables.md) guides.

## Safe operating rules

- Never reset/delete Kafka topics, connector offsets or Spark checkpoints as
  routine troubleshooting.
- Never infer Debezium snapshot completion from REST `RUNNING` alone.
- Do not run Airflow CDC and Spark streaming as competing consumers for the
  same planned sink.
- Do not treat `.env.example` as runtime configuration or disclose `.env`.
- `docker compose down` preserves named volumes; volume deletion is
  destructive and is not documented as a normal stop procedure.
