# Operational Runbook

Run commands from the repository root with a configured local `.env`. This page is the canonical command index. Do not use setup commands as an alternative to checking a running stream.

## Make targets

| Command | Effect | Caution |
| --- | --- | --- |
| `make init` | Validate local setup, preserve/create `.env`, generate required missing secrets, prepare dirs, parse Compose | Do not display secret values |
| `make compose-config` | Validate Compose | No containers started |
| `make up-core` | PostgreSQL, ClickHouse and schema initializers | Avoid unnecessary restarts during active CDC |
| `make up-batch` | Core and Airflow services | Does not execute source DAG automatically |
| `make up-pipeline` / `make up` | Core, Airflow and Grafana | Does not imply Kafka/Connect/Spark CDC are started |
| `make dashboards-up` | Core and Grafana | Business dashboard only |
| `make cdc-up` | Kafka/Connect plus one-shot connector registration | Use only for deliberate setup, not a health check |
| `make streaming-up` | Core, CDC setup, Spark stream and Grafana | Do not rerun over an already-working checkpointed consumer |
| `make warehouse-load` | Separate one-shot full Spark publisher | Alternative to Airflow warehouse DAG; never overlap |
| `make staging-init`, `make warehouse-init`, `make cdc-warehouse-init` | Apply DDL/scripts | Review changes and database state before rerunning |
| `make status` | Show Compose services | Read-only |
| `make logs` | Follow container logs | May contain identifying/source information |
| `make test` | Unit tests | Local code test, not full integration |
| `make down` | Stop/remove containers and network, preserve named volumes | Disrupts a running stream; only stop deliberately |

## Airflow operation

- Full snapshot `sqlserver_to_postgres_staging`: manual.
- Warehouse `postgres_to_clickhouse_warehouse`: manual; locally patched preflight `verify_spark_runtime` plus `load_clickhouse_warehouse`, if those uncommitted files are present.
- Legacy Kafka CDC `sqlserver_debezium_cdc_to_staging`: **paused while Spark CDC owns the source stream**.

## Read-only diagnostics

```bash
make status
docker compose --profile '*' ps -a
# If available in the local working tree:
python3 check_students_platform.py
```

Airflow UI: `http://localhost:18080`; Grafana: `http://localhost:13000`. For the initial CDC boundary and current publication state, follow [Streaming Execution](./streaming-execution.md); for full source/warehouse operations, see [Batch Execution](./batch-execution.md).

## Safety, semantics and validation

Do not reset/delete Kafka topics, connector offsets or Spark checkpoints in ordinary troubleshooting. Physical ClickHouse CDC appends are not exactly-once across retries. The current Spark event path does not update batch warehouse star-schema dimensions/facts or implement materialized incremental SCD2. The final CDC generation activation remains to be verified even after the last 100% boundary report.

Keep `.env` private. Persisted named volumes are **not backups**. The historical [2026-11-16 readiness report](../13-validation/cdc-readiness-report.md) and newer [2026-11-21 status](../13-validation/validation-update-2026-11-21.md) must be interpreted with their evidence dates.
