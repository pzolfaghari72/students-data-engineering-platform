# Quick Start

This is a safe, incremental route through the platform. It avoids starting
every service at once.

| Step | Action | Expected evidence |
| --- | --- | --- |
| Prepare | Configure `.env` and run `make init`. | Environment validation and Compose parsing succeed. |
| Core | Run `make up-core`. | PostgreSQL and ClickHouse are healthy; staging and warehouse DDL are applied. |
| Batch ingestion | Start Airflow with `make up-batch`, then trigger `sqlserver_to_postgres_staging` only when SQL Server is authorized and `ALLOW_SNAPSHOT_ISOLATION` is enabled. | A successful pipeline run and all 15 snapshot batches in PostgreSQL metadata. |
| Batch warehouse | After verifying staging, manually trigger the separate `postgres_to_clickhouse_warehouse` DAG. | Spark exits successfully and a new row is present in `warehouse_model_loads_v2`. |
| BI dashboards | Run `make dashboards-up`. | Grafana starts with its ClickHouse business-KPI datasource; live query behavior still requires validation. |
| CDC | Follow the blockers in the [CDC readiness report](../13-validation/cdc-readiness-report.md) before registration or streaming. | Do not start a competing consumer or assume topic lineage. |

Commands are maintained in one place: the
[operational runbook](../10-operations/operational-runbook.md). The CDC and
streaming workflows are deliberately not part of this default quick start.
