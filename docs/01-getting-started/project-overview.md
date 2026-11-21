# Project Overview

This is a local learning platform for assessing a SQL Server student database,
loading typed snapshots, transforming enrollment data, and serving analytical
views. It also contains a separate CDC capture path and an implemented but
not yet end-to-end-validated Spark streaming path.

## Current capability

| Capability | Repository evidence | Status boundary |
| --- | --- | --- |
| Source assessment | `db/source_assessment/` | Historical results are stored; SQL is run manually. |
| Batch ingestion | Airflow DAG and PostgreSQL `raw` schema | Code implements a serialized, single-SNAPSHOT-transaction load. A DBA must enable SQL Server snapshot isolation before execution. |
| Warehouse batch | `spark/load_warehouse.py` and ClickHouse snapshot tables/views | Implemented; live re-validation is reported separately. |
| CDC capture | Debezium registration helper, Kafka Connect, Kafka | Configured; source setup and topic lineage are blockers in the readiness report. |
| CDC persistence | Airflow consumer and PostgreSQL `history`/`scd` | Implemented separately; its DAG is paused. |
| Streaming | `spark/stream_cdc.py`, Compose profile, checkpoint volume | Code/configuration exist; it was not started or integration-tested. |
| Business analytics | Grafana dashboards backed by ClickHouse | Provisioned; live query validation is reported separately. Infrastructure monitoring is intentionally excluded. |

## Learning and use

Follow the [learning roadmap](../11-learning-path/learning-roadmap.md). Begin
with the [architecture](../02-architecture/architecture-overview.md), then
use the [quick start](./quick-start.md). Commands and current blockers are
owned by the [operations runbook](../10-operations/operational-runbook.md).

## Data safety

This repository includes a real student-data model. Do not copy real source
records, `.env`, or credentials into commits, issue reports, screenshots, or
documentation. ClickHouse intentionally excludes direct identifiers; the raw
staging database does not. Use only approved access and storage for source
data.
