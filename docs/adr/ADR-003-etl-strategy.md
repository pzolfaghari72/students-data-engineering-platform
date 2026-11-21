# ADR-003: Batch Ingestion and Transformation

## Status

Accepted; Airflow orchestrates the snapshot and Spark warehouse task.

## Context

The project needs finite source extraction, durable staging, a reproducible
transform, and a warehouse publication boundary without a persistent Spark
cluster or access from Airflow to the host Docker daemon.

## Decision

Use Airflow for finite SQL Server → PostgreSQL snapshots, then run the
existing Spark batch code in local mode from an Airflow scheduler task.
Publish ClickHouse snapshot rows for nine dimensions and three facts. Keep
the one-shot Compose Spark service as a manual alternative.

## Alternatives considered

- Direct SQL Server → Spark → ClickHouse: skips auditable typed staging and
  run metadata.
- A Spark cluster or new orchestration service: adds infrastructure not
  needed for this local learning project.
- Mounting the Docker socket into Airflow: grants broad host Docker control.
- Airflow as an indefinite stream runtime: unsuitable for the implemented
  unbounded consumer lifecycle.

## Consequences

The Airflow image includes Java 17 and PySpark 3.5.6. Spark runs with
`local[2]` and a bounded driver heap in the scheduler container. Avoid
concurrent batch/streaming Spark workloads. Snapshot consistency still
depends on SQL Server `SNAPSHOT` isolation being enabled. The model-load row
is written last to publish a generation.

## Implementation references

`airflow/dags/sqlserver_to_postgres_staging.py`,
`airflow/plugins/Dockerfile`, `docker-compose.yml`, `spark/load_warehouse.py`,
and [Batch Architecture](../02-architecture/batch-architecture.md).
