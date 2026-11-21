# Phase 1 — Architecture

**Objectives:** distinguish finite batch from continuous streaming, locate
each durable state boundary, and identify what is configured versus validated.

**Read:** [architecture overview](../02-architecture/architecture-overview.md),
[data flow](../02-architecture/data-flow.md), and the
[canonical diagrams](../02-architecture/diagrams/system-context.md).

**Inspect:** `docker-compose.yml`, both Airflow DAG files,
`spark/load_warehouse.py`, `spark/stream_cdc.py`, and the two ClickHouse DDLs.

**Exercise:** trace `CourseEnrollments` from SQL Server through the current
batch warehouse; then trace a hypothetical update through Debezium, Kafka and
the two alternative consumers. Mark each unvalidated edge.

**Expected result:** explain that raw snapshots and CDC history are separate,
that the batch fact publishes by completed snapshot ID, and that no
end-to-end CDC result is currently verified.

**Validation:** compare your map with the batch and streaming diagrams and
the [implementation status](../12-implementation/implementation-status.md).
