# Architecture Overview

The platform has a verified batch architecture and two alternative CDC
consumers. Airflow's existing CDC consumer persists event history and SCD2
versions in PostgreSQL. The independent Spark Structured Streaming code is
configured to persist Kafka events in PostgreSQL and ClickHouse, but its
end-to-end flow has not been validated. It must not be described as a
production-ready or exactly-once pipeline.

## Component responsibilities

| Component | Input | Responsibility | Output |
| --- | --- | --- | --- |
| SQL Server `Students` | Operational student, degree, course and enrollment tables | Source of truth; optionally SQL Server CDC capture | Source rows and CDC log |
| Airflow | Schedule/manual triggers and environment | Finite full-snapshot orchestration; optional bounded CDC consumer | PostgreSQL snapshot batches or CDC history |
| PostgreSQL | Airflow source reads / CDC records | Typed raw staging, run metadata, CDC event log, SCD2 staging and offsets | Durable staging and replay evidence |
| Debezium/Kafka Connect | SQL Server CDC | Read source changes and publish records | Kafka topics plus connector offsets |
| Kafka | Debezium events | Retain partitioned event log for consumers | Replayable event stream within configured retention |
| Spark batch | Successful PostgreSQL raw snapshot | Join and validate enrollment-grain warehouse frames | ClickHouse snapshot tables plus completion marker |
| Spark streaming | Kafka topics | Normalize events, write PostgreSQL history and ClickHouse CDC event rows | Streaming-generation event records |
| ClickHouse | Batch and configured CDC writes | BI-facing batch snapshots and CDC query-time current state | Analytics views |
| Grafana | ClickHouse warehouse | Student business KPI dashboards | Analytics views |

## Architectural boundary

The batch warehouse consumes `raw`, not CDC history. The Airflow CDC consumer
and Spark streaming service are separate consumers of the same event source;
they must not be run concurrently against the same records unless an explicit
ownership plan exists. Current runtime blockers and validation are in the
[CDC readiness report](../13-validation/cdc-readiness-report.md).

See the canonical [system diagrams](./diagrams/system-context.md), the
[batch design](./batch-architecture.md), the
[streaming design](./streaming-architecture.md), and the
[component decision records](../adr/index.md).
