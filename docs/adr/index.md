# Architecture Decision Record Index

These records document choices evidenced by the repository. They distinguish
the currently implemented choice from its validation state; they are not
claims that every integration is production-ready.

| ADR | Decision | Status |
| --- | --- | --- |
| [001](./ADR-001-warehouse-database.md) | ClickHouse as analytical warehouse | Accepted in implementation |
| [002](./ADR-002-dimensional-modeling.md) | Enrollment-centered dimensional model | Accepted in implementation |
| [003](./ADR-003-etl-strategy.md) | PostgreSQL staging, Airflow orchestration, Spark transform | Accepted in implementation |
| [004](./ADR-004-docker-compose.md) | Docker Compose for local learning deployment | Accepted |
| [005](./ADR-005-source-connectivity.md) | SQL Server ODBC and environment-driven connection | Accepted in implementation |
| [006](./ADR-006-cdc-and-debezium.md) | SQL Server CDC via Debezium and Kafka | Accepted in configuration; source bootstrap blocked |
| [007](./ADR-007-streaming-processing.md) | Independent Spark Structured Streaming consumer | Implemented in code; not validated |
| [008](./ADR-008-clickhouse-current-state.md) | Batch snapshot publication and CDC query-time state | Batch accepted; CDC projection unvalidated |

Each record uses the same context/decision/alternatives/consequences/reference
format. See the [architecture overview](../02-architecture/architecture-overview.md)
for the system context.
