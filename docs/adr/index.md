# Architecture Decision Record Index

Decisions describe the project's intended architecture. Status statements below reflect evidence through **2026-11-21**, not live production certification. Older dated validation reports remain historical.

| ADR | Decision | Status |
| --- | --- | --- |
| [001](./ADR-001-warehouse-database.md) | ClickHouse analytical warehouse | Accepted; batch publication populated; CDC active generation still under verification |
| [002](./ADR-002-dimensional-modeling.md) | Enrollment-centered dimensional model | Implemented as complete published snapshots |
| [003](./ADR-003-etl-strategy.md) | PostgreSQL staging, Airflow orchestration, Spark transforms | Implemented for manual full snapshots |
| [004](./ADR-004-docker-compose.md) | Local Docker Compose deployment | Accepted |
| [005](./ADR-005-source-connectivity.md) | Environment-driven SQL Server ODBC access | Implemented, source-side preconditions checked per new run |
| [006](./ADR-006-cdc-and-debezium.md) | SQL Server CDC via Debezium and Kafka | Source-topic streaming observed; snapshot boundary consumed |
| [007](./ADR-007-streaming-processing.md) | Independent Spark Structured Streaming | Running; final generation activation / retry verification pending |
| [008](./ADR-008-clickhouse-current-state.md) | Batch snapshot publication plus separate CDC query-time projections | Batch populated; CDC event store populated, projections need publication and change tests |

A future **incremental CDC-to-analytical-star-schema SCD Type 2** loader is not implied by the accepted decisions; that integration is not implemented. See [Architecture Overview](../02-architecture/architecture-overview.md), [SCD Type 2](../07-data-warehouse/scd-type-2.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
