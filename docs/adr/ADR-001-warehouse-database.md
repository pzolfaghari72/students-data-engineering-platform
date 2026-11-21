# ADR-001: Analytical Warehouse Database

## Status

Accepted in implementation; live CDC warehouse flow not validated.

## Context

The project needs an analytical target for a multi-million-row enrollment
fact and Grafana query workload while retaining PostgreSQL for staging/control
state.

## Decision

Use ClickHouse as the BI warehouse. PostgreSQL remains the typed raw staging,
CDC history, metadata and Airflow metadata host.

## Alternatives considered

- Keep all analytical queries in PostgreSQL: simpler, but not the selected
  columnar analytical demonstration.
- Add another warehouse engine: increases service and memory cost without a
  project requirement.

## Consequences

ClickHouse MergeTree tables and snapshot views provide the current batch
model. JDBC writes are append-oriented; ClickHouse does not provide a
cross-system transaction with PostgreSQL. Reconciliation and deduplication
must be explicit.

## Implementation references

`db/warehouse_clickhouse/warehouse_ddl.sql`,
`spark/load_warehouse.py`, and
[ClickHouse fundamentals](../07-data-warehouse/clickhouse-fundamentals.md).
