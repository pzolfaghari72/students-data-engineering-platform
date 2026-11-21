# ADR-006: SQL Server CDC with Debezium and Kafka

## Status

Accepted in configuration; source connector bootstrap is blocked.

## Context

The project needs source change events and replayable transport separate from
full batch extraction.

## Decision

Use SQL Server CDC, the Debezium SQL Server connector in Kafka Connect, and
Kafka table topics. Keep a PostgreSQL event log/offset store for durable
audit/recovery. Connector registration is explicit and create-only.

## Alternatives considered

- Poll source tables by timestamp/identity: not equivalent to log-based CDC
  and can miss deletes.
- PostgreSQL polling instead of Kafka consumption: would not demonstrate
  genuine Kafka streaming.

## Consequences

SQL Server Agent, CDC capture/cleanup jobs, permissions, retention, Kafka
topic lineage and connector offsets become operational prerequisites. A
Debezium RUNNING state does not establish snapshot completion. Existing
PostgreSQL event positions must be reconciled before a fresh connector.

## Implementation references

`db/staging_postgres/enable_source_cdc.sql`,
`debezium/register_connector.py`, `docker-compose.yml`, and
[CDC readiness](../13-validation/cdc-readiness-report.md).
