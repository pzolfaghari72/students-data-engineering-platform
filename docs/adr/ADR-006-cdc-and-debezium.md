# ADR-006: SQL Server CDC with Debezium and Kafka

## Status

**Accepted and exercised in the 2026-11-21 local training environment.** The initial Kafka snapshot boundary reached 100% consumption, but the active streaming generation and subsequent change/replay correctness still require verification. Earlier source/bootstrap blockers in the [2026-11-16 report](../13-validation/cdc-readiness-report.md) were **historical**.

## Context

The project needs source change events and replayable transport distinct from its manual full-batch extraction.

## Decision

Use SQL Server CDC, a Debezium SQL Server connector in Kafka Connect, and Kafka table topics. Store CDC event history and consumer generation metadata in PostgreSQL. Connector registration is explicit and create-only so that an existing connector's offsets/configuration are not inadvertently replaced.

## Alternatives considered

- Source-table timestamp or identity polling: not equivalent to log-based CDC and can miss deletes.
- Polling PostgreSQL instead of Kafka: would not demonstrate genuine source CDC / Kafka streaming.

## Consequences

SQL Server Agent, CDC capture/cleanup jobs, permissions, Kafka retention, topic lineage and Debezium offsets are operational dependencies. Connector REST `RUNNING` alone does not prove snapshot completion. On 2026-11-21 the running Spark consumer had consumed the captured snapshot offsets, but PostgreSQL still reported `BOOTSTRAPPING` in the supplied completion query; publication activation must be verified separately. Keep the alternative Airflow CDC DAG paused while Spark owns the stream.

## Implementation references

`db/staging_postgres/enable_source_cdc.sql`, `debezium/register_connector.py`, `docker-compose.yml`, [Streaming Execution](../10-operations/streaming-execution.md) and [Validation Update](../13-validation/validation-update-2026-11-21.md).
