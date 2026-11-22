# ADR-003 — CDC for freshness and batch for independent snapshots

**Status:** Accepted

Operate two independent paths. Debezium and Spark Structured Streaming serve low-latency CDC analytics. Airflow and Spark Batch support full extraction, rebuilding, bulk analytical work and reconciliation. Run batch on demand by default. The agreed batch cadence may be configured through the environment.

PostgreSQL stages a consistent SQL Server snapshot per extraction run. Airflow hands off the exact ID. ClickHouse exposes a batch attempt only after read-back validation and publication. CDC owns its own models and recovery path. Compare published counts without coupling either pipeline to the comparison.
