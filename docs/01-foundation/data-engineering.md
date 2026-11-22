# Data engineering through the student platform

The platform answers questions about enrollment, offered capacity, admissions and grades from an operational SQL Server database. Its analytical layer makes grains, filters and publication times explicit so a reader can distinguish a course from an offering, or an enrollment attempt from a distinct student.

| Concept | What it means in this project |
|---|---|
| Batch processing | Airflow extracts a consistent, bounded input. Spark transforms it into a validated star schema. Whole runs support repeatable historical analysis and recovery. |
| Stream processing | Spark Structured Streaming runs microbatches over an open-ended Kafka log. Current counts update incrementally, with a saved baseline and offset checkpoints for recovery. |
| ETL and ELT | Extraction uses SQL and Python, followed by a typed raw load into PostgreSQL. Spark performs dimensional transformation before the final warehouse load. Staging lets a retry reuse the same source input. |
| Change data capture | SQL Server exposes committed changes, including deletes. Debezium converts them to events and Kafka retains them for consumers and replay. |
| Data warehousing | Facts define what gets counted. Dimensions supply business context such as department or term. ClickHouse’s columnar storage supports scans and aggregations over the published models. |
| Lineage and idempotency | A published batch points to a staging run. A CDC publication points to a generation and verified offsets. Retrying verifies the same inputs and preserves publication boundaries. |
| Reconciliation and observability | Count comparisons use the same business grain across both paths. Grafana reports metrics and progress, while Airflow, Connect, Spark and database logs show where processing stopped. |

Python implements the staging loader, connector bootstrap, Spark applications and tests. SQL defines the source projections, database constraints, analytical views and reconciliation. Docker packages runtimes, Compose defines the local topology, and Linux or WSL2 supplies the host environment. The [service map](../03-infrastructure/services.md) shows how those components communicate.

## A practical reading path

1. Trace one offering and its enrollment attempts in the [database models](../02-architecture/data-model.md).
2. Follow a staging run ID in the [batch guide](../04-data-pipelines/batch.md). Explain what Grafana sees if the final publication insert fails.
3. Predict the result of the example update, replay and delete in the [CDC guide](../04-data-pipelines/cdc.md).
4. Compare current source students with distinct students in the selected enrollments in the [Grafana KPI guide](../06-observability-and-quality/grafana.md).
5. Follow [setup](../03-infrastructure/setup.md) and [testing](../06-observability-and-quality/testing-and-validation.md) to reproduce a validation case.
