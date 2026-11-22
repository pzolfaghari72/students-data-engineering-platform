# Batch pipeline diagram

This sequence describes the current code. The [Airflow captures](../../14-screenshots/README.md#airflow-batch-orchestration) record earlier graph revisions.

```mermaid
sequenceDiagram
    actor O as Operator or schedule
    participant A as Airflow staging DAG
    participant S as SQL Server
    participant P as PostgreSQL
    participant W as Airflow warehouse DAG with Spark
    participant C as ClickHouse Batch
    participant G as Grafana
    O->>A: Trigger full extraction
    A->>P: Create staging run
    A->>S: Open one SNAPSHOT transaction
    loop 15 source tables
        A->>S: Count and fetch source rows
        A->>P: Write typed batches and reconcile counts
    end
    A->>P: Finalize the exact run
    A->>W: Trigger with source_run_id
    W->>P: Resolve run and read its raw batches
    W->>C: Write dimensions and facts under candidate snapshot_id
    W->>C: Read back counts, keys and selected totals
    W->>C: Append publication marker
    G->>C: Read published views
```

Staging persists diagnostic rows on failure. A failed extraction never reaches the Spark publication step. A failed candidate write can leave data behind while the serving views continue to resolve the most recent valid publication. [Batch reliability](../../06-data-processing/idempotency-and-retries.md)
