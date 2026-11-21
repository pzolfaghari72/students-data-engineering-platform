# CDC and Streaming Diagrams

The current active CDC consumer design is Spark Structured Streaming. The older Airflow CDC DAG is **paused** and should not be enabled as a second owner of the same source/sinks.

## Flow

```mermaid
flowchart LR
    SQL[(SQL Server CDC)] --> DBZ["Debezium in Kafka Connect"]
    DBZ --> K[(Kafka table topics)]
    K --> Spark["Spark Structured Streaming / checkpoint"]
    Spark --> PG[(PostgreSQL history and streaming_generations)]
    Spark --> CH[(ClickHouse cdc_events)]
    Spark --> Marker["Publish streaming generation when gated"]
    Airflow["Alternative Airflow CDC DAG: paused"] -.-> PG
    K -.-> Airflow
```

The batch `fact_course_enrollment` and `dim_student` views are **not** updated by this CDC event path.

## Bootstrap state machine

```mermaid
stateDiagram-v2
    [*] --> Snapshotting: Debezium initial source snapshot
    Snapshotting --> KafkaTail: Source snapshot complete
    KafkaTail --> Bootstrapping: Capture Kafka topic boundary offsets
    Bootstrapping --> BoundaryConsumed: Spark has consumed all captured offsets
    BoundaryConsumed --> Published: Write ClickHouse generation marker
    Published --> Active: Mark PostgreSQL generation active
    Active --> Active: Consume subsequent changes
    Bootstrapping --> NeedsReview: Missing topic / offset / failed batch
    BoundaryConsumed --> NeedsReview: Publication not confirmed
```

**2026-11-21 observation:** PostgreSQL reported `snapshot_events_complete=t` and 100% consumed offsets, but the same query still showed `BOOTSTRAPPING`. That proves the **BoundaryConsumed** step, not necessarily **Published** or **Active**. The final ClickHouse generation marker and PostgreSQL `ACTIVE` state were not supplied.

## Spark microbatch and recovery

```mermaid
sequenceDiagram
    participant K as Kafka
    participant S as Spark
    participant PG as PostgreSQL
    participant CH as ClickHouse
    participant CP as Checkpoint volume
    K-->>S: Messages with topic / partition / offset
    S->>PG: Insert unique CDC positions, update generation offsets
    S->>CH: Append cdc_events
    alt Both sink writes succeed
        S->>CP: Commit Spark microbatch progress
    else Failure after PG commit
        S->>K: Replay from checkpoint on retry
        S->>PG: Unique position prevents duplicate PG insert
        S->>CH: Replay may duplicate physical rows
    end
```

## Student SCD Type 2 projection

```mermaid
stateDiagram-v2
    [*] --> Current: Initial read or create
    Current --> Current: Update with no tracked changes
    Current --> Superseded: Changed attributes
    Superseded --> Current: New version starts
    Current --> Deleted: Delete event
    Deleted --> Current: Subsequent create / update
```

`cdc_student_scd2` is a **ClickHouse query-time projection** over a published generation; it is not a Spark materialization into the batch `dim_student` dimension. The legacy PostgreSQL `scd.entity_versions` path is maintained by the paused Airflow consumer, not by the running Spark stream.

See [Streaming Architecture](../streaming-architecture.md) and [Streaming Execution](../../10-operations/streaming-execution.md).
