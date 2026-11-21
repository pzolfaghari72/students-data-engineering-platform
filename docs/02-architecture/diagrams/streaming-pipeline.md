# CDC and Streaming Diagrams

The following diagrams describe code/configuration paths and their validation
limits. The Airflow CDC consumer and Spark streaming consumer must not compete
for one logical sink/checkpoint.

## CDC pipeline

```mermaid
flowchart LR
    SQL[(SQL Server CDC)]
    Connect["Kafka Connect + Debezium"]
    K[("Kafka table topics")]
    Airflow["Airflow CDC consumer<br/>bounded, currently paused"]
    Spark["Spark Structured Streaming<br/>configured, not started"]
    PG[("PostgreSQL history<br/>offsets / generation metadata")]
    CH[("ClickHouse cdc_events<br/>append-only")]
    SQL --> Connect --> K
    K --> Airflow --> PG
    K -. "use instead of Airflow consumer" .-> Spark
    Spark -. "transactional history metadata" .-> PG
    Spark -. "append events; no cross-sink transaction" .-> CH
```

## CDC lifecycle

```mermaid
stateDiagram-v2
    [*] --> SourceReady: CDC enabled / login and Agent verified
    SourceReady --> ConnectorRegistered: create-only registration
    ConnectorRegistered --> Snapshotting: Debezium initial snapshot
    Snapshotting --> KafkaTail: snapshot complete; connector streams log
    KafkaTail --> ConsumerBootstrapping: capture per-partition Kafka end offsets
    ConsumerBootstrapping --> ActiveGeneration: consume through all captured ends
    ActiveGeneration --> KafkaTail: process later changes
    ConsumerBootstrapping --> Blocked: missing topics / offsets / checkpoint
    ConnectorRegistered --> Blocked: SQL permissions or capture job failure
```

## Debezium → Kafka → Spark sequence

```mermaid
sequenceDiagram
    participant SQL as SQL Server CDC
    participant D as Debezium Connect
    participant K as Kafka
    participant S as Spark Structured Streaming
    participant PG as PostgreSQL
    participant CH as ClickHouse
    D->>SQL: Read initial snapshot and CDC log
    D->>K: Write keyed JSON envelope to table topic
    S->>K: Read by subscribePattern and checkpoint offsets
    S->>S: Normalize operation, key, images, LSN and Kafka position
    S->>PG: Insert history and advance generation offsets
    S->>CH: Append CDC rows
    Note over PG,CH: No atomic transaction spans PostgreSQL and ClickHouse
    S->>CH: Publish generation after snapshot-boundary completion
```

## Streaming recovery sequence

```mermaid
sequenceDiagram
    participant K as Kafka
    participant S as Spark
    participant CP as Checkpoint volume
    participant PG as PostgreSQL
    participant CH as ClickHouse
    S->>K: Read next microbatch using checkpoint offsets
    S->>PG: Commit idempotent event rows and generation metadata
    S->>CH: Append batch events
    alt Both sinks return successfully
        S->>CP: Commit Spark batch progress
    else ClickHouse fails after PostgreSQL commit
        S->>K: Replay microbatch after retry/restart
        S->>PG: Kafka-position unique key avoids duplicate history
        S->>CH: Append may duplicate physical event rows
    end
```

The Spark app checks Debezium's exported snapshot-complete metric before
capturing Kafka end offsets. Per-partition Kafka offsets establish consumer
progress, not a single SQL Server transaction LSN across multiple topics.

## Student SCD Type 2 lifecycle

```mermaid
stateDiagram-v2
    [*] --> Current: c or snapshot r
    Current --> Current: unchanged u (no new version)
    Current --> Superseded: changed u
    Superseded --> Current: insert new attribute version
    Current --> Deleted: d (retain last attributes)
    Deleted --> Current: later c/r/u reactivates key
    Current --> Current: t (no SCD version)
```

The PostgreSQL consumer implements generic transitions for its selected
tables. The ClickHouse `cdc_student_scd2` view derives Student history from
the event log; the Spark consumer does not update PostgreSQL's SCD table.
