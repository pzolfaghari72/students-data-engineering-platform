# CDC pipeline diagrams

```mermaid
flowchart LR
    S["SQL Server CDC capture instances"] --> C["Debezium task on Kafka Connect"]
    C --> K[("Kafka data topics")]
    C --> H[("Connect offsets and Debezium schema history")]
    K --> W["Spark Structured Streaming"]
    W <--> P[("Spark checkpoint volume")]
    W --> E[("ClickHouse events and verified seals")]
    E --> V["Active-generation current state and aggregates"]
    V --> G["Grafana CDC datasource"]
```

```mermaid
sequenceDiagram
    participant K as Kafka and Connect
    participant S as Spark
    participant C as ClickHouse CDC
    K->>S: Topic identity and snapshot completion evidence
    S->>C: Save generation and per-partition cut
    loop Retained input batches
        S->>C: Write normalized records
        S->>C: Compare Kafka positions and hashes
        S->>C: Seal verified microbatch
        Note over S,C: A replay verifies the existing seal
    end
    S->>C: Validate complete coverage and baseline
    S->>C: Publish the generation
    Note over S,C: Subsequent sealed changes feed the active views
```

The saved Kafka cut can include early live events. The baseline LSN and snapshot markers identify the completed snapshot cohort. [Bootstrap and event identity](../../05-streaming-and-cdc/event-processing.md)
