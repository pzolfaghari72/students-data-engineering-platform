# System Context and Data Model Diagrams

These are the canonical system context, complete architecture, and model
diagrams. Solid batch edges describe the implemented workflow; dashed CDC
edges distinguish configured but unvalidated streaming behavior.

## System context

```mermaid
flowchart LR
    Owner["Source owner / DBA"]
    Engineer["Data engineer / learner"]
    Source[("SQL Server<br/>Students")]
    Platform["Students data platform<br/>Docker Compose"]
    BI["Analyst / learner"]
    Owner -->|"permissions, CDC and isolation"| Source
    Engineer -->|"configure and operate"| Platform
    Source -->|"snapshots / CDC log"| Platform
    Platform -->|"Business KPIs"| BI
```

## Complete architecture

```mermaid
flowchart LR
    SQL[(SQL Server Students)]
    subgraph Batch["Batch path"]
        AF["Airflow snapshot DAG"]
        Raw[("PostgreSQL raw + metadata")]
        SB["Spark batch"]
        CH[("ClickHouse snapshot warehouse")]
        AF --> Raw --> SB --> CH
    end
    subgraph CDC["CDC path"]
        Deb["Debezium SQL Server connector"]
        K[("Kafka")]
        ACDC["Airflow bounded CDC consumer<br/>currently paused"]
        PGH[("PostgreSQL history + SCD2")]
        SCDC["Spark Structured Streaming<br/>configured; not runtime-validated"]
        CDE[("ClickHouse CDC event log")]
        Deb --> K
        K --> ACDC --> PGH
        K -. "candidate exclusive consumer" .-> SCDC
        SCDC -. "configured sink" .-> PGH
        SCDC -. "configured sink" .-> CDE
    end
    SQL --> AF
    SQL --> Deb
    subgraph Analytics["Business analytics"]
        Graf["Grafana"]
        CH --> Graf
    end
```

## Implemented ClickHouse batch dimensional model

```mermaid
erDiagram
    DIM_APPLICANT ||--o{ FACT_APPLICATION : "applicant_key"
    DIM_DATE ||--o{ FACT_APPLICATION : "application_date_key"
    DIM_TERM ||--o{ FACT_APPLICATION : "entry_term_key"
    DIM_COURSE ||--o{ FACT_COURSE_OFFERING : "course_key"
    DIM_DEPARTMENT ||--o{ FACT_COURSE_OFFERING : "department_key"
    DIM_TERM ||--o{ FACT_COURSE_OFFERING : "term_key"
    DIM_STUDENT ||--o{ FACT_ENROLLMENT : "student_key"
    DIM_COURSE ||--o{ FACT_ENROLLMENT : "course_key"
    DIM_DEPARTMENT ||--o{ FACT_ENROLLMENT : "department_key"
    DIM_TERM ||--o{ FACT_ENROLLMENT : "term_key"
    DIM_GRADE ||--o{ FACT_ENROLLMENT : "grade_key"
    DIM_DEGREE ||--o{ FACT_ENROLLMENT : "degree_key"
    DIM_CLASS_STANDING ||--o{ FACT_ENROLLMENT : "class_standing_key"
    WAREHOUSE_MODEL_LOADS ||--o{ FACT_APPLICATION : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ FACT_COURSE_OFFERING : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ FACT_ENROLLMENT : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_APPLICANT : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_CLASS_STANDING : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_COURSE : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_DATE : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_DEGREE : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_DEPARTMENT : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_GRADE : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_STUDENT : "snapshot_id"
    WAREHOUSE_MODEL_LOADS ||--o{ DIM_TERM : "snapshot_id"
    FACT_APPLICATION {
        String applicant_key
        UInt32 application_date_key
        String entry_term_key
        UInt8 application_count
        Decimal gpa
    }
    FACT_COURSE_OFFERING {
        UInt32 course_offering_id
        String course_key
        String term_key
        UInt16 capacity
    }
    FACT_COURSE_ENROLLMENT {
        UInt32 course_enrollment_id
        String student_key
        UInt32 course_offering_id
        String course_key
        String grade_key
        String term_code
        Decimal credits
    }
    WAREHOUSE_MODEL_LOADS {
        String snapshot_id
        UInt64 source_run_id
        DateTime completed_at
    }
```

The event-history and PostgreSQL schemas are detailed in
[Data Model](../data-model.md); do not infer source relationships from this
analytical ER diagram.
