# Batch Pipeline Diagrams

Two **manual Airflow DAGs**: a full SQL Server snapshot to PostgreSQL staging, followed by a controlled Spark warehouse publication.

## Batch flow

```mermaid
flowchart TD
    Trigger["Manual full snapshot DAG"]
    Start["Create pipeline run"]
    Tx["One SQL Server SNAPSHOT transaction"]
    Tables["Read 15 source tables"]
    Raw[("PostgreSQL raw batches and metadata")]
    Done{"All required batches successful?"}
    WarehouseTrigger["Manual warehouse DAG"]
    Runtime["Verify Java and Spark runtime (local patch)"]
    Select["Select latest successful full run"]
    Build["Build 9 dimensions and 3 facts"]
    Check{"Validate keys and row counts"}
    Snapshot[("ClickHouse dim_*_snapshots and fact_*_snapshots")]
    Publish["Insert warehouse_model_loads LAST"]
    Views["Published views for Grafana"]
    Trigger --> Start --> Tx --> Tables --> Raw --> Done
    Done -- No --> Stop["Stop and review failed run"]
    Done -- Yes --> WarehouseTrigger --> Runtime --> Select --> Build --> Check
    Check -- No --> Error["Fail without publication marker"]
    Check -- Yes --> Snapshot --> Publish --> Views
```

## Source-to-star-schema shape

```mermaid
flowchart LR
    Applicants --> FA["fact_application"]
    Applicants --> DA["dim_applicant"]
    Courses --> DC["dim_course"]
    Departments --> DD["dim_department"]
    CourseOfferings --> FO["fact_course_offering"]
    Students --> DS["dim_student"]
    CourseEnrollments --> FE["fact_course_enrollment"]
    Terms --> DT["dim_term"]
    Grades --> DG["dim_grade"]
    Degrees --> DDEG["dim_degree"]
    ClassStandings --> DCS["dim_class_standing"]
    Sessions --> DT
    FO --> FE
    DS --> FE
    DC --> FE
    DG --> FE
```

## Execution sequence

```mermaid
sequenceDiagram
    actor User
    participant AF as Airflow
    participant SQL as SQL Server
    participant PG as PostgreSQL
    participant S as Spark batch
    participant CH as ClickHouse
    User->>AF: Trigger sqlserver_to_postgres_staging
    AF->>PG: Create run metadata
    AF->>SQL: Open one SNAPSHOT transaction
    loop 15 source tables
        SQL-->>AF: Read bounded batches
        AF->>PG: Append raw rows and batch status
    end
    AF->>PG: Finalize full run status
    User->>AF: Trigger postgres_to_clickhouse_warehouse after validation
    AF->>AF: verify_spark_runtime (locally patched)
    AF->>S: load_clickhouse_warehouse
    S->>PG: Select successful full run and batches
    S->>CH: Check if source run already published (local patch)
    alt Already published
        S-->>AF: Skip redundant rebuild
    else New full source run
        S->>CH: Append snapshot rows by snapshot_id
        S->>CH: Insert warehouse_model_loads last
    end
```

The local guard does not make two concurrent publishers safe. The batch path is not incremental CDC-to-star-schema SCD2. Older reports of SQL Server snapshot isolation being off describe a **dated** readiness check; verify the source setting before a new full extraction.

See [Batch Architecture](../batch-architecture.md) and [Batch Execution](../../10-operations/batch-execution.md).
