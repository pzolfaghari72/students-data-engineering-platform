# Batch Pipeline Diagrams

Canonical flow for the SQL Server snapshot and subsequent Spark warehouse
publication. The snapshot and warehouse publication are separate, manually
triggered Airflow DAGs.

## Batch pipeline

```mermaid
flowchart TD
    Trigger["Manual snapshot DAG trigger"]
    Start["Create metadata.pipeline_runs row"]
    Tx["One ODBC connection<br/>autocommit disabled<br/>SNAPSHOT isolation"]
    Tables["Sequentially read 15 source tables"]
    Raw[("PostgreSQL raw<br/>typed rows + batch IDs")]
    Finish["Finish task<br/>SUCCESS / PARTIAL / FAILED"]
    WarehouseTrigger["Manual warehouse DAG trigger<br/>after staging verification"]
    Select["Spark selects latest successful full run"]
    Build["Build 9 dimensions and 3 facts"]
    Check{"Dimension keys unique and each<br/>fact count matches its source?"}
    Snapshot[("Versioned ClickHouse v2 tables")]
    Publish["Insert warehouse_model_loads_v2 last"]
    Views["Model views expose completed snapshot"]
    Trigger --> Start --> Tx --> Tables --> Raw
    Tables --> Finish
    Raw --> WarehouseTrigger --> Select --> Build --> Check
    Check -- "no" --> Stop["Fail before publication"]
    Check -- "yes" --> Snapshot --> Publish --> Views
```

## Source-to-warehouse map

```mermaid
flowchart LR
    A["Applicants"] --> FA[("FactApplication")]
    A --> DA[("DimApplicant")]
    T["Terms + Sessions"] --> DT[("DimTerm")]
    A --> DD[("DimDate")]
    CO["CourseOfferings"] --> FO[("FactCourseOffering")]
    C["Courses"] --> DC[("DimCourse")]
    DEP["Departments"] --> DDEP[("DimDepartment")]
    CE["CourseEnrollments"] --> FE[("FactEnrollment")]
    S["Students"] --> DS[("DimStudent")]
    G["Grades"] --> DG[("DimGrade")]
    CS["ClassStandings"] --> DCS[("DimClassStanding")]
    DEG["Degrees + Types + Levels"] --> DDEG[("DimDegree")]
    T --> FO
    DC --> FO
    DDEP --> FO
    DT --> FA
    DT --> FE
    DC --> FE
    DDEP --> FE
    DS --> FE
    DG --> FE
    DCS --> FE
    DDEG --> FE
```

## Batch execution sequence

```mermaid
sequenceDiagram
    actor User
    participant AF as Airflow
    participant SQL as SQL Server
    participant PG as PostgreSQL
    participant Spark as Local Spark in Airflow
    participant CH as ClickHouse
    User->>AF: Trigger sqlserver_to_postgres_staging
    AF->>PG: Create run metadata
    AF->>SQL: Open one SNAPSHOT transaction
    loop 15 tables, sequential
        AF->>SQL: SELECT source table
        SQL-->>AF: Bounded row batches
        AF->>PG: Append typed rows and batch status
    end
    AF->>SQL: Commit source transaction
    AF->>PG: Finalize run status
    Note over User,AF: Operator verifies staging before the next manual trigger
    User->>AF: Trigger postgres_to_clickhouse_warehouse
    AF->>Spark: Run warehouse task (no Docker socket)
    Spark->>PG: Select successful run and required batch IDs
    Spark->>PG: Read 13 model input tables
    Spark->>CH: Append 9 dimensions and 3 facts by snapshot_id
    Spark->>CH: Insert warehouse_model_loads_v2 last
```

The implementation requires SQL Server `ALLOW_SNAPSHOT_ISOLATION=ON`; the
current readiness report records it as OFF. See
[snapshot consistency](../../04-data-ingestion/snapshot-consistency.md).
