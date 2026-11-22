# Database models across the pipelines

The [SQL Server DDL and assessment](../../db/source_assessment) describe the upstream contract. The staging and batch diagrams below use the modeled tables and keys in this repository. The streaming diagram follows how ClickHouse publishes verified events.

## SQL Server source

![Source ERD with academic reference tables, students, course offerings, enrollments and applicants](../assets/diagrams/sqlserver-source-erd.png)

The assessed source includes composite course identity `(DepartmentCode, CourseNumber)`, offerings keyed by `CourseOfferingId`, and enrollment attempts keyed by `CourseEnrollmentId`. The [15-table extraction mapping](../../airflow/dags/students_staging/source_tables.py) fixes the application boundary and preserves source-key columns.

## PostgreSQL staging

```mermaid
erDiagram
    PIPELINE_RUNS ||--o{ EXTRACTION_BATCHES : tracks
    EXTRACTION_BATCHES |o--o{ TABLE_LOAD_STATUS : last_batch
    EXTRACTION_BATCHES ||--o{ RAW_COURSES : scopes
    PIPELINE_RUNS {
        bigint run_id PK
    }
    EXTRACTION_BATCHES {
        bigint batch_id PK
        bigint run_id FK
    }
    TABLE_LOAD_STATUS {
        varchar source_schema PK
        varchar source_table PK
        bigint last_batch_id FK
    }
    RAW_COURSES {
        varchar DepartmentCode PK
        integer CourseNumber PK
        bigint _batch_id PK, FK
    }
```

One metadata run tracks a separate extraction batch per table. Each raw table has a foreign key to its extraction batch and includes that batch in its primary key. The latest table status is keyed by source schema and table. See the [PostgreSQL DDL](../../db/staging_postgres/staging_ddl) for physical constraints and types.

## ClickHouse batch

```mermaid
erDiagram
    DIM_APPLICANT ||--o{ FACT_APPLICATION : applicant_key
    DIM_DATE ||--o{ FACT_APPLICATION : application_date_key
    DIM_TERM ||--o{ FACT_APPLICATION : entry_term_key
    DIM_COURSE ||--o{ FACT_COURSE_OFFERING : course_key
    DIM_DEPARTMENT ||--o{ FACT_COURSE_OFFERING : department_key
    DIM_TERM ||--o{ FACT_COURSE_OFFERING : term_key
    DIM_STUDENT ||--o{ FACT_COURSE_ENROLLMENT : student_key
    FACT_COURSE_OFFERING ||--o{ FACT_COURSE_ENROLLMENT : course_offering_id
    DIM_COURSE ||--o{ FACT_COURSE_ENROLLMENT : course_key
    DIM_DEPARTMENT ||--o{ FACT_COURSE_ENROLLMENT : department_key
    DIM_TERM ||--o{ FACT_COURSE_ENROLLMENT : term_key
    DIM_DEGREE ||--o{ DIM_STUDENT : degree_key
    DIM_CLASS_STANDING |o--o{ DIM_STUDENT : class_standing_key
    DIM_TERM ||--o{ DIM_STUDENT : enrollment_term_key
    DIM_GRADE |o--o{ FACT_COURSE_ENROLLMENT : grade_key
    DIM_DEGREE ||--o{ FACT_COURSE_ENROLLMENT : degree_key
    DIM_CLASS_STANDING |o--o{ FACT_COURSE_ENROLLMENT : class_standing_key
```

The diagram shows logical joins within one published snapshot. The application grain is one applicant record, the offering grain is one course offering, and the enrollment grain is one attempt. Spark checks keys and references before publishing. Grade and class standing can be absent. Spark pseudonymizes student and applicant keys and derives an unambiguous course key from the composite source key. ClickHouse physical tables repeat those grains by `snapshot_id`; the publication marker selects one validated source run for the views. The [fact and dimension catalog](../05-data-platform/warehouse.md#batch-dimensional-model) maps view names to tables. The [DDL](../../db/warehouse_clickhouse/warehouse_ddl.sql) defines the projections.

## ClickHouse CDC

```mermaid
flowchart TD
    G["cdc_generations · lineage and baseline bounds"]
    E["cdc_events · Kafka transport records"]
    B["cdc_batch_commits · verified write seals"]
    P["cdc_publications · active generation"]
    E --> R["cdc_committed_records"]
    B --> R
    R --> S["cdc_serving_records"]
    P --> A["cdc_active_generation"]
    A --> S
    S --> L["cdc_logical_events"]
    S --> C["cdc_current_state"]
    S --> K["cdc_business_kpis"]
    A --> K
    B --> H["cdc_pipeline_health"]
    G --> H
    P --> H
```

Transport records are identified within a generation by Kafka topic, partition and offset. Logical event identity includes the source table, canonical source key, operation and source version. The current-state grain is one latest nondeleted row per source table and key. The published KPI views aggregate those rows. See the [CDC warehouse design](../05-data-platform/warehouse.md#cdc-event-and-current-state-model) and [CDC DDL](../../db/warehouse_clickhouse/cdc_warehouse_ddl.sql).
