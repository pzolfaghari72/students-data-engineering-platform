# Phase 1 - Architecture

**Objectives:** distinguish full-snapshot batch analytics from continuous CDC event processing; identify which paths are operating and which integrations remain incomplete.

**Read:** [Architecture Overview](../02-architecture/architecture-overview.md), [Data Flow](../02-architecture/data-flow.md), and [System Context](../02-architecture/diagrams/system-context.md).

**Inspect:** `docker-compose.yml`, the three Airflow DAGs, `spark/load_warehouse.py`, `spark/stream_cdc.py`, and ClickHouse batch/CDC DDL.

**Exercise:** trace `CourseEnrollments` through the full staging batch and published ClickHouse `fact_course_enrollment` view. Separately trace a Kafka CDC change through Spark, PostgreSQL `history.cdc_events`, ClickHouse `cdc_events`, and the generation publication gate.

**Expected result:** explain that the batch star schema is not automatically updated by CDC; distinguish `warehouse_model_loads` and `warehouse_stream_generations` and the difference between PostgreSQL legacy `scd.entity_versions` and ClickHouse `cdc_student_scd2`.

**Validation:** on 2026-11-21 the stream was running and initial snapshot boundary consumption reached 100%, but `ACTIVE` publication and complete downstream source-change/replay validation were not yet evidenced. See [Implementation Status](../12-implementation/implementation-status.md).
