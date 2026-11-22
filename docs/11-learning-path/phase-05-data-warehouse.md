# Phase 5 - Data Warehouse

**Objective:** distinguish full snapshot publication, fact grain, SCD Type 2 validity, CDC event history and current-state projections.

**Read:** [Dimensional Modeling](../07-data-warehouse/dimensional-modeling.md), [Fact and Dimension Tables](../07-data-warehouse/fact-and-dimension-tables.md), [SCD Type 2](../07-data-warehouse/scd-type-2.md), and [Data Publication](../07-data-warehouse/data-publication-and-reconciliation.md).

**Inspect:** `db/warehouse_clickhouse/warehouse_ddl.sql`, `db/warehouse_clickhouse/cdc_warehouse_ddl.sql`, `spark/load_warehouse.py`, and Grafana's published ClickHouse views.

**Exercise:** query the published `fact_course_enrollment` count and locate the row in `warehouse_model_loads` that selects its `snapshot_id`. Explain why counting rows from all physical `_snapshots` generations would overcount. Inspect CDC generation markers and the SQL definition for `cdc_student_scd2`, without assuming it is active before verifying publication.

**Expected result:** identify fact grains by `ApplicantId`, `CourseOfferingId`, `CourseEnrollmentId`; distinguish source snapshots from row-level SCD2; and explain that **CDC does not currently update the batch star schema**.

**Validation:** historical published view counts exist, but incremental SCD2 for the analytical model is a future enhancement; a 15-minute Airflow schedule alone does not implement it.
