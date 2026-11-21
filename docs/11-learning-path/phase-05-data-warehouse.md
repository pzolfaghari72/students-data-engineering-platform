# Phase 5 — Data Warehouse

**Objectives:** reason about fact grain, dimension attributes, snapshot
publication, CDC projections and selected SCD2.

**Read:** [dimensional modeling](../07-data-warehouse/dimensional-modeling.md),
[table grains](../07-data-warehouse/fact-and-dimension-tables.md),
[SCD2](../07-data-warehouse/scd-type-2.md), and
[publication/reconciliation](../07-data-warehouse/data-publication-and-reconciliation.md).

**Inspect:** ClickHouse batch and CDC DDL, Spark joins, and Grafana SQL.

**Exercise:** write a read-only query that counts fact rows by term from the
published view, then explain why all physical snapshot rows must not be summed.
For CDC, inspect the query definition but expect no current runtime rows.

**Expected result:** identify one fact row per `CourseEnrollmentId`, one
snapshot ID per publication, and a separate `cdc_events` model.

**Validation:** counts and KPIs must use published views and their documented
grain; compare results only with matching run/snapshot metadata.
