# Phase 6 — Business Analytics

**Objectives:** interpret the warehouse-backed student KPIs and understand
what refreshing a dashboard does and does not do.

**Read:** [Grafana](../08-analytics-and-monitoring/grafana.md),
and [KPI definitions](../08-analytics-and-monitoring/business-kpis.md).

**Inspect:** the student dashboard JSON, ClickHouse datasource provisioning,
and the SQL views used by its panels.

**Exercise:** run `make dashboards-up` when ClickHouse is available. Check the
dashboard datasource and panel queries. Do not treat a configured panel as a
successful query. The current Compose file does not assign Grafana a profile;
the Make target selects it explicitly.

**Expected result:** explain each KPI's source, grain and known limitations;
distinguish dashboard refreshes from pipeline ingestion.

**Validation:** confirm actual panel query status in Grafana; see the runtime
validation report for the latest recorded evidence.
