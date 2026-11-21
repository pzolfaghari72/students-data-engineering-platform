# Grafana

Grafana is used for business KPIs, not as an ingestion engine or an infrastructure monitoring stack. Its `students-warehouse` ClickHouse datasource queries the published **batch** dimension/fact views. Dashboard refreshes do not cause new SQL Server extracts or Spark jobs.

## Current dashboard

| Property | Observed configuration / result |
| --- | --- |
| Dashboard | **Student Enrollment, Capacity & Grades** |
| Dashboard UID | `students-enrollment-kpis` |
| Data source | ClickHouse, UID `students-warehouse` |
| Panels | **18 business panels**, organized into Overview (6), Enrollment demand (2), Capacity/admissions (3), Department detail (1), and Grade analytics (6) |
| Data source test | Working / healthy in the 2026-11-21 local report |
| Host URL | `http://localhost:13000` |

Overview metrics are enrollment records, distinct enrolled students, course offerings, offered seats, enrollment/capacity ratio, and applications. Additional panels show enrollment by term/department, capacity by department, application status and term, department comparison, grade coverage/distribution and weighted grade points.

The dashboard was adjusted locally to show full integer counts with thousands separators rather than compact `K`/`M` units. A newer saved/provisioned JSON should be checked before describing that formatting as part of the GitHub default branch. Screenshots under `docs/14-screenshots/` show historical rendering and must retain capture-date context.

## Model and freshness caveats

- Grafana reads the **batch** warehouse views whose rows are selected via `warehouse_model_loads`.
- Streaming CDC records live separately in ClickHouse `cdc_events`; they do **not** automatically refresh the published enrollment/application/offerings facts.
- Application facts have no department key. Application metrics are labeled **all departments** even when dashboard enrollment data is department-filtered.
- Distinct students in an enrollment selection are not the total row count of the Student dimension.
- Grade coverage measures recorded grade codes, not a course completion rate; credit-weighted grade points are based on the source grade scale, not a verified institutional GPA.

See [Business KPIs](./business-kpis.md), [Warehouse Tables](../07-data-warehouse/fact-and-dimension-tables.md), and [Validation Update](../13-validation/validation-update-2026-11-21.md).
