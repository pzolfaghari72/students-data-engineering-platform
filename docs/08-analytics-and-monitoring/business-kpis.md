# Business KPI Reference

The published **Student Enrollment, Capacity & Grades** dashboard has 18 panels. It uses the latest published **batch** ClickHouse analytical views and term / department selectors. These definitions describe configured calculations, not official institutional policy.

## Overview and enrollment

| Metric | Business meaning and caution |
| --- | --- |
| Enrollment records | Number of selected `fact_course_enrollment` records (enrollment attempts), not necessarily distinct people or completions. |
| Unique students | Exact distinct pseudonymous `student_key` in the selected enrollment rows, not total source students. |
| Course offerings | Rows of `fact_course_offering` in selected term/department scope; includes offerings with no enrollment. |
| Offered seats | Sum of `capacity` for the selected offering rows; not actual enrollment. |
| Enrollment / capacity | Enrollment-record count divided by offering seat capacity for a selected scope. Can exceed 100%; not a physical-seat utilization guarantee. |
| Applications | Source Applicant rows in `fact_application`; labeled **all departments** because there is no department relationship in this fact. |
| Enrollment by academic term | Enrollment counts by term, not by an actual per-enrollment event timestamp. |
| Enrollment by department | Enrollment counts using the selected offering's department. |

## Capacity and admissions

- **Enrollment / capacity by department:** denominator is the selected department's summed offering capacity.
- **Application status:** application rows grouped by status from `dim_applicant`.
- **Applications by entry term:** application records grouped by target term; no department filter can be inferred from the current fact.
- **Department comparison:** table of enrollment/capacity/other metrics scoped by term and department.

## Grade analytics

| Panel | Interpretation |
| --- | --- |
| Grades recorded | Enrollment attempts with a nonblank recorded `grade_code`. |
| Grade coverage | `100 * (attempts with recorded grade) / enrollment attempts` in selected scope. This is not a pass or completion rate. |
| Credit-weighted grade points | `sum(grade_points * credits) / sum(credits)` for recorded-grade attempts that are GPA-eligible and have positive credits and valid points. Uses source point scale. |
| Grade distribution | Counts by grade code, not a completion classification. |
| Credit-weighted grade points by term | Same eligible-credit definition grouped by academic term. |
| Grade results by department | Department-level grade metrics using the selected enrollment scope. |

## Scope and data sensitivity

Applications in the dashboard lack a department foreign key, so their cards/charts are explicitly labeled **all departments**. Student and applicant keys are HMAC pseudonyms; they are not anonymous identifiers. No direct student names, addresses, or contact information are in the published star model.

**Refresh warning:** dashboard refresh reads ClickHouse; it does not apply streaming CDC to the batch warehouse. The last supplied batch snapshot contained 3,724,654 enrollment rows, 59,279 offerings and 600,000 applications; these are historical observed counts, not a live result or a promise of current freshness.

See [Grafana](./grafana.md), [Fact and Dimension Tables](../07-data-warehouse/fact-and-dimension-tables.md), and [2026-11-21 validation](../13-validation/validation-update-2026-11-21.md).
