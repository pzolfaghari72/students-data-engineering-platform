# Transformations

The batch model turns application, offering and enrollment records into three fact grains. Reference tables supply conformed descriptive context. SQL Server remains the operational model, PostgreSQL preserves the extract, and Spark shapes the analytical model for ClickHouse.

| Transformation | Rule |
|---|---|
| Applications | Keep one row per application. Resolve entry term and received date. Use the pseudonymous applicant key. |
| Offerings | Keep one row per offering. Attach course, department and term, with capacity at offering grain. |
| Enrollments | Keep one row per attempt. Follow the offering to course, department and term, and the student to degree and class standing. Preserve nullable grade/class-standing where the model allows. |
| Date | Generate a daily calendar across application received dates in the input. Empty applications have no calendar range. |
| Grades | Preserve the grade code, points and whether the source grade is included in weighted grade measures. |
| Sensitive keys | Use the deployment HMAC secret for student/applicant keys. Other descriptive keys are derived deterministically. |

Business SQL should use the grain matching its measure. Counting seats from joined enrollment attempts would multiply capacity by attendance. Aggregate seats from the offering fact and counts from the enrollment fact before forming a ratio. The [KPI guide](../08-analytics-and-monitoring/business-kpis.md) explains the actual dashboard scope.
