# Business KPI Reference

The provisioned dashboard queries the published ClickHouse model. It does
not define institutional policy; confirm definitions with data owners.

| KPI / candidate | Current status and definition |
| --- | --- |
| Applications | Implemented as the sum of `application_count` in FactApplication. The grain is one source `ApplicantId` row and does not assert a distinct person count. |
| Applications by status | Implemented from `DimApplicant.ApplicationStatus` and FactApplication counts. |
| Total enrollments | Implemented as the sum of `enrollment_count` in FactEnrollment under term and department filters. |
| Students | Exact distinct pseudonymous student keys represented by enrollment rows; not all rows in `Students`. |
| Course offerings | Count of FactCourseOffering rows, including offerings with no enrollments. |
| Offered seats | Sum of source capacity values; it is not an enrollment count. |
| Enrollment trends | Grouped by academic term. The source has no enrollment event date, so the chart uses a representative date derived from session/year. |
| Average/weighted GPA | Not shown in the default dashboard because the institutional definition is not approved. Grade points and GPA inclusion flags remain available in the model. |
| Course completion rate | Unsupported: the model has no validated completion outcome or definition. |
| At-risk students | Unsupported: no approved risk definition or intervention outcome exists. |

Applicant/student keys use `STUDENT_ID_HMAC_KEY`; they are pseudonymous,
not anonymous. The same key is domain-separated for applicants and students.
The dashboard has seven panels and provides term and department selectors;
application panels use term but have no department relationship.
