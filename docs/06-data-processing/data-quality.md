# Data Quality

Only checks present in code or captured assessment are listed as implemented.
The source assessment is historical and does not prove a live database still
satisfies the same conditions.

| Rule | Dataset | Implemented validation | Failure / limitation |
| --- | --- | --- | --- |
| Required source tables exist in the batch run | Metadata | Spark checks every required table has a successful batch. | Fails before warehouse read if missing. |
| Join preserves each fact grain | Spark facts | Application, course-offering, and enrollment counts must each equal their source row count after joins. | Fails before warehouse writes/publication. |
| Dimension key uniqueness | Spark dimensions | Each generated dimension key is checked for duplicates. | Fails before warehouse writes/publication. |
| Nullable/invalid lookup keys | Spark enrollment | Null grades are allowed; non-null grades and student class standings must match their dimensions. | Fails before warehouse writes/publication. |
| Source relationships for facts | Spark joins | Required term, course, department, degree, student, and offering joins must preserve the input fact count. | Fails before publication if rows are lost or multiplied. |
| Raw row bounds | Selected PostgreSQL DDL | CHECK constraints exist on selected fields (e.g. GPA, capacities, term year). | Rejects an insert; constraints are not exhaustive business validation. |
| CDC topic/source identity | CDC parser | Validates database, schema, table and event shape. | Raises an error; consumer transaction is rolled back. |
| CDC event uniqueness | PostgreSQL history | Unique topic/partition/offset with `ON CONFLICT DO NOTHING`. | Deduplicates repeated transport positions. |
| Existing source referential checks | Saved assessment output | Captured assessment includes selected FK/integrity results. | Historical; rerun SQL for current source evidence. |

Not implemented as a complete control set: source-vs-warehouse value
reconciliation, freshness SLA/alerts, all-table null/range rules, business KPI
approval, or automatic rejected-row quarantine. These are recommendations,
not existing behavior. `ApplicantId`, `CourseOfferingId`, and
`CourseEnrollmentId` define source-row grains; the model does not infer a
person-level identity or uniqueness of a student/offering pair.
