# Dimensional Modeling

The batch warehouse has three explicit source-row grains:

- FactApplication: one row per `ApplicantId`.
- FactCourseOffering: one row per `CourseOfferingId`.
- FactEnrollment: one row per `CourseEnrollmentId`.

The source does not establish that `ApplicantId` is a person identifier or
that a `(StudentId, CourseOfferingId)` enrollment pair is unique. The model
does not infer either relationship or deduplicate source rows.

The nine dimensions are Applicant, Class Standing, Course, Date, Degree,
Department, Grade, Student, and Term. Course uses the composite source
business key `(DepartmentCode, CourseNumber)`. FactApplication uses
application-received date and entry term; the source has no enrollment or
course-offering event date, so those facts are analyzed by academic term.
Date of birth is excluded.

Non-PII dimension keys are deterministic namespaced SHA-256 values.
Applicant/student keys are HMAC pseudonyms; Student keys retain the existing
HMAC input format so they stay stable across snapshots. Applicant keys use an
entity prefix to avoid linking applicant IDs to student IDs.

Dimensions are snapshot copies, not row-level warehouse SCD Type 2
dimensions. Changes are represented by new published `snapshot_id`
generations. The CDC history and its SCD2 projections remain separate in
PostgreSQL and ClickHouse. See [the model diagram](../02-architecture/diagrams/system-context.md)
and [SCD Type 2](./scd-type-2.md).
