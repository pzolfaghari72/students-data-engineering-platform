# ADR-002: Student Data Dimensional Model

## Status

Accepted; implemented as a snapshot-based three-fact model.

## Context

The source contains applicants, students, course offerings, and course
enrollments. Source identifiers define row-level grains, while some business
semantics (such as whether an applicant row represents a person or a
submission) are not represented in the source schema.

## Decision

Model nine dimensions—Applicant, Class Standing, Course, Date, Degree,
Department, Grade, Student, and Term—and three facts:

- FactApplication: one row per `ApplicantId`.
- FactCourseOffering: one row per `CourseOfferingId`.
- FactEnrollment: one row per `CourseEnrollmentId`.

Use stable, namespaced dimension keys, HMAC pseudonyms for applicant/student
identifiers, and snapshot-versioned physical tables. Exclude direct
identifiers, contact details, and date of birth from the analytical model.

## Alternatives considered

- A full source mirror in ClickHouse: would not provide explicit fact grains
  or analytical dimensions.
- A degree-requirements fact: it has a different grain and is outside the
  requested three-fact model.
- Merging CDC history into batch facts: bootstrap/reconciliation semantics are
  not established.

## Consequences

All fact joins must preserve the corresponding source-row counts, and all
dimension keys must be unique before publication. ApplicantId is treated as
one source row per application record but does not assert person identity.
Dimensions are snapshot copies, not warehouse SCD2 tables; historical student
attributes require a separate effective-dating decision. Rotating the HMAC
secret changes pseudonyms.

## Implementation references

`spark/load_warehouse.py`, `db/warehouse_clickhouse/warehouse_ddl.sql`, and
[Fact and Dimension Tables](../07-data-warehouse/fact-and-dimension-tables.md).
