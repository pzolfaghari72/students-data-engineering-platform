# SQL Server Source

The external source is SQL Server database `Students`, schema `dbo`. The
repository contains captured DDL and assessment results, not a local SQL
Server container. The source owner remains authoritative for live schema,
permissions and business meaning.

| Table group | Tables | Representative observed key |
| --- | --- | --- |
| Applications | `Applicants` | `ApplicantId` |
| Student and status | `Students`, `StudentProfiles`, `ClassStandings` | `StudentId`, `StudentId`, `ClassStandingCode` |
| Enrollment | `CourseEnrollments` | `CourseEnrollmentId` |
| Catalog and offerings | `CourseOfferings`, `Courses`, `Departments` | `CourseOfferingId`, `(DepartmentCode, CourseNumber)`, `DepartmentCode` |
| Academic calendar | `Terms`, `Sessions` | `TermCode`, `SessionCode` |
| Degree/reference | `Degrees`, `DegreeTypes`, `DegreeLevels`, `DegreeRequirements` | `DegreeId`, `DegreeTypeCode`, `DegreeLevelCode`, `RequirementId` |
| Grading | `Grades` | `Grade` |

Captured relationships include `CourseEnrollments` → `Students`,
`CourseOfferings`, and `Grades`; offerings → course composite key and `Terms`;
Students → `Degrees`, `ClassStandings`, and `Terms`; and degree requirements
→ degrees, courses, class standings and sessions. The detailed captured
foreign-key output is `db/source_assessment/results/06_foreign_keys.csv`.

The saved assessment reports about 3.7 million enrollment rows, but those are
historical counts, not a live guarantee. The source has direct identifiers and
contact information; handle the source and raw staging as sensitive data.
`db/source_assessment/` contains captured source DDL, inspection SQL, historical
CSV/workbook assessment results, and the entity relationship diagram. These
artifacts are not automatically refreshed and do not establish the current
live schema or row counts. The entity diagram is available in
[the architecture diagrams](../02-architecture/diagrams/source-entity-relationship.png).
See [source extraction](./source-extraction.md) and
[data model](../02-architecture/data-model.md).
