# Dimensional modeling

A fact records a business event or measurable entity at a declared grain. A dimension supplies descriptive context for filtering and grouping. Grain determines which counts and sums are valid. A star schema is convenient when a reader wants to compare enrollment, capacity and outcomes by academic term or department.

The batch warehouse has three source-row grains.

- **Application** has one row per `ApplicantId`.
- **Course offering** has one row per `CourseOfferingId`.
- **Enrollment** has one row per `CourseEnrollmentId`.

`ApplicantId` identifies a source record. A student can have multiple enrollment attempts. Neither the applicant-as-person nor a distinct student/offering pair is asserted as a fact key. Capacity belongs to the offering fact. Summing it after joining to enrollment attempts would multiply seats. Aggregate each measure at its own grain and combine summaries at a compatible term or department level.

The nine dimensions are Applicant, Class Standing, Course, Date, Degree, Department, Grade, Student and Term. Course uses the composite source business key `(DepartmentCode, CourseNumber)`. Application facts use received date and entry term. Offerings and attempts use academic term because the assessed source gives no event date for those records. Date of birth stays outside the warehouse.

Namespaced SHA-256 supplies deterministic non-person identifiers. Applicant/student keys use HMAC pseudonyms. Student keys retain the previous HMAC input format. Applicant keys use an entity prefix. Key pseudonymization reduces exposure and still supports linkage within an authorized data model.

The batch dimensions are full snapshot copies identified by publication. Row-level business history is available through retained CDC events, while CDC current state is independent of the star. [Logical model](../02-architecture/diagrams/database-models.md) · [Fact and dimension catalog](fact-and-dimension-tables.md) · [SCD Type 2 concept](scd-type-2.md)
