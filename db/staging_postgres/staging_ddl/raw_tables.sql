BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;

-- ============================================================
-- Applicants
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.applicants (
    "ApplicantId" INTEGER NOT NULL,
    "FirstName" VARCHAR(40) NOT NULL,
    "MiddleName" VARCHAR(40) NOT NULL,
    "LastName" VARCHAR(40) NOT NULL,
    "Gender" VARCHAR(1) NOT NULL,
    "StreetAddress" VARCHAR(50) NOT NULL,
    "City" VARCHAR(30) NOT NULL,
    "State" VARCHAR(2) NOT NULL,
    "ZipCode" VARCHAR(5) NOT NULL,
    "Telephone" VARCHAR(20) NOT NULL,
    "Email" VARCHAR(80) NOT NULL,
    "DateOfBirth" DATE NOT NULL,
    "ApplicationReceivedDate" DATE NOT NULL,
    "EntryTerm" VARCHAR(6) NOT NULL,
    "ApplicationType" VARCHAR(1) NOT NULL,
    "Gpa" NUMERIC(3,2),
    "SatMathScore" INTEGER,
    "SatReadingScore" INTEGER,
    "SatWritingScore" INTEGER,
    "EssayScore" INTEGER,
    "ApplicationStatus" VARCHAR(1) NOT NULL,
    "AdmissionPoints" DOUBLE PRECISION,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_applicants_batch
        PRIMARY KEY ("ApplicantId", _batch_id),
    CONSTRAINT ck_applicants_gender
        CHECK (UPPER("Gender") IN ('F', 'M')),
    CONSTRAINT ck_applicants_gpa
        CHECK ("Gpa" BETWEEN 0 AND 4),
    CONSTRAINT ck_applicants_sat_math_score
        CHECK ("SatMathScore" BETWEEN 200 AND 800),
    CONSTRAINT ck_applicants_sat_reading_score
        CHECK ("SatReadingScore" BETWEEN 200 AND 800),
    CONSTRAINT ck_applicants_sat_writing_score
        CHECK ("SatWritingScore" BETWEEN 200 AND 800)
);

-- ============================================================
-- ClassStandings
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.class_standings (
    "ClassStandingCode" VARCHAR(2) NOT NULL,
    "ClassStandingName" VARCHAR(25) NOT NULL,
    "DegreeLevelCode" VARCHAR(1) NOT NULL,
    "RequiredCredits" INTEGER NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("ClassStandingCode", _batch_id)
);

-- ============================================================
-- CourseEnrollments
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.course_enrollments (
    "CourseEnrollmentId" INTEGER NOT NULL,
    "CourseOfferingId" INTEGER NOT NULL,
    "StudentId" INTEGER NOT NULL,
    "Grade" VARCHAR(1),

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("CourseEnrollmentId", _batch_id)
);

-- ============================================================
-- CourseOfferings
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.course_offerings (
    "CourseOfferingId" INTEGER NOT NULL,
    "DepartmentCode" VARCHAR(2) NOT NULL,
    "CourseNumber" INTEGER NOT NULL,
    "TermCode" VARCHAR(6) NOT NULL,
    "Section" INTEGER NOT NULL,
    "Capacity" INTEGER NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_course_offerings_batch
        PRIMARY KEY ("CourseOfferingId", _batch_id),
    CONSTRAINT ck_course_offerings_capacity
        CHECK ("Capacity" > 0 AND "Capacity" < 1000)
);

-- ============================================================
-- Courses
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.courses (
    "DepartmentCode" VARCHAR(2) NOT NULL,
    "CourseNumber" INTEGER NOT NULL,
    "CourseTitle" VARCHAR(64) NOT NULL,
    "CourseDescription" VARCHAR(512) NOT NULL,
    "Credits" NUMERIC(3,1) NOT NULL,
    "MaximumSectionSize" INTEGER,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_courses_batch
        PRIMARY KEY ("DepartmentCode", "CourseNumber", _batch_id),
    CONSTRAINT ck_courses_course_number
        CHECK ("CourseNumber" BETWEEN 9 AND 1000)
);

-- ============================================================
-- DegreeLevels
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.degree_levels (
    "DegreeLevelCode" VARCHAR(1) NOT NULL,
    "DegreeLevelName" VARCHAR(20) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("DegreeLevelCode", _batch_id)
);

-- ============================================================
-- DegreeRequirements
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.degree_requirements (
    "RequirementId" INTEGER NOT NULL,
    "DegreeId" INTEGER NOT NULL,
    "ClassStandingCode" VARCHAR(2) NOT NULL,
    "SessionCode" VARCHAR(2) NOT NULL,
    "DepartmentCode" VARCHAR(2) NOT NULL,
    "CourseNumber" INTEGER NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("RequirementId", _batch_id)
);

-- ============================================================
-- Degrees
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.degrees (
    "DegreeId" INTEGER NOT NULL,
    "DepartmentCode" VARCHAR(2) NOT NULL,
    "DegreeTypeCode" VARCHAR(3) NOT NULL,
    "DegreeName" VARCHAR(64) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("DegreeId", _batch_id)
);

-- ============================================================
-- DegreeTypes
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.degree_types (
    "DegreeTypeCode" VARCHAR(3) NOT NULL,
    "DegreeTypeName" VARCHAR(25) NOT NULL,
    "DegreeLevelCode" VARCHAR(1) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("DegreeTypeCode", _batch_id)
);

-- ============================================================
-- Departments
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.departments (
    "DepartmentCode" VARCHAR(2) NOT NULL,
    "DepartmentName" VARCHAR(25) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("DepartmentCode", _batch_id)
);

-- ============================================================
-- Grades
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.grades (
    "Grade" VARCHAR(1) NOT NULL,
    "Points" NUMERIC(2,1),
    "IncludeInGpa" BOOLEAN NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("Grade", _batch_id)
);

-- ============================================================
-- Sessions
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.sessions (
    "SessionCode" VARCHAR(2) NOT NULL,
    "SessionName" VARCHAR(10) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("SessionCode", _batch_id)
);

-- ============================================================
-- StudentProfiles
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.student_profiles (
    "StudentId" INTEGER NOT NULL,
    "ProfileId" INTEGER NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("StudentId", _batch_id)
);

-- ============================================================
-- Students
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.students (
    "StudentId" INTEGER NOT NULL,
    "FirstName" VARCHAR(40) NOT NULL,
    "MiddleName" VARCHAR(40) NOT NULL,
    "LastName" VARCHAR(40) NOT NULL,
    "Gender" VARCHAR(1) NOT NULL,
    "StreetAddress" VARCHAR(50) NOT NULL,
    "City" VARCHAR(30) NOT NULL,
    "State" VARCHAR(2) NOT NULL,
    "ZipCode" VARCHAR(5) NOT NULL,
    "Telephone" VARCHAR(20) NOT NULL,
    "email" VARCHAR(80) NOT NULL,
    "DateOfBirth" DATE NOT NULL,
    "DegreePursued" INTEGER NOT NULL,
    "ClassStandingCode" VARCHAR(2),
    "StudentStatus" VARCHAR(1) NOT NULL,
    "EnrollmentTerm" VARCHAR(6) NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY ("StudentId", _batch_id)
);

-- ============================================================
-- Terms
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.terms (
    "TermCode" VARCHAR(6) NOT NULL,
    "SessionCode" VARCHAR(2) NOT NULL,
    "Year" INTEGER NOT NULL,

    _batch_id BIGINT NOT NULL REFERENCES metadata.extraction_batches(batch_id),
    _extracted_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT pk_terms_batch
        PRIMARY KEY ("TermCode", _batch_id),
    CONSTRAINT ck_terms_year
        CHECK ("Year" BETWEEN 1990 AND 2050)
);

-- ============================================================
-- Batch indexes
-- ============================================================

CREATE INDEX IF NOT EXISTS ix_applicants_batch
    ON raw.applicants (_batch_id);

CREATE INDEX IF NOT EXISTS ix_class_standings_batch
    ON raw.class_standings (_batch_id);

CREATE INDEX IF NOT EXISTS ix_course_enrollments_batch
    ON raw.course_enrollments (_batch_id);

CREATE INDEX IF NOT EXISTS ix_course_offerings_batch
    ON raw.course_offerings (_batch_id);

CREATE INDEX IF NOT EXISTS ix_courses_batch
    ON raw.courses (_batch_id);

CREATE INDEX IF NOT EXISTS ix_degree_levels_batch
    ON raw.degree_levels (_batch_id);

CREATE INDEX IF NOT EXISTS ix_degree_requirements_batch
    ON raw.degree_requirements (_batch_id);

CREATE INDEX IF NOT EXISTS ix_degrees_batch
    ON raw.degrees (_batch_id);

CREATE INDEX IF NOT EXISTS ix_degree_types_batch
    ON raw.degree_types (_batch_id);

CREATE INDEX IF NOT EXISTS ix_departments_batch
    ON raw.departments (_batch_id);

CREATE INDEX IF NOT EXISTS ix_grades_batch
    ON raw.grades (_batch_id);

CREATE INDEX IF NOT EXISTS ix_sessions_batch
    ON raw.sessions (_batch_id);

CREATE INDEX IF NOT EXISTS ix_student_profiles_batch
    ON raw.student_profiles (_batch_id);

CREATE INDEX IF NOT EXISTS ix_students_batch
    ON raw.students (_batch_id);

CREATE INDEX IF NOT EXISTS ix_terms_batch
    ON raw.terms (_batch_id);

COMMIT;