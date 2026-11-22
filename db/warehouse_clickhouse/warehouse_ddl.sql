CREATE TABLE IF NOT EXISTS warehouse_model_loads (
    snapshot_id String,
    source_run_id UInt64,
    completed_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    dim_applicant_rows UInt64,
    dim_class_standing_rows UInt64,
    dim_course_rows UInt64,
    dim_date_rows UInt64,
    dim_degree_rows UInt64,
    dim_department_rows UInt64,
    dim_grade_rows UInt64,
    dim_student_rows UInt64,
    dim_term_rows UInt64,
    fact_application_rows UInt64,
    fact_course_offering_rows UInt64,
    fact_enrollment_rows UInt64
)
ENGINE = MergeTree
ORDER BY (completed_at, snapshot_id);

ALTER TABLE warehouse_model_loads
    MODIFY COLUMN completed_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC');

ALTER TABLE warehouse_model_loads
    ADD COLUMN IF NOT EXISTS snapshot_started_at Nullable(DateTime64(3, 'UTC'));

CREATE OR REPLACE VIEW batch_publication_status SQL SECURITY DEFINER AS
SELECT snapshot_id, source_run_id, snapshot_started_at, completed_at,
       fact_application_rows, fact_course_offering_rows, fact_enrollment_rows
FROM warehouse_model_loads
ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
LIMIT 1;

CREATE TABLE IF NOT EXISTS dim_applicant_snapshots (
    snapshot_id String,
    applicant_key String,
    application_type LowCardinality(String),
    application_status LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (snapshot_id, applicant_key);

CREATE TABLE IF NOT EXISTS dim_class_standing_snapshots (
    snapshot_id String,
    class_standing_key String,
    class_standing_code String,
    class_standing_name String,
    degree_level_code String,
    degree_level_name LowCardinality(String),
    required_credits Int32
)
ENGINE = MergeTree
ORDER BY (snapshot_id, class_standing_key);

CREATE TABLE IF NOT EXISTS dim_course_snapshots (
    snapshot_id String,
    course_key String,
    department_key String,
    department_code String,
    course_number Int32,
    course_title String,
    credits Decimal(3, 1),
    maximum_section_size Nullable(Int32)
)
ENGINE = MergeTree
ORDER BY (snapshot_id, department_code, course_number);

CREATE TABLE IF NOT EXISTS dim_date_snapshots (
    snapshot_id String,
    date_key UInt32,
    calendar_date Date,
    calendar_year UInt16,
    calendar_quarter UInt8,
    calendar_month UInt8,
    month_name LowCardinality(String),
    day_of_month UInt8,
    day_of_week UInt8,
    day_name LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (snapshot_id, date_key);

CREATE TABLE IF NOT EXISTS dim_degree_snapshots (
    snapshot_id String,
    degree_key String,
    degree_id Int32,
    degree_name String,
    department_key String,
    department_code String,
    degree_type_code String,
    degree_type_name String,
    degree_level_code String,
    degree_level_name String
)
ENGINE = MergeTree
ORDER BY (snapshot_id, degree_id);

CREATE TABLE IF NOT EXISTS dim_department_snapshots (
    snapshot_id String,
    department_key String,
    department_code String,
    department_name String
)
ENGINE = MergeTree
ORDER BY (snapshot_id, department_code);

CREATE TABLE IF NOT EXISTS dim_grade_snapshots (
    snapshot_id String,
    grade_key String,
    grade_code String,
    grade_points Nullable(Decimal(3, 1)),
    include_in_gpa UInt8
)
ENGINE = MergeTree
ORDER BY (snapshot_id, grade_code);

CREATE TABLE IF NOT EXISTS dim_student_snapshots (
    snapshot_id String,
    student_key String,
    student_status LowCardinality(String),
    degree_key String,
    class_standing_key Nullable(String),
    enrollment_term_key String
)
ENGINE = MergeTree
ORDER BY (snapshot_id, student_key);

CREATE TABLE IF NOT EXISTS dim_term_snapshots (
    snapshot_id String,
    term_key String,
    term_code String,
    session_code String,
    session_name LowCardinality(String),
    year UInt16
)
ENGINE = MergeTree
ORDER BY (snapshot_id, term_code);

CREATE TABLE IF NOT EXISTS fact_application_snapshots (
    snapshot_id String,
    applicant_key String,
    application_date_key UInt32,
    entry_term_key String,
    entry_term_code String,
    application_count UInt8,
    gpa Nullable(Decimal(3, 2)),
    sat_math_score Nullable(Int32),
    sat_reading_score Nullable(Int32),
    sat_writing_score Nullable(Int32),
    essay_score Nullable(Int32),
    admission_points Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (snapshot_id, entry_term_code, application_date_key, applicant_key);

CREATE TABLE IF NOT EXISTS fact_course_offering_snapshots (
    snapshot_id String,
    course_offering_id UInt32,
    course_key String,
    department_key String,
    department_code String,
    term_key String,
    term_code String,
    section UInt32,
    capacity UInt16,
    offering_count UInt8
)
ENGINE = MergeTree
ORDER BY (snapshot_id, term_code, department_code, course_offering_id);

CREATE TABLE IF NOT EXISTS fact_enrollment_snapshots (
    snapshot_id String,
    course_enrollment_id UInt32,
    student_key String,
    course_offering_id UInt32,
    course_key String,
    department_key String,
    department_code String,
    course_number Int32,
    term_key String,
    term_code String,
    grade_key Nullable(String),
    grade_code Nullable(String),
    degree_key String,
    class_standing_key Nullable(String),
    credits Decimal(3, 1),
    grade_points Nullable(Decimal(3, 1)),
    include_in_gpa UInt8,
    enrollment_count UInt8
)
ENGINE = MergeTree
ORDER BY (snapshot_id, term_code, department_code, course_offering_id, course_enrollment_id);

CREATE OR REPLACE VIEW dim_applicant SQL SECURITY DEFINER AS
SELECT applicant_key, application_type, application_status
FROM dim_applicant_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_class_standing SQL SECURITY DEFINER AS
SELECT
    class_standing_key,
    class_standing_code,
    class_standing_name,
    degree_level_code,
    degree_level_name,
    required_credits
FROM dim_class_standing_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_course SQL SECURITY DEFINER AS
SELECT
    course_key,
    department_key,
    department_code,
    course_number,
    course_title,
    credits,
    maximum_section_size
FROM dim_course_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_date SQL SECURITY DEFINER AS
SELECT
    date_key,
    calendar_date,
    calendar_year,
    calendar_quarter,
    calendar_month,
    month_name,
    day_of_month,
    day_of_week,
    day_name
FROM dim_date_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_degree SQL SECURITY DEFINER AS
SELECT
    degree_key,
    degree_id,
    degree_name,
    department_key,
    department_code,
    degree_type_code,
    degree_type_name,
    degree_level_code,
    degree_level_name
FROM dim_degree_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_department SQL SECURITY DEFINER AS
SELECT department_key, department_code, department_name
FROM dim_department_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_grade SQL SECURITY DEFINER AS
SELECT grade_key, grade_code, grade_points, include_in_gpa
FROM dim_grade_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_student SQL SECURITY DEFINER AS
SELECT
    student_key,
    student_status,
    degree_key,
    class_standing_key,
    enrollment_term_key
FROM dim_student_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW dim_term SQL SECURITY DEFINER AS
SELECT term_key, term_code, session_code, session_name, year
FROM dim_term_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW fact_application SQL SECURITY DEFINER AS
SELECT
    applicant_key,
    application_date_key,
    entry_term_key,
    entry_term_code,
    application_count,
    gpa,
    sat_math_score,
    sat_reading_score,
    sat_writing_score,
    essay_score,
    admission_points
FROM fact_application_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW fact_course_offering SQL SECURITY DEFINER AS
SELECT
    course_offering_id,
    course_key,
    department_key,
    department_code,
    term_key,
    term_code,
    section,
    capacity,
    offering_count
FROM fact_course_offering_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);

CREATE OR REPLACE VIEW fact_course_enrollment SQL SECURITY DEFINER AS
SELECT
    course_enrollment_id,
    student_key,
    course_offering_id,
    course_key,
    department_key,
    department_code,
    course_number,
    term_key,
    term_code,
    grade_key,
    grade_code,
    degree_key,
    class_standing_key,
    credits,
    grade_points,
    include_in_gpa,
    enrollment_count
FROM fact_enrollment_snapshots
WHERE snapshot_id = (
    SELECT snapshot_id
    FROM warehouse_model_loads
    ORDER BY source_run_id DESC, completed_at DESC, snapshot_id DESC
    LIMIT 1
);
