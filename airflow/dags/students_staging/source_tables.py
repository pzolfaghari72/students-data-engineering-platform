"""Assessed SQL Server tables included in the staging pipeline."""

SOURCE_TABLES = (
    ("Applicants", "applicants"),
    ("ClassStandings", "class_standings"),
    ("CourseEnrollments", "course_enrollments"),
    ("CourseOfferings", "course_offerings"),
    ("Courses", "courses"),
    ("DegreeLevels", "degree_levels"),
    ("DegreeRequirements", "degree_requirements"),
    ("Degrees", "degrees"),
    ("DegreeTypes", "degree_types"),
    ("Departments", "departments"),
    ("Grades", "grades"),
    ("Sessions", "sessions"),
    ("StudentProfiles", "student_profiles"),
    ("Students", "students"),
    ("Terms", "terms"),
)

# Keep SCD2 for entity/reference rows; the large enrollment fact stream stays
# in raw snapshots and the immutable CDC event log.
SCD2_TABLES = frozenset(
    source_table
    for source_table, _ in SOURCE_TABLES
    if source_table != "CourseEnrollments"
)
