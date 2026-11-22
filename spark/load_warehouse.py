"""Build and publish a ClickHouse dimensional-model snapshot."""

from __future__ import annotations

import argparse
import logging
import os
import uuid
from typing import Any

import clickhouse_connect
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    date_format,
    dayofmonth,
    dayofweek,
    explode,
    expr,
    lit,
    max as spark_max,
    min as spark_min,
    quarter,
    sequence,
    sha2,
    sum as spark_sum,
    udf,
    year,
)
from pyspark.sql.types import StringType

from warehouse_keys import pseudonymize_identifier

LOGGER = logging.getLogger("students_warehouse")

STAGING_JDBC_URL = os.environ["STAGING_JDBC_URL"]
STAGING_DB_USER = os.environ["STAGING_DB_USER"]
STAGING_DB_PASSWORD = os.environ["STAGING_DB_PASSWORD"]
WAREHOUSE_JDBC_URL = os.environ["WAREHOUSE_JDBC_URL"]
WAREHOUSE_DB_USER = os.environ["WAREHOUSE_DB_USER"]
WAREHOUSE_DB_PASSWORD = os.environ["WAREHOUSE_DB_PASSWORD"]

STAGING_DRIVER = os.environ["STAGING_JDBC_DRIVER"]
WAREHOUSE_DRIVER = os.environ["WAREHOUSE_JDBC_DRIVER"]
REQUIRED_TABLES = {
    "Applicants",
    "ClassStandings",
    "CourseEnrollments",
    "CourseOfferings",
    "Courses",
    "DegreeLevels",
    "Degrees",
    "DegreeTypes",
    "Departments",
    "Grades",
    "Sessions",
    "Students",
    "Terms",
}
READ_PARTITIONS = int(os.getenv("SPARK_BATCH_READ_PARTITIONS", "2"))
JDBC_FETCH_SIZE = int(os.getenv("SPARK_BATCH_FETCH_SIZE", "10000"))
WRITE_BATCH_SIZE = int(os.getenv("SPARK_BATCH_WRITE_SIZE", "10000"))

DIMENSION_TABLES = {
    "dim_applicant_snapshots": "dim_applicant_rows",
    "dim_class_standing_snapshots": "dim_class_standing_rows",
    "dim_course_snapshots": "dim_course_rows",
    "dim_date_snapshots": "dim_date_rows",
    "dim_degree_snapshots": "dim_degree_rows",
    "dim_department_snapshots": "dim_department_rows",
    "dim_grade_snapshots": "dim_grade_rows",
    "dim_student_snapshots": "dim_student_rows",
    "dim_term_snapshots": "dim_term_rows",
}
FACT_TABLES = {
    "fact_application_snapshots": "fact_application_rows",
    "fact_course_offering_snapshots": "fact_course_offering_rows",
    "fact_enrollment_snapshots": "fact_enrollment_rows",
}
FACT_KEYS = {
    "fact_application_snapshots": "applicant_key",
    "fact_course_offering_snapshots": "course_offering_id",
    "fact_enrollment_snapshots": "course_enrollment_id",
}
DIMENSION_KEYS = {
    "dim_applicant_snapshots": "applicant_key",
    "dim_class_standing_snapshots": "class_standing_key",
    "dim_course_snapshots": "course_key",
    "dim_date_snapshots": "date_key",
    "dim_degree_snapshots": "degree_key",
    "dim_department_snapshots": "department_key",
    "dim_grade_snapshots": "grade_key",
    "dim_student_snapshots": "student_key",
    "dim_term_snapshots": "term_key",
}


def _jdbc_reader(spark: SparkSession, url: str, user: str, password: str) -> Any:
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .option("driver", STAGING_DRIVER)
        .option("fetchsize", str(JDBC_FETCH_SIZE))
    )


def _read_staging_query(spark: SparkSession, query: str) -> DataFrame:
    return _jdbc_reader(
        spark, STAGING_JDBC_URL, STAGING_DB_USER, STAGING_DB_PASSWORD
    ).option("dbtable", f"({query}) AS staging_source").load()


def _snapshot_batches(spark: SparkSession, run_id: int) -> tuple[Any, dict[str, int], dict[str, int]]:
    selected_run = _read_staging_query(
        spark,
        f"""
        SELECT started_at
        FROM metadata.pipeline_runs
        WHERE run_id = {run_id}
          AND pipeline_name = 'sqlserver_to_postgres_staging'
          AND run_type = 'FULL'
          AND status = 'SUCCESS'
        """,
    ).first()
    if selected_run is None:
        raise RuntimeError(f"Staging run {run_id} is not a successful full snapshot.")

    batches = _read_staging_query(
        spark,
        f"""
        SELECT source_table, batch_id, source_rows_expected
        FROM metadata.extraction_batches
        WHERE run_id = {run_id}
          AND extraction_type = 'SNAPSHOT'
          AND status = 'SUCCESS'
          AND source_rows_expected = extracted_rows
          AND source_rows_expected = inserted_rows
        """,
    ).collect()
    batch_ids = {row["source_table"]: int(row["batch_id"]) for row in batches}
    missing = REQUIRED_TABLES - batch_ids.keys()
    if missing:
        raise RuntimeError(
            f"Successful snapshot run {run_id} is missing required tables: "
            f"{', '.join(sorted(missing))}."
        )
    if len(batch_ids) != len(batches) or len(batch_ids) != 15:
        raise RuntimeError(f"Snapshot run {run_id} must have 15 reconciled table batches.")
    source_counts = {row["source_table"]: int(row["source_rows_expected"]) for row in batches}
    return selected_run["started_at"], batch_ids, source_counts


def _read_raw_table(
    spark: SparkSession,
    table: str,
    batch_id: int,
    partition_column: str | None = None,
) -> DataFrame:
    if partition_column is None:
        dbtable = (
            f'(SELECT * FROM raw.{table} WHERE "_batch_id" = {batch_id}) '
            "AS snapshot_rows"
        )
    else:
        dbtable = (
            f'(SELECT snapshot_rows.*, '
            f'"{partition_column}" AS partition_key '
            f'FROM raw.{table} AS snapshot_rows '
            f'WHERE "_batch_id" = {batch_id}) AS snapshot_rows'
        )
    reader = _jdbc_reader(
        spark, STAGING_JDBC_URL, STAGING_DB_USER, STAGING_DB_PASSWORD
    ).option("dbtable", dbtable)

    if partition_column is None:
        return reader.load()

    bounds = _read_staging_query(
        spark,
        f"""
        SELECT MIN("{partition_column}") AS lower_bound,
               MAX("{partition_column}") AS upper_bound
        FROM raw.{table}
        WHERE "_batch_id" = {batch_id}
        """,
    ).first()
    if bounds is None or bounds["lower_bound"] is None or bounds["upper_bound"] is None:
        return reader.load()

    return (
        reader.option("partitionColumn", "partition_key")
        .option("lowerBound", str(bounds["lower_bound"]))
        .option("upperBound", str(bounds["upper_bound"]))
        .option("numPartitions", str(READ_PARTITIONS))
        .load()
    )


def _surrogate_key(namespace: str, *parts: Column) -> Column:
    return sha2(
        concat_ws("\u001f", lit(namespace), *[part.cast("string") for part in parts]),
        256,
    )


def _pseudonymizer(secret: str) -> Any:
    if not secret:
        raise RuntimeError("STUDENT_ID_HMAC_KEY must not be empty.")

    def key_for(entity: str, identifier: Any) -> str | None:
        if identifier is None:
            return None
        return pseudonymize_identifier(entity, identifier, secret)

    return udf(key_for, StringType())


def _assert_no_orphans(
    source: DataFrame,
    target: DataFrame,
    condition: Column,
    relationship: str,
) -> None:
    if source.join(target, condition, "left_anti").limit(1).count():
        raise RuntimeError(f"Source relationship failed validation: {relationship}.")


def _date_dimension(applicants: DataFrame) -> DataFrame:
    bounds = applicants.agg(
        spark_min(col("ApplicationReceivedDate")).alias("first_date"),
        spark_max(col("ApplicationReceivedDate")).alias("last_date"),
    ).first()
    if bounds is None or bounds["first_date"] is None or bounds["last_date"] is None:
        dates = applicants.select(col("ApplicationReceivedDate").alias("calendar_date")).limit(0)
    else:
        dates = applicants.sparkSession.createDataFrame(
            [(bounds["first_date"], bounds["last_date"])],
            ["first_date", "last_date"],
        ).select(
            explode(
                sequence(col("first_date"), col("last_date"), expr("INTERVAL 1 DAY"))
            ).alias("calendar_date")
        )
    return dates.select(
        date_format(col("calendar_date"), "yyyyMMdd")
        .cast("int")
        .alias("date_key"),
        col("calendar_date"),
        year(col("calendar_date")).cast("short").alias("calendar_year"),
        quarter(col("calendar_date")).cast("tinyint").alias("calendar_quarter"),
        date_format(col("calendar_date"), "M")
        .cast("tinyint")
        .alias("calendar_month"),
        date_format(col("calendar_date"), "MMMM").alias("month_name"),
        dayofmonth(col("calendar_date")).cast("tinyint").alias("day_of_month"),
        dayofweek(col("calendar_date")).cast("tinyint").alias("day_of_week"),
        date_format(col("calendar_date"), "E").alias("day_name"),
    )


def build_warehouse_frames(
    spark: SparkSession,
    batch_ids: dict[str, int],
) -> tuple[dict[str, DataFrame], dict[str, DataFrame]]:
    pseudonymize = _pseudonymizer(os.environ["STUDENT_ID_HMAC_KEY"])
    applicants = _read_raw_table(
        spark, "applicants", batch_ids["Applicants"], "ApplicantId"
    )
    class_standings = _read_raw_table(
        spark, "class_standings", batch_ids["ClassStandings"]
    )
    enrollments = _read_raw_table(
        spark,
        "course_enrollments",
        batch_ids["CourseEnrollments"],
        "CourseEnrollmentId",
    )
    offerings = _read_raw_table(
        spark, "course_offerings", batch_ids["CourseOfferings"]
    )
    courses = _read_raw_table(spark, "courses", batch_ids["Courses"])
    degree_levels = _read_raw_table(spark, "degree_levels", batch_ids["DegreeLevels"])
    degrees = _read_raw_table(spark, "degrees", batch_ids["Degrees"])
    degree_types = _read_raw_table(spark, "degree_types", batch_ids["DegreeTypes"])
    departments = _read_raw_table(spark, "departments", batch_ids["Departments"])
    grades = _read_raw_table(spark, "grades", batch_ids["Grades"])
    sessions = _read_raw_table(spark, "sessions", batch_ids["Sessions"])
    students = _read_raw_table(
        spark, "students", batch_ids["Students"], "StudentId"
    )
    terms = _read_raw_table(spark, "terms", batch_ids["Terms"])

    department_dim = departments.select(
        _surrogate_key("department", col("DepartmentCode")).alias("department_key"),
        col("DepartmentCode").alias("department_code"),
        col("DepartmentName").alias("department_name"),
    )
    department_lookup = department_dim.alias("department")

    course_dim = (
        courses.alias("course")
        .join(
            department_lookup,
            col("course.DepartmentCode") == col("department.department_code"),
            "inner",
        )
        .select(
            _surrogate_key(
                "course",
                col("course.DepartmentCode"),
                col("course.CourseNumber"),
            ).alias("course_key"),
            col("department.department_key"),
            col("course.DepartmentCode").alias("department_code"),
            col("course.CourseNumber").alias("course_number"),
            col("course.CourseTitle").alias("course_title"),
            col("course.Credits").alias("credits"),
            col("course.MaximumSectionSize").alias("maximum_section_size"),
        )
    )
    course_lookup = course_dim.alias("course")

    degree_type_lookup = (
        degree_types.alias("degree_type")
        .join(
            degree_levels.alias("degree_level"),
            col("degree_type.DegreeLevelCode") == col("degree_level.DegreeLevelCode"),
            "inner",
        )
        .select(
            col("degree_type.DegreeTypeCode").alias("degree_type_code"),
            col("degree_type.DegreeTypeName").alias("degree_type_name"),
            col("degree_level.DegreeLevelCode").alias("degree_level_code"),
            col("degree_level.DegreeLevelName").alias("degree_level_name"),
        )
    )
    degree_dim = (
        degrees.alias("degree")
        .join(
            degree_type_lookup.alias("degree_type"),
            col("degree.DegreeTypeCode") == col("degree_type.degree_type_code"),
            "inner",
        )
        .join(
            department_lookup,
            col("degree.DepartmentCode") == col("department.department_code"),
            "inner",
        )
        .select(
            _surrogate_key("degree", col("degree.DegreeId")).alias("degree_key"),
            col("degree.DegreeId").alias("degree_id"),
            col("degree.DegreeName").alias("degree_name"),
            col("department.department_key"),
            col("degree.DepartmentCode").alias("department_code"),
            col("degree_type.degree_type_code"),
            col("degree_type.degree_type_name"),
            col("degree_type.degree_level_code"),
            col("degree_type.degree_level_name"),
        )
    )
    degree_lookup = degree_dim.alias("degree")

    class_standing_dim = (
        class_standings.alias("class_standing")
        .join(
            degree_levels.alias("degree_level"),
            col("class_standing.DegreeLevelCode")
            == col("degree_level.DegreeLevelCode"),
            "inner",
        )
        .select(
            _surrogate_key(
                "class-standing", col("class_standing.ClassStandingCode")
            ).alias("class_standing_key"),
            col("class_standing.ClassStandingCode").alias("class_standing_code"),
            col("class_standing.ClassStandingName").alias("class_standing_name"),
            col("class_standing.DegreeLevelCode").alias("degree_level_code"),
            col("degree_level.DegreeLevelName").alias("degree_level_name"),
            col("class_standing.RequiredCredits").alias("required_credits"),
        )
    )
    class_standing_lookup = class_standing_dim.alias("class_standing")

    term_dim = (
        terms.alias("term")
        .join(
            sessions.alias("session"),
            col("term.SessionCode") == col("session.SessionCode"),
            "inner",
        )
        .select(
            _surrogate_key("term", col("term.TermCode")).alias("term_key"),
            col("term.TermCode").alias("term_code"),
            col("term.SessionCode").alias("session_code"),
            col("session.SessionName").alias("session_name"),
            col("term.Year").cast("short").alias("year"),
        )
    )
    term_lookup = term_dim.alias("term")

    grade_dim = grades.select(
        _surrogate_key("grade", col("Grade")).alias("grade_key"),
        col("Grade").alias("grade_code"),
        col("Points").alias("grade_points"),
        col("IncludeInGpa").cast("boolean").alias("include_in_gpa"),
    )
    grade_lookup = grade_dim.alias("grade")

    student_lookup = (
        students.alias("student")
        .join(
            degree_lookup,
            col("student.DegreePursued") == col("degree.degree_id"),
            "inner",
        )
        .join(
            class_standing_lookup,
            col("student.ClassStandingCode")
            == col("class_standing.class_standing_code"),
            "left",
        )
        .join(
            term_lookup,
            col("student.EnrollmentTerm") == col("term.term_code"),
            "inner",
        )
        .select(
            col("student.StudentId").alias("source_student_id"),
            pseudonymize(lit("student"), col("student.StudentId").cast("string"))
            .alias("student_key"),
            col("student.StudentStatus").alias("student_status"),
            col("degree.degree_key"),
            col("class_standing.class_standing_key"),
            col("term.term_key").alias("enrollment_term_key"),
            col("student.DegreePursued").alias("source_degree_id"),
            col("student.ClassStandingCode").alias("source_class_standing_code"),
        )
    )

    applicant_dim = applicants.select(
        pseudonymize(lit("applicant"), col("ApplicantId").cast("string"))
        .alias("applicant_key"),
        col("ApplicationType").alias("application_type"),
        col("ApplicationStatus").alias("application_status"),
    )
    date_dim = _date_dimension(applicants)

    application_fact = (
        applicants.alias("applicant")
        .join(
            term_lookup,
            col("applicant.EntryTerm") == col("term.term_code"),
            "inner",
        )
        .join(
            date_dim.alias("date"),
            col("applicant.ApplicationReceivedDate") == col("date.calendar_date"),
            "inner",
        )
        .select(
            pseudonymize(
                lit("applicant"),
                col("applicant.ApplicantId").cast("string"),
            ).alias("applicant_key"),
            col("date.date_key").alias("application_date_key"),
            col("term.term_key").alias("entry_term_key"),
            col("applicant.EntryTerm").alias("entry_term_code"),
            lit(1).cast("tinyint").alias("application_count"),
            col("applicant.Gpa").alias("gpa"),
            col("applicant.SatMathScore").alias("sat_math_score"),
            col("applicant.SatReadingScore").alias("sat_reading_score"),
            col("applicant.SatWritingScore").alias("sat_writing_score"),
            col("applicant.EssayScore").alias("essay_score"),
            col("applicant.AdmissionPoints").alias("admission_points"),
        )
    )

    offering_fact = (
        offerings.alias("offering")
        .join(
            course_lookup,
            (col("offering.DepartmentCode") == col("course.department_code"))
            & (col("offering.CourseNumber") == col("course.course_number")),
            "inner",
        )
        .join(
            department_lookup,
            col("offering.DepartmentCode") == col("department.department_code"),
            "inner",
        )
        .join(
            term_lookup,
            col("offering.TermCode") == col("term.term_code"),
            "inner",
        )
        .select(
            col("offering.CourseOfferingId").alias("course_offering_id"),
            col("course.course_key"),
            col("department.department_key"),
            col("offering.DepartmentCode").alias("department_code"),
            col("term.term_key"),
            col("offering.TermCode").alias("term_code"),
            col("offering.Section").alias("section"),
            col("offering.Capacity").alias("capacity"),
            lit(1).cast("tinyint").alias("offering_count"),
        )
    )

    _assert_no_orphans(
        enrollments.filter(col("Grade").isNotNull()).alias("enrollment"),
        grade_lookup,
        col("enrollment.Grade") == col("grade.grade_code"),
        "CourseEnrollments.Grade",
    )
    _assert_no_orphans(
        students.filter(col("ClassStandingCode").isNotNull()).alias("student"),
        class_standing_lookup,
        col("student.ClassStandingCode")
        == col("class_standing.class_standing_code"),
        "Students.ClassStandingCode",
    )

    enrollment_fact = (
        enrollments.alias("enrollment")
        .join(
            offerings.alias("offering"),
            col("enrollment.CourseOfferingId")
            == col("offering.CourseOfferingId"),
            "inner",
        )
        .join(
            student_lookup.alias("student"),
            col("enrollment.StudentId") == col("student.source_student_id"),
            "inner",
        )
        .join(
            course_lookup,
            (col("offering.DepartmentCode") == col("course.department_code"))
            & (col("offering.CourseNumber") == col("course.course_number")),
            "inner",
        )
        .join(
            department_lookup,
            col("offering.DepartmentCode") == col("department.department_code"),
            "inner",
        )
        .join(
            term_lookup,
            col("offering.TermCode") == col("term.term_code"),
            "inner",
        )
        .join(
            grade_lookup,
            col("enrollment.Grade") == col("grade.grade_code"),
            "left",
        )
        .select(
            col("enrollment.CourseEnrollmentId").alias("course_enrollment_id"),
            col("student.student_key"),
            col("offering.CourseOfferingId").alias("course_offering_id"),
            col("course.course_key"),
            col("department.department_key"),
            col("offering.DepartmentCode").alias("department_code"),
            col("offering.CourseNumber").alias("course_number"),
            col("term.term_key"),
            col("offering.TermCode").alias("term_code"),
            col("grade.grade_key"),
            col("enrollment.Grade").alias("grade_code"),
            col("student.degree_key"),
            col("student.class_standing_key"),
            col("course.credits"),
            col("grade.grade_points"),
            coalesce(col("grade.include_in_gpa"), lit(False))
            .cast("boolean")
            .alias("include_in_gpa"),
            lit(1).cast("tinyint").alias("enrollment_count"),
        )
    )

    dimensions = {
        "dim_applicant_snapshots": applicant_dim,
        "dim_class_standing_snapshots": class_standing_dim,
        "dim_course_snapshots": course_dim,
        "dim_date_snapshots": date_dim,
        "dim_degree_snapshots": degree_dim,
        "dim_department_snapshots": department_dim,
        "dim_grade_snapshots": grade_dim,
        "dim_student_snapshots": student_lookup.select(
            "student_key",
            "student_status",
            "degree_key",
            "class_standing_key",
            "enrollment_term_key",
        ),
        "dim_term_snapshots": term_dim,
    }
    facts = {
        "fact_application_snapshots": application_fact,
        "fact_course_offering_snapshots": offering_fact,
        "fact_enrollment_snapshots": enrollment_fact,
    }
    return dimensions, facts


def _write_clickhouse(frame: DataFrame, table: str) -> None:
    (
        frame.write.format("jdbc")
        .option("url", WAREHOUSE_JDBC_URL)
        .option("dbtable", table)
        .option("user", WAREHOUSE_DB_USER)
        .option("password", WAREHOUSE_DB_PASSWORD)
        .option("driver", WAREHOUSE_DRIVER)
        .option("batchsize", str(WRITE_BATCH_SIZE))
        .mode("append")
        .save()
    )


def _with_snapshot_id(frame: DataFrame, snapshot_id: str) -> DataFrame:
    return frame.withColumn("snapshot_id", lit(snapshot_id)).select(
        "snapshot_id", *frame.columns
    )


FACT_TOTALS = {
    "fact_application_snapshots": "application_count",
    "fact_course_offering_snapshots": "capacity",
    "fact_enrollment_snapshots": "enrollment_count",
}


def _frame_expectation(table: str, frame: DataFrame, source_rows: int | None) -> tuple:
    key = DIMENSION_KEYS[table] if table in DIMENSION_KEYS else FACT_KEYS[table]
    measure = FACT_TOTALS.get(table)
    aggregates = [count(lit(1)), countDistinct(key)]
    if measure:
        aggregates.append(coalesce(spark_sum(measure), lit(0)))
    result = tuple(frame.agg(*aggregates).first())
    if result[0] != result[1]:
        raise RuntimeError(f"{table} contains duplicate or null grain keys.")
    if source_rows is not None and result[0] != source_rows:
        raise RuntimeError(f"{table} produced {result[0]} rows from {source_rows} source rows.")
    return result


def _validate_stored_snapshot(client: Any, snapshot_id: str, table: str, expected: tuple) -> None:
    key = DIMENSION_KEYS[table] if table in DIMENSION_KEYS else FACT_KEYS[table]
    measure = FACT_TOTALS.get(table)
    aggregate = f", sum({measure})" if measure else ""
    actual = client.query(
        f"SELECT count(), uniqExact({key}){aggregate} FROM {table} "
        "WHERE snapshot_id = {snapshot:String}",
        parameters={"snapshot": snapshot_id},
    ).first_row
    if tuple(actual) != expected:
        raise RuntimeError(
            f"ClickHouse reconciliation failed for {table}, snapshot {snapshot_id}: "
            f"expected {expected}, stored {actual}."
        )


def run(spark: SparkSession, client: Any, source_run_id: int) -> None:
    already_published = client.query(
        "SELECT count() FROM warehouse_model_loads WHERE source_run_id = {run:UInt64}",
        parameters={"run": source_run_id},
    ).first_row[0]
    if already_published:
        LOGGER.info(
            "Full staging run %s already has a published warehouse snapshot; skipping.",
            source_run_id,
        )
        return
    snapshot_started_at, batch_ids, source_counts = _snapshot_batches(spark, source_run_id)
    snapshot_id = str(uuid.uuid4())
    dimensions, facts = build_warehouse_frames(spark, batch_ids)
    frames = {**dimensions, **facts}
    # Persist the final plans once. The count, grain and measure checks share an action.
    for frame in frames.values():
        frame.persist(StorageLevel.DISK_ONLY)

    expected_sources = {
        "dim_applicant_snapshots": "Applicants",
        "dim_class_standing_snapshots": "ClassStandings",
        "dim_course_snapshots": "Courses",
        "dim_degree_snapshots": "Degrees",
        "dim_department_snapshots": "Departments",
        "dim_grade_snapshots": "Grades",
        "dim_student_snapshots": "Students",
        "dim_term_snapshots": "Terms",
        "fact_application_snapshots": "Applicants",
        "fact_course_offering_snapshots": "CourseOfferings",
        "fact_enrollment_snapshots": "CourseEnrollments",
    }
    try:
        expected = {
            table: _frame_expectation(table, frame, source_counts.get(expected_sources.get(table)))
            for table, frame in frames.items()
        }
        for table, frame in frames.items():
            LOGGER.info("Writing %s rows to %s", expected[table][0], table)
            if expected[table][0]:
                _write_clickhouse(_with_snapshot_id(frame, snapshot_id), table)
            _validate_stored_snapshot(client, snapshot_id, table, expected[table])

        load_columns = [
            "snapshot_id",
            "source_run_id",
            "snapshot_started_at",
            *DIMENSION_TABLES.values(),
            *FACT_TABLES.values(),
        ]
        load_values = [
            (
                snapshot_id,
                source_run_id,
                snapshot_started_at,
                *[expected[table][0] for table in DIMENSION_TABLES],
                *[expected[table][0] for table in FACT_TABLES],
            )
        ]
        client.insert("warehouse_model_loads", load_values, column_names=load_columns)
    finally:
        for frame in frames.values():
            frame.unpersist()

    LOGGER.info(
        "Published model snapshot %s from staging run %s",
        snapshot_id,
        source_run_id,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run-id", required=True, type=int)
    args = parser.parse_args()
    if not 0 < args.source_run_id < 2**63:
        parser.error("--source-run-id must be a positive PostgreSQL bigint.")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    spark = (
        SparkSession.builder.appName("students-staging-to-clickhouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        client = clickhouse_connect.get_client(
            host=os.environ["WAREHOUSE_HOST"],
            port=int(os.environ["WAREHOUSE_HTTP_PORT"]),
            database=os.environ["WAREHOUSE_DB_NAME"],
            username=WAREHOUSE_DB_USER,
            password=WAREHOUSE_DB_PASSWORD,
        )
        try:
            run(spark, client, args.source_run_id)
        finally:
            client.close()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
