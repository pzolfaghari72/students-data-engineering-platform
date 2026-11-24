"""Exercise the incremental job in disposable PostgreSQL and ClickHouse databases.

Run in the Airflow image with the existing staging and warehouse connection
variables. Fixtures use synthetic IDs, never the SQL Server source or Kafka.
"""

from contextlib import ExitStack, closing
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4

import clickhouse_connect
import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark"))
sys.path.insert(0, str(ROOT / "airflow/dags"))

from incremental_model import ddl
from incremental_warehouse import (
    ClickHousePublisher, PostgresRelay, SourceLedger, SparkProjection, run, verify_publication,
)
from stream_cdc import ClickHouseCDC, event_row

GENERATION = "incremental-fixture"
CHECKPOINT = "fixture-checkpoint"
SECRET = "fixture-hmac-secret"
KEYS = {
    "Applicants": ("ApplicantId",), "ClassStandings": ("ClassStandingCode",),
    "CourseEnrollments": ("CourseEnrollmentId",), "CourseOfferings": ("CourseOfferingId",),
    "Courses": ("DepartmentCode", "CourseNumber"), "DegreeLevels": ("DegreeLevelCode",),
    "DegreeRequirements": ("RequirementId",), "Degrees": ("DegreeId",),
    "DegreeTypes": ("DegreeTypeCode",), "Departments": ("DepartmentCode",),
    "Grades": ("Grade",), "Sessions": ("SessionCode",), "StudentProfiles": ("StudentId",),
    "Students": ("StudentId",), "Terms": ("TermCode",),
}
STUDENT = dict(StudentId=1, StudentStatus="A", DegreePursued=1,
               ClassStandingCode="FR", EnrollmentTerm="FA2004")
ENROLLMENT = dict(CourseEnrollmentId=1, CourseOfferingId=1, StudentId=1, Grade="A")
BASELINE = [
    ("Applicants", dict(ApplicantId=1, ApplicationType="N", ApplicationStatus="A",
                        ApplicationReceivedDate=12_700, EntryTerm="FA2004", Gpa="AXw=",
                        SatMathScore=None, SatReadingScore=550, SatWritingScore=None,
                        EssayScore=6, AdmissionPoints=8.5)),
    ("ClassStandings", dict(ClassStandingCode="FR", ClassStandingName="Freshman",
                            DegreeLevelCode="B", RequiredCredits=0)),
    ("CourseEnrollments", ENROLLMENT),
    ("CourseOfferings", dict(CourseOfferingId=1, DepartmentCode="CS", CourseNumber=101,
                            TermCode="FA2004", Section=1, Capacity=20)),
    ("Courses", dict(DepartmentCode="CS", CourseNumber=101, CourseTitle="Foundations",
                     Credits="Hg==", MaximumSectionSize=None)),
    ("DegreeLevels", dict(DegreeLevelCode="B", DegreeLevelName="Bachelor")),
    ("DegreeRequirements", dict(RequirementId=1)),
    ("Degrees", dict(DegreeId=1, DepartmentCode="CS", DegreeTypeCode="BS", DegreeName="Computing")),
    ("DegreeTypes", dict(DegreeTypeCode="BS", DegreeTypeName="Science", DegreeLevelCode="B")),
    ("Departments", dict(DepartmentCode="CS", DepartmentName="Computing")),
    ("Grades", dict(Grade="A", Points="KA==", IncludeInGpa=True)),
    ("Sessions", dict(SessionCode="FA", SessionName="Fall")),
    ("StudentProfiles", dict(StudentId=1)),
    ("Students", STUDENT), ("Students", {**STUDENT, "StudentId": 2}),
    ("Terms", dict(TermCode="FA2004", SessionCode="FA", Year=2004)),
]


def lsn(index):
    return f"00000001:00000000:{index:04x}"


class FixtureSource:
    def __init__(self, client):
        self.writer = ClickHouseCDC(client, GENERATION)
        self.offset = 0
        self.batch = 0
        self.offsets = {"fixture:0": 0}
        self.writer.client.insert(
            "cdc_publications", [[GENERATION, lsn(1)]],
            column_names=["generation_id", "baseline_lsn"],
        )

    def send(self, events):
        from datetime import datetime, timezone

        rows = []
        for table, image, op, position in events:
            rows.append(event_row(GENERATION, self.batch, {
                "kafka_topic": "fixture", "kafka_partition": 0, "kafka_offset": self.offset,
                "source_table": table, "source_key": {key: image[key] for key in KEYS[table]},
                "operation": op, "before_image": image if op == "d" else None,
                "after_image": None if op == "d" else image, "source_commit_lsn": lsn(position),
                "source_change_lsn": lsn(position), "source_event_serial_no": "1",
                "source_snapshot_state": "true" if op == "r" else "false",
                "source_timestamp": datetime.fromtimestamp(1_700_000_000 + position, timezone.utc),
            }))
            self.offset += 1
        self.offsets = self.writer.seal(
            CHECKPOINT, self.batch, rows, self.offsets, {"fixture:0": self.offset},
        )
        self.batch += 1


def assert_metrics(ch, *, enrollments, students, capacity):
    assert ch.query("""
SELECT count(), uniqExact(student_key) FROM incremental_fact_course_enrollment
""").first_row == (enrollments, students)
    assert ch.query("SELECT sum(capacity) FROM incremental_fact_course_offering").first_row == (capacity,)
    assert ch.query("SELECT count() FROM incremental_fact_application").first_row == (1,)


def check_dashboard(client):
    spec = importlib.util.spec_from_file_location(
        "render_dashboard", ROOT / "scripts/render_incremental_dashboard.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dashboard = module.render(json.loads(
        (ROOT / "grafana/dashboards/enrollment-performance.json").read_text()
    ), preview=True)
    macros = re.compile(r"\$__conditionalAll\((.+? IN \(\$\{(term|department):singlequote\}\)), \$(?:term|department)\)")
    queries = [
        target["rawSql"] for panel in dashboard["panels"] for target in panel.get("targets", [])
    ]
    assert len(queries) == 18
    for variable in dashboard["templating"]["list"]:
        assert client.query(variable["query"]).result_rows
    for query in queries:
        all_scope = macros.sub("1", query)
        selected = macros.sub(lambda match: match[1].replace(
            "${" + match[2] + ":singlequote}", "'FA2004'" if match[2] == "term" else "'CS'",
        ), query)
        for sql_query in (all_scope, selected):
            assert "$" not in sql_query
            client.query(sql_query).result_rows


def fixture(ch_source, ch_model, pg, spark):
    src = FixtureSource(ch_source)
    source = SourceLedger(ch_source)
    relay = PostgresRelay(pg)
    publisher = ClickHousePublisher(ch_model)
    src.send([(table, image, "r", 1) for table, image in BASELINE])
    assert run(source, relay, publisher, spark) is None
    assert run(source, relay, publisher, spark, bootstrap=True).through == 0
    assert_metrics(ch_model, enrollments=1, students=1, capacity=20)
    verify_publication(source, publisher)
    for name, count in {
        "applicant": 1, "class_standing": 1, "course": 1, "date": 1, "degree": 1,
        "department": 1, "grade": 1, "student": 2, "term": 1,
    }.items():
        assert ch_model.query(f"SELECT count() FROM incremental_dim_{name}").first_row == (count,), name
        if name != "date":
            rows = ch_model.query(f"SELECT count() FROM incremental_dim_{name}_history WHERE is_current = 1 AND is_deleted = 0")
            assert rows.first_row == (count,), (name, rows.first_row)
    baseline_key = hashlib.sha256(b"course\x1fCS\x1f101").hexdigest()
    assert ch_model.query("SELECT course_key FROM incremental_dim_course").first_row == (baseline_key,)

    # Publish a partial candidate, then retry from the durable relay.
    src.send([
        ("CourseEnrollments", {**ENROLLMENT, "CourseEnrollmentId": 2, "StudentId": 2}, "c", 2),
        ("Students", {**STUDENT, "StudentStatus": "B"}, "u", 3),
    ])

    class PartialWriter(ClickHousePublisher):
        def stage(self, table, rows):
            super().stage(table, rows)
            raise OSError("injected failure after ClickHouse write")

    try:
        run(source, relay, PartialWriter(ch_model), spark)
        raise AssertionError("Partial write should fail.")
    except OSError:
        pass
    assert publisher.latest()[1] == 0
    assert_metrics(ch_model, enrollments=1, students=1, capacity=20)
    assert run(source, relay, publisher, spark).through == 1
    assert_metrics(ch_model, enrollments=2, students=2, capacity=20)
    assert ch_model.query("""
SELECT student_status, is_current
FROM incremental_dim_student_history WHERE student_key IN (
    SELECT student_key FROM incremental_dim_student WHERE student_status = 'B'
) ORDER BY valid_from_position
""").result_rows == [("A", 0), ("B", 1)]

    # Replayed insertion, late source version, unchanged attributes, and a parent
    # lookup affecting both history and previously published fact rows.
    src.send([
        ("CourseEnrollments", {**ENROLLMENT, "CourseEnrollmentId": 2, "StudentId": 2}, "c", 2),
        ("Students", {**STUDENT, "StudentStatus": "X"}, "u", 2),
        ("Students", {**STUDENT, "StudentStatus": "B"}, "u", 4),
        ("DegreeLevels", dict(DegreeLevelCode="B", DegreeLevelName="Undergraduate"), "u", 4),
        ("Grades", dict(Grade="A", Points="Iw==", IncludeInGpa=True), "u", 5),
        ("Courses", dict(DepartmentCode="CS", CourseNumber=101, CourseTitle="Foundations",
                         Credits="KA==", MaximumSectionSize=None), "u", 5),
        ("CourseOfferings", {**BASELINE[3][1], "Capacity": 25}, "u", 5),
    ])
    assert run(source, relay, publisher, spark).through == 2
    assert_metrics(ch_model, enrollments=2, students=2, capacity=25)
    assert ch_model.query("""
SELECT student_status, is_current FROM incremental_dim_student_history WHERE student_key IN (
    SELECT student_key FROM incremental_dim_student WHERE student_status = 'B'
) ORDER BY valid_from_position
""").result_rows == [("A", 0), ("X", 0), ("B", 1)]
    assert ch_model.query("""
SELECT degree_level_name, is_current FROM incremental_dim_degree_history ORDER BY valid_from_position
""").result_rows == [("Bachelor", 0), ("Undergraduate", 1)]
    assert ch_model.query("SELECT toString(credits), toString(grade_points) FROM incremental_fact_course_enrollment").result_rows == [
        ("4", "3.5"), ("4", "3.5"),
    ]
    check_dashboard(ch_model)

    src.send([
        ("CourseEnrollments", {**ENROLLMENT, "CourseEnrollmentId": 2, "StudentId": 2}, "d", 6),
        ("Students", {**STUDENT, "StudentId": 2}, "d", 6),
    ])
    assert run(source, relay, publisher, spark).through == 3
    assert_metrics(ch_model, enrollments=1, students=1, capacity=25)
    assert ch_model.query("SELECT sum(is_deleted) FROM incremental_dim_student_history WHERE is_current = 1").first_row == (1,)

    class LostReceipt(ClickHousePublisher):
        def publish(self, cut, load_id, count):
            super().publish(cut, load_id, count)
            raise OSError("injected lost publication response")

    src.send([("Students", {**STUDENT, "StudentId": 2}, "c", 7)])
    try:
        run(source, relay, LostReceipt(ch_model), spark)
        raise AssertionError("Lost publication receipt should fail.")
    except OSError:
        pass
    assert run(source, relay, publisher, spark) is None
    assert publisher.latest()[1] == 4
    assert ch_model.query("SELECT count() FROM incremental_dim_student").first_row == (2,)
    assert ch_model.query("""
SELECT is_deleted, is_current FROM incremental_dim_student_history WHERE student_key NOT IN (
    SELECT student_key FROM incremental_dim_student WHERE student_status = 'B'
) ORDER BY valid_from_position
""").result_rows == [(0, 0), (1, 0), (0, 1)]

    src.send([])
    assert run(source, relay, publisher, spark).through == 5
    assert run(source, relay, publisher, spark) is None
    assert_metrics(ch_model, enrollments=1, students=1, capacity=25)
    verify_publication(source, publisher)
    with pg, pg.cursor() as cursor:
        cursor.execute("SELECT status, count(*) FROM incremental_cdc.runs GROUP BY status ORDER BY status")
        assert cursor.fetchall() == [("FAILED", 1), ("PUBLISHED", 6)]
        cursor.execute("SELECT count(*) FROM incremental_cdc.relay_batches")
        assert cursor.fetchone() == (6,)


def main():
    suffix = uuid4().hex[:10]
    source_db, model_db = f"inc_source_test_{suffix}", f"inc_model_test_{suffix}"
    pg_db = f"inc_test_{suffix}"
    config = os.environ
    pg_options = dict(host=config["STAGING_DB_HOST"], port=config["STAGING_DB_PORT"],
                      user=config["STAGING_DB_USER"], password=config["STAGING_DB_PASSWORD"])
    ch_options = dict(host=config["WAREHOUSE_HOST"], port=int(config["WAREHOUSE_HTTP_PORT"]),
                      username=config["WAREHOUSE_DB_USER"], password=config["WAREHOUSE_DB_PASSWORD"])
    spark = SparkProjection(config, SECRET)
    with ExitStack() as stack:
        admin = stack.enter_context(closing(psycopg2.connect(dbname=config["STAGING_DB_NAME"], **pg_options)))
        admin.autocommit = True
        cursor = stack.enter_context(admin.cursor())
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(pg_db)))
        stack.callback(cursor.execute, sql.SQL("DROP DATABASE {}").format(sql.Identifier(pg_db)))
        pg = stack.enter_context(closing(psycopg2.connect(dbname=pg_db, **pg_options)))
        with pg, pg.cursor() as pg_cursor:
            pg_cursor.execute((ROOT / "db/staging_postgres/staging_ddl/incremental_cdc.sql").read_text())

        ch_admin = stack.enter_context(clickhouse_connect.get_client(database=config["WAREHOUSE_DB_NAME"], **ch_options))
        for ch_db in (source_db, model_db):
            ch_admin.command(f"CREATE DATABASE {ch_db}")
            stack.callback(ch_admin.command, f"DROP DATABASE {ch_db} SYNC")
        ch_source = stack.enter_context(clickhouse_connect.get_client(database=source_db, **ch_options))
        ch_model = stack.enter_context(clickhouse_connect.get_client(database=model_db, **ch_options))
        for statement in (ROOT / "db/warehouse_clickhouse/cdc_warehouse_ddl.sql").read_text().split(";"):
            if statement.strip():
                ch_source.command(statement)
        for statement in ddl():
            ch_model.command(statement)
        stack.callback(spark.close)
        fixture(ch_source, ch_model, pg, spark)
    print("Incremental PostgreSQL/Spark/ClickHouse fixture and all 18 dashboard queries passed.")


if __name__ == "__main__":
    main()
