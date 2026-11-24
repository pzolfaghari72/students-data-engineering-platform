"""Typed CDC projections and SQL for the incremental analytical warehouse."""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from warehouse_keys import pseudonymize_identifier


@dataclass(frozen=True)
class Source:
    keys: tuple[str, ...]
    fields: dict[str, str]


# Fields used by the analytical model. The relay also records key/position
# history for tables with no analytical attributes.
SOURCES = {
    "Applicants": Source(("ApplicantId",), {
        "ApplicantId": "String", "ApplicationType": "String", "ApplicationStatus": "String",
        "ApplicationReceivedDate": "Date", "EntryTerm": "String",
        "Gpa": "Decimal(3, 2)", "SatMathScore": "Int32", "SatReadingScore": "Int32",
        "SatWritingScore": "Int32", "EssayScore": "Int32", "AdmissionPoints": "Float64",
    }),
    "ClassStandings": Source(("ClassStandingCode",), {
        "ClassStandingCode": "String", "ClassStandingName": "String",
        "DegreeLevelCode": "String", "RequiredCredits": "Int32",
    }),
    "CourseEnrollments": Source(("CourseEnrollmentId",), {
        "CourseEnrollmentId": "UInt32", "CourseOfferingId": "UInt32",
        "StudentId": "String", "Grade": "String",
    }),
    "CourseOfferings": Source(("CourseOfferingId",), {
        "CourseOfferingId": "UInt32", "DepartmentCode": "String", "CourseNumber": "Int32",
        "TermCode": "String", "Section": "UInt32", "Capacity": "UInt16",
    }),
    "Courses": Source(("DepartmentCode", "CourseNumber"), {
        "DepartmentCode": "String", "CourseNumber": "Int32", "CourseTitle": "String",
        "Credits": "Decimal(3, 1)", "MaximumSectionSize": "Int32",
    }),
    "DegreeLevels": Source(("DegreeLevelCode",), {
        "DegreeLevelCode": "String", "DegreeLevelName": "String",
    }),
    "DegreeRequirements": Source(("RequirementId",), {}),
    "Degrees": Source(("DegreeId",), {
        "DegreeId": "Int32", "DepartmentCode": "String", "DegreeTypeCode": "String",
        "DegreeName": "String",
    }),
    "DegreeTypes": Source(("DegreeTypeCode",), {
        "DegreeTypeCode": "String", "DegreeTypeName": "String", "DegreeLevelCode": "String",
    }),
    "Departments": Source(("DepartmentCode",), {
        "DepartmentCode": "String", "DepartmentName": "String",
    }),
    "Grades": Source(("Grade",), {
        "Grade": "String", "Points": "Decimal(3, 1)", "IncludeInGpa": "UInt8",
    }),
    "Sessions": Source(("SessionCode",), {"SessionCode": "String", "SessionName": "String"}),
    "StudentProfiles": Source(("StudentId",), {}),
    "Students": Source(("StudentId",), {
        "StudentId": "String", "StudentStatus": "String", "DegreePursued": "Int32",
        "ClassStandingCode": "String", "EnrollmentTerm": "String",
    }),
    "Terms": Source(("TermCode",), {
        "TermCode": "String", "SessionCode": "String", "Year": "UInt16",
    }),
}

POSITION = re.compile(r"\d{25}:[01]:\d{25}:\d{20}")
LSN = re.compile(r"[0-9a-f]{8}:[0-9a-f]{8}:[0-9a-f]{4}")
ROW_COLUMNS = ["load_id", "generation_id", "position", "commit_position",
               "occurred_at", "baseline", "deleted", "payload_checksum"]


def narrow_image(table: str, key: dict, image: dict | None) -> dict:
    spec = SOURCES[table]
    if set(key) != set(spec.keys) or any(value is None for value in key.values()):
        raise ValueError(f"Unexpected {table} CDC key shape.")
    if image is not None and any(image.get(field) != value for field, value in key.items()):
        raise ValueError(f"{table} CDC image and key disagree.")
    if image is None:
        return key
    return {field: image[field] for field in spec.fields}


def typed_value(field: str, kind: str, value, secret: str):
    if value is None:
        return None
    if field in {"StudentId", "ApplicantId"}:
        entity = "student" if field == "StudentId" else "applicant"
        return pseudonymize_identifier(entity, int(value), secret)
    if kind.startswith("Decimal"):
        scale = int(kind.split(",")[1].strip(" )"))
        if isinstance(value, str):
            try:
                return Decimal(value).quantize(Decimal(1).scaleb(-scale))
            except ArithmeticError:
                # Kafka Connect JSON Converter encodes Decimal as base64 bytes.
                binary = base64.b64decode(value, validate=True)
                return Decimal(int.from_bytes(binary, "big", signed=True)).scaleb(-scale)
        return Decimal(str(value)).quantize(Decimal(1).scaleb(-scale))
    if kind == "Date":
        return date(1970, 1, 1) + timedelta(days=value) if isinstance(value, int) else date.fromisoformat(value)
    if kind == "String":
        if not isinstance(value, str):
            raise ValueError(f"Unexpected non-string CDC field {field}.")
        return value
    if kind == "Float64":
        return float(value)
    return int(value)


def project_row(event: tuple, secret: str, load_id: str, generation: str):
    table, version, lsn, time_ms, operation, image, checksum = event
    if not POSITION.fullmatch(version) or not LSN.fullmatch(lsn):
        raise ValueError("Invalid source position in relayed event.")
    spec = SOURCES[table]
    row = (
        load_id, generation, int(version.replace(":", "")), int(lsn.replace(":", ""), 16),
        datetime.fromtimestamp(time_ms / 1000, tz=timezone.utc),
        int(operation == "r"), int(operation == "d"), checksum,
        *(typed_value(name, kind, image.get(name), secret) for name, kind in spec.fields.items()),
    )
    return table, row


def table_name(table: str) -> str:
    return "inc_source_" + table.lower()


def _fields(names):
    return ", ".join(names)


def _merge_state(table, expressions, historical=False):
    spec = SOURCES[table]
    attributes = [name for name in spec.fields if name not in spec.keys]
    keys = _fields(spec.keys)
    value = _fields([*attributes, "deleted"])
    unpacked = _fields(f"state.{index} AS {name}" for index, name in enumerate([*attributes, "deleted"], 1))
    if historical:
        return f"""
SELECT {keys}, {unpacked}, commit_position, occurred_at, toUInt8(1) AS present
FROM (
    SELECT {keys}, commit_position, min(occurred_at) AS occurred_at,
           argMax(tuple({value}), position) AS state
    FROM {expressions}
    GROUP BY {keys}, commit_position
)"""
    current_fields = _fields([*spec.keys, *attributes])
    updated_fields = _fields([*spec.keys, *(
        f"state.{index} AS {name}" for index, name in enumerate(attributes, 1)
    )])
    return f"""
WITH changed AS (
    SELECT {keys}, argMax(tuple({value}), position) AS state
    FROM {expressions} WHERE baseline = 0 GROUP BY {keys}
)
SELECT {current_fields}, toUInt8(1) AS present, toUInt8(0) AS deleted
FROM {expressions}
WHERE baseline = 1 AND ({keys}) NOT IN (SELECT {keys} FROM changed)
UNION ALL
SELECT {updated_fields}, toUInt8(1) AS present, toUInt8(0) AS deleted
FROM changed WHERE state.{len(attributes) + 1} = 0
"""


@dataclass(frozen=True)
class Link:
    alias: str
    table: str
    parent: str
    fields: tuple[tuple[str, str], ...]
    optional: bool = False


@dataclass(frozen=True)
class Model:
    root: str
    key: str
    columns: dict[str, str]
    links: tuple[Link, ...] = ()


def key_sql(namespace, *fields):
    parts = _fields(f"toString({field})" for field in fields)
    return f"lower(hex(SHA256(concatWithSeparator(char(31), '{namespace}', {parts}))))"


def date_key(field):
    return f"toUInt32(formatDateTime({field}, '%Y%m%d'))"


def link(alias, table, parent, *columns, optional=False):
    return Link(alias, table, parent, tuple(
        (column, column) if isinstance(column, str) else column for column in columns
    ), optional)


DEPARTMENT = link("dep", "Departments", "r", "DepartmentCode")
DEGREE_TYPE = link("dtype", "DegreeTypes", "r", "DegreeTypeCode")
STANDING = link("standing", "ClassStandings", "r", "ClassStandingCode", optional=True)
GRADE = link("grade", "Grades", "r", "Grade", optional=True)
TERM = link("term", "Terms", "r", "TermCode")
COURSE = link("course", "Courses", "r", "DepartmentCode", "CourseNumber")

DIMENSIONS = {
    "dim_applicant": Model("Applicants", "applicant_key", {
        "applicant_key": "r.ApplicantId", "application_type": "r.ApplicationType",
        "application_status": "r.ApplicationStatus",
    }),
    "dim_class_standing": Model("ClassStandings", "class_standing_key", {
        "class_standing_key": key_sql("class-standing", "r.ClassStandingCode"),
        "class_standing_code": "r.ClassStandingCode", "class_standing_name": "r.ClassStandingName",
        "degree_level_code": "r.DegreeLevelCode", "degree_level_name": "level.DegreeLevelName",
        "required_credits": "r.RequiredCredits",
    }, (link("level", "DegreeLevels", "r", "DegreeLevelCode"),)),
    "dim_course": Model("Courses", "course_key", {
        "course_key": key_sql("course", "r.DepartmentCode", "r.CourseNumber"),
        "department_key": key_sql("department", "r.DepartmentCode"),
        "department_code": "r.DepartmentCode", "course_number": "r.CourseNumber",
        "course_title": "r.CourseTitle", "credits": "r.Credits",
        "maximum_section_size": "r.MaximumSectionSize",
    }, (DEPARTMENT,)),
    "dim_degree": Model("Degrees", "degree_key", {
        "degree_key": key_sql("degree", "r.DegreeId"), "degree_id": "r.DegreeId",
        "degree_name": "r.DegreeName", "department_key": key_sql("department", "r.DepartmentCode"),
        "department_code": "r.DepartmentCode", "degree_type_code": "r.DegreeTypeCode",
        "degree_type_name": "dtype.DegreeTypeName", "degree_level_code": "dtype.DegreeLevelCode",
        "degree_level_name": "level.DegreeLevelName",
    }, (DEPARTMENT, DEGREE_TYPE, link("level", "DegreeLevels", "dtype", "DegreeLevelCode"))),
    "dim_department": Model("Departments", "department_key", {
        "department_key": key_sql("department", "r.DepartmentCode"),
        "department_code": "r.DepartmentCode", "department_name": "r.DepartmentName",
    }),
    "dim_grade": Model("Grades", "grade_key", {
        "grade_key": key_sql("grade", "r.Grade"), "grade_code": "r.Grade",
        "grade_points": "r.Points", "include_in_gpa": "r.IncludeInGpa",
    }),
    "dim_student": Model("Students", "student_key", {
        "student_key": "r.StudentId", "student_status": "r.StudentStatus",
        "degree_key": key_sql("degree", "r.DegreePursued"),
        "class_standing_key": f"if(standing.present = 1 AND standing.deleted = 0, {key_sql('class-standing', 'standing.ClassStandingCode')}, NULL)",
        "enrollment_term_key": key_sql("term", "r.EnrollmentTerm"),
    }, (link("degree", "Degrees", "r", ("DegreePursued", "DegreeId")),
        STANDING, link("term", "Terms", "r", ("EnrollmentTerm", "TermCode")))),
    "dim_term": Model("Terms", "term_key", {
        "term_key": key_sql("term", "r.TermCode"), "term_code": "r.TermCode",
        "session_code": "r.SessionCode", "session_name": "session.SessionName", "year": "r.Year",
    }, (link("session", "Sessions", "r", "SessionCode"),)),
}

FACTS = {
    "fact_application": Model("Applicants", "applicant_key", {
        "applicant_key": "r.ApplicantId", "application_date_key": date_key("r.ApplicationReceivedDate"),
        "entry_term_key": key_sql("term", "r.EntryTerm"), "entry_term_code": "r.EntryTerm",
        "application_count": "toUInt8(1)", "gpa": "r.Gpa", "sat_math_score": "r.SatMathScore",
        "sat_reading_score": "r.SatReadingScore", "sat_writing_score": "r.SatWritingScore",
        "essay_score": "r.EssayScore", "admission_points": "r.AdmissionPoints",
    }, (link("term", "Terms", "r", ("EntryTerm", "TermCode")),)),
    "fact_course_offering": Model("CourseOfferings", "course_offering_id", {
        "course_offering_id": "r.CourseOfferingId",
        "course_key": key_sql("course", "r.DepartmentCode", "r.CourseNumber"),
        "department_key": key_sql("department", "r.DepartmentCode"), "department_code": "r.DepartmentCode",
        "term_key": key_sql("term", "r.TermCode"), "term_code": "r.TermCode",
        "section": "r.Section", "capacity": "r.Capacity", "offering_count": "toUInt8(1)",
    }, (COURSE, DEPARTMENT, TERM)),
    "fact_course_enrollment": Model("CourseEnrollments", "course_enrollment_id", {
        "course_enrollment_id": "r.CourseEnrollmentId", "student_key": "r.StudentId",
        "course_offering_id": "r.CourseOfferingId",
        "course_key": key_sql("course", "offering.DepartmentCode", "offering.CourseNumber"),
        "department_key": key_sql("department", "offering.DepartmentCode"),
        "department_code": "offering.DepartmentCode", "course_number": "offering.CourseNumber",
        "term_key": key_sql("term", "offering.TermCode"), "term_code": "offering.TermCode",
        "grade_key": f"if(grade.present = 1, {key_sql('grade', 'grade.Grade')}, NULL)",
        "grade_code": "r.Grade", "degree_key": key_sql("degree", "student.DegreePursued"),
        "class_standing_key": f"if(standing.present = 1, {key_sql('class-standing', 'standing.ClassStandingCode')}, NULL)",
        "credits": "course.Credits", "grade_points": "grade.Points",
        "include_in_gpa": "coalesce(grade.IncludeInGpa, toUInt8(0))",
        "enrollment_count": "toUInt8(1)",
    }, (link("offering", "CourseOfferings", "r", "CourseOfferingId"),
        link("student", "Students", "r", "StudentId"),
        link("course", "Courses", "offering", "DepartmentCode", "CourseNumber"),
        link("dep", "Departments", "offering", "DepartmentCode"),
        link("term", "Terms", "offering", "TermCode"),
        link("degree", "Degrees", "student", ("DegreePursued", "DegreeId")),
        link("standing", "ClassStandings", "student", "ClassStandingCode", optional=True),
        link("student_term", "Terms", "student", ("EnrollmentTerm", "TermCode")), GRADE)),
}

VIEW_NAMES = {*DIMENSIONS, *FACTS, "dim_date"}


def _join_terms(relationship):
    return [f"{relationship.parent}.{a} = {relationship.alias}.{b}" for a, b in relationship.fields]


def _current_model(model):
    columns = _fields(f"{expr} AS {name}" for name, expr in model.columns.items())
    joins = "\n".join(
        f"{'LEFT' if item.optional else 'INNER'} JOIN {table_name(item.table)}_current AS {item.alias}"
        f" ON {' AND '.join(_join_terms(item))}" for item in model.links
    )
    return f"SELECT {columns}\nFROM {table_name(model.root)}_current AS r\n{joins}"


def _time_points(model):
    root_keys = SOURCES[model.root].keys
    keys = _fields(f"r.{key} AS {key}" for key in root_keys)
    history = table_name(model.root) + "_history"
    yield f"SELECT {keys}, commit_position, occurred_at FROM {history} AS r"
    relationships = {item.alias: item for item in model.links}
    for item in model.links:
        chain = [item]
        while chain[-1].parent != "r":
            chain.append(relationships[chain[-1].parent])
        joins = "\n".join(
            f"INNER JOIN {table_name(step.table)}_history AS {step.alias}"
            f" ON {' AND '.join(_join_terms(step))}" for step in reversed(chain)
        )
        yield f"SELECT {keys}, {item.alias}.commit_position AS commit_position, {item.alias}.occurred_at AS occurred_at FROM {history} AS r\n{joins}"


def _dimension_history(model):
    keys = SOURCES[model.root].keys
    columns = _fields(f"{expression} AS {name}" for name, expression in model.columns.items())
    root = table_name(model.root) + "_history"
    deletion = " OR ".join(["r.deleted = 1", *(
        f"({item.alias}.present = 0 OR {item.alias}.deleted = 1)"
        for item in model.links if not item.optional
    )])
    if model.links:
        group = _fields([*keys, "commit_position"])
        points = " UNION ALL ".join(_time_points(model))
        root_join = " AND ".join(f"p.{key} = r.{key}" for key in keys)
        joins = "\n".join(
            f"ASOF LEFT JOIN {table_name(item.table)}_history AS {item.alias}"
            f" ON {' AND '.join(_join_terms(item))} AND p.commit_position >= {item.alias}.commit_position"
            for item in model.links
        )
        values = f"""
SELECT {columns}, p.commit_position AS valid_from_position, p.occurred_at AS valid_from,
       toUInt8({deletion}) AS is_deleted
FROM (SELECT {group}, min(occurred_at) AS occurred_at FROM ({points}) GROUP BY {group}) AS p
ASOF INNER JOIN {root} AS r ON {root_join} AND p.commit_position >= r.commit_position
{joins}"""
    else:
        values = f"""
SELECT {columns}, r.commit_position AS valid_from_position, r.occurred_at AS valid_from,
       r.deleted AS is_deleted FROM {root} AS r"""
    key = model.key
    attributes = _fields(name for name in model.columns if name != key)
    return f"""
WITH values_at_position AS ({values}),
normalized AS (
    SELECT *, if(is_deleted = 1, 'deleted', toJSONString(tuple({attributes}))) AS signature
    FROM values_at_position
),
boundaries AS (
    SELECT *, lagInFrame(signature, 1, '') OVER timeline AS previous
    FROM normalized
    WINDOW timeline AS (PARTITION BY {key} ORDER BY valid_from_position)
),
versions AS (
    SELECT *, leadInFrame(toNullable(valid_from), 1) OVER timeline AS valid_to,
              leadInFrame(toNullable(valid_from_position), 1) OVER timeline AS valid_to_position
    FROM boundaries WHERE signature != previous
    WINDOW timeline AS (PARTITION BY {key} ORDER BY valid_from_position
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
)
SELECT {_fields(model.columns)}, {key_sql('version', key, 'valid_from_position')} AS version_key,
       valid_from, valid_to, valid_from_position, valid_to_position,
       isNull(valid_to_position) AS is_current, is_deleted
FROM versions
"""


def ddl():
    yield """
CREATE TABLE IF NOT EXISTS incremental_publications (
    generation_id String,
    load_id String,
    through_batch UInt64,
    source_checkpoint String,
    baseline_lsn String,
    key_identity String,
    normalized_rows UInt64,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
) ENGINE = MergeTree ORDER BY (published_at, load_id)
"""
    yield """
CREATE OR REPLACE VIEW incremental_publication_status SQL SECURITY DEFINER AS
SELECT generation_id, load_id, through_batch, source_checkpoint, baseline_lsn, key_identity, normalized_rows, published_at
FROM incremental_publications ORDER BY published_at DESC, through_batch DESC, load_id DESC LIMIT 1
"""
    yield """
CREATE OR REPLACE VIEW incremental_visible_loads AS
SELECT load_id
FROM incremental_publications
WHERE generation_id IN (SELECT generation_id FROM incremental_publication_status)
"""
    for table, spec in SOURCES.items():
        if not spec.fields:
            continue
        name = table_name(table)
        types = _fields(f"{field} {kind if field in spec.keys else f'Nullable({kind})'}" for field, kind in spec.fields.items())
        yield f"""
CREATE TABLE IF NOT EXISTS {name} (
    load_id String, generation_id String, position UInt256, commit_position UInt128,
    occurred_at DateTime64(3, 'UTC'), baseline UInt8, deleted UInt8, payload_checksum UInt64,
    {types}
) ENGINE = MergeTree ORDER BY (generation_id, baseline, {_fields(spec.keys)}, position, load_id)
"""
        yield f"""
CREATE OR REPLACE VIEW {name}_visible AS
SELECT * FROM {name}
WHERE generation_id IN (SELECT generation_id FROM incremental_publication_status)
  AND load_id IN (SELECT load_id FROM incremental_visible_loads)
"""
        yield f"CREATE OR REPLACE VIEW {name}_current AS\n{_merge_state(table, name + '_visible')}"
        if table not in {"CourseEnrollments", "CourseOfferings"}:
            yield f"CREATE OR REPLACE VIEW {name}_history AS\n{_merge_state(table, name + '_visible', historical=True)}"

    for name, model in {**DIMENSIONS, **FACTS}.items():
        yield f"CREATE OR REPLACE VIEW incremental_{name} SQL SECURITY DEFINER AS\n{_current_model(model)}"
        if name in DIMENSIONS:
            yield f"CREATE OR REPLACE VIEW incremental_{name}_history SQL SECURITY DEFINER AS\n{_dimension_history(model)}"
    yield f"""
CREATE OR REPLACE VIEW incremental_dim_date SQL SECURITY DEFINER AS
WITH dates AS (
    SELECT minOrNull(ApplicationReceivedDate) AS first_date,
           maxOrNull(ApplicationReceivedDate) AS last_date FROM {table_name('Applicants')}_current
),
calendar AS (
    SELECT addDays(first_date, arrayJoin(range(toUInt32(coalesce(dateDiff('day', first_date, last_date) + 1, 0))))) AS calendar_date
    FROM dates
)
SELECT {date_key('calendar_date')} AS date_key, calendar_date,
       toYear(calendar_date) AS calendar_year, toQuarter(calendar_date) AS calendar_quarter,
       toMonth(calendar_date) AS calendar_month, formatDateTime(calendar_date, '%M') AS month_name,
       toDayOfMonth(calendar_date) AS day_of_month, toDayOfWeek(calendar_date, 2) + 1 AS day_of_week,
       formatDateTime(calendar_date, '%a') AS day_name
FROM calendar
"""


if __name__ == "__main__":
    print(";\n\n".join(query.strip() for query in ddl()) + ";")
