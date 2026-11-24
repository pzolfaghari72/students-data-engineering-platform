"""Relay sealed CDC events and publish incremental analytical changes."""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import closing
from dataclasses import dataclass
import json
import logging
import os
import re
import sys
from uuid import uuid4

from cdc_events import fingerprint
from incremental_model import ROW_COLUMNS, SOURCES, ddl, narrow_image, project_row, table_name
from warehouse_keys import pseudonymize_identifier

LOGGER = logging.getLogger("students_incremental")
# Session lock shared by the CLI and the Airflow task in the staging database.
PUBLISHER_LOCK = 672902598407640412
PAGE_SIZE = 10_000
MAX_ROUTINE_SEALS = 32
CHECKSUM_MASK = (1 << 64) - 1
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class Seal:
    batch: int
    checkpoint: str
    digest: str
    records: int


@dataclass(frozen=True)
class Cut:
    generation: str
    baseline: str
    checkpoint: str
    after: int
    through: int
    key_identity: str


def optional_row(result):
    return next(iter(result.result_rows), None)


class SourceLedger:
    def __init__(self, client):
        self.client = client

    def active(self):
        return optional_row(self.client.query(
            "SELECT generation_id, baseline_lsn FROM cdc_active_generation"
        ))

    def seals(self, generation, after, limit=None):
        sql = """
SELECT batch_id, groupUniqArray(tuple(checkpoint_id, toString(record_digest), records))
FROM cdc_batch_commits WHERE generation_id = {generation:String} AND batch_id > {after:Int64}
GROUP BY batch_id ORDER BY batch_id
"""
        if limit:
            sql += " LIMIT {limit:UInt32}"
        rows = self.client.query(sql, parameters={
            "generation": generation, "after": after, "limit": limit,
        }).result_rows
        result = []
        for batch, versions in rows:
            if len(versions) != 1 or batch != after + len(result) + 1:
                raise RuntimeError("The sealed CDC input has a gap or a conflicting batch seal.")
            result.append(Seal(batch, *versions[0]))
        if len({seal.checkpoint for seal in result}) > 1:
            raise RuntimeError("CDC checkpoint changed within the selected generation.")
        return result

    def events(self, generation, seal):
        return self.client.query("""
SELECT DISTINCT kafka_topic, kafka_partition, kafka_offset, toString(record_hash),
       source_table, source_key, source_version, commit_lsn, source_time_ms, operation, after_image
FROM cdc_events
WHERE generation_id = {generation:String} AND batch_id = {batch:UInt64}
ORDER BY kafka_topic, kafka_partition, kafka_offset
""", parameters={"generation": generation, "batch": seal.batch}).result_rows

    def analytical_counts(self, generation, through, baseline):
        return self.client.query("""
SELECT source_table, countIf(state.1 != 'd') AS rows,
       sumIf(state.2, state.1 != 'd') AS seats,
       uniqExactIf(state.3, source_table = 'CourseEnrollments' AND state.1 != 'd') AS students
FROM (
    SELECT source_table, source_key,
           argMax(tuple(operation, JSONExtractUInt(after_image, 'Capacity'),
                        JSONExtractUInt(after_image, 'StudentId')),
                  tuple(source_version, kafka_topic, kafka_partition, kafka_offset)) AS state
    FROM cdc_events
    WHERE generation_id = {generation:String} AND batch_id <= {through:UInt64}
      AND batch_id IN (
          SELECT batch_id FROM cdc_batch_commits WHERE generation_id = {generation:String}
            AND batch_id <= {through:UInt64}
      )
      AND (operation = 'r' AND commit_lsn = {baseline:String}
           OR operation IN ('c', 'u', 'd') AND commit_lsn >= {baseline:String})
    GROUP BY source_table, source_key
)
GROUP BY source_table
""", parameters={"generation": generation, "through": through, "baseline": baseline}).result_rows


def checked_events(rows, seal, baseline):
    """Validate the transport seal before selecting/deduplicating logical changes."""
    positions = {}
    eligible = {}
    for topic, partition, offset, hash_, table, key, version, lsn, time_ms, op, image in rows:
        transport = (topic, partition, offset)
        if transport in positions:
            raise RuntimeError("One transport position has conflicting CDC records.")
        positions[transport] = hash_
        if not (op == "r" and lsn == baseline or op in {"c", "u", "d"} and lsn >= baseline):
            continue
        fields = narrow_image(table, json.loads(key), None if op == "d" else json.loads(image))
        identity = (table, key, version)
        digest = fingerprint((identity, lsn, op, fields))
        record = (*identity, lsn, time_ms, op, fields, digest)
        previous = eligible.setdefault(identity, record)
        if previous[-1] != digest:
            raise RuntimeError("One source position has conflicting analytical row images.")
    if len(positions) != seal.records or fingerprint(sorted(positions.items())) != seal.digest:
        raise RuntimeError(f"CDC batch {seal.batch} does not match its transport seal.")
    return list(eligible.values())


class PostgresRelay:
    def __init__(self, connection):
        self.connection = connection

    def lock(self):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (PUBLISHER_LOCK,))
            return cursor.fetchone()[0]

    def unlock(self):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_unlock(%s)", (PUBLISHER_LOCK,))

    def recover_runs(self, publisher):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("SELECT load_id FROM incremental_cdc.runs WHERE status = 'RUNNING'")
            for (load_id,) in cursor.fetchall():
                published = publisher.find(load_id)
                cursor.execute("""
UPDATE incremental_cdc.runs SET status = %s, normalized_rows = %s, error_message = %s,
    completed_at = coalesce(%s, clock_timestamp()) WHERE load_id = %s
""", (
                    "PUBLISHED" if published else "FAILED",
                    published[0] if published else None,
                    None if published else "Previous publisher stopped before publication.",
                    published[1] if published else None, load_id,
                ))

    def start(self, load_id, cut):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("""
INSERT INTO incremental_cdc.runs
    (load_id, generation_id, source_checkpoint, after_batch, through_batch, status)
VALUES (%s, %s, %s, %s, %s, 'RUNNING')
""", (load_id, cut.generation, cut.checkpoint, cut.after, cut.through))

    def finish(self, load_id, rows, error=None):
        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("""
UPDATE incremental_cdc.runs SET status = %s, normalized_rows = %s, error_message = %s,
    completed_at = clock_timestamp() WHERE load_id = %s
""", ("FAILED" if error else "PUBLISHED", rows, error, load_id))

    def receive(self, source, cut, seal):
        from psycopg2.extras import Json, execute_values

        with self.connection, self.connection.cursor() as cursor:
            cursor.execute("""
SELECT source_checkpoint, source_digest, transport_records
FROM incremental_cdc.relay_batches WHERE generation_id = %s AND batch_id = %s
""", (cut.generation, seal.batch))
            existing = cursor.fetchone()
            if existing is not None:
                if existing != (seal.checkpoint, seal.digest, seal.records):
                    raise RuntimeError("A previously relayed batch no longer matches its seal.")
                return

            events = checked_events(source.events(cut.generation, seal), seal, cut.baseline)
            cursor.execute("""
INSERT INTO incremental_cdc.relay_batches VALUES (%s, %s, %s, %s, %s, DEFAULT)
""", (cut.generation, seal.batch, seal.checkpoint, seal.digest, seal.records))
            if not events:
                return
            written = execute_values(cursor, """
INSERT INTO incremental_cdc.history
    (generation_id, first_batch, source_table, source_key, source_version, commit_lsn,
     source_time_ms, operation, image, payload_hash)
VALUES %s
ON CONFLICT (generation_id, source_table, source_key, source_version) DO UPDATE
SET payload_hash = EXCLUDED.payload_hash
WHERE incremental_cdc.history.payload_hash = EXCLUDED.payload_hash
RETURNING 1
""", [
                (cut.generation, seal.batch, *record[:6], Json(record[6]), record[7])
                for record in events
            ], page_size=1000, fetch=True)
            if len(written) != len(events):
                raise RuntimeError("A replay disagrees with durable CDC history.")

    def pages(self, cut):
        with self.connection, self.connection.cursor(name="incremental_history") as cursor:
            cursor.execute("""
SELECT source_table, source_version, commit_lsn, source_time_ms, operation, image, payload_hash
FROM incremental_cdc.history
WHERE generation_id = %s AND first_batch > %s AND first_batch <= %s
ORDER BY first_batch, source_table, source_key, source_version
""", (cut.generation, cut.after, cut.through))
            while rows := cursor.fetchmany(PAGE_SIZE):
                yield [
                    (*row[:-1], int(row[-1][:16], 16)) for row in rows if SOURCES[row[0]].fields
                ]


class ClickHousePublisher:
    def __init__(self, client):
        self.client = client

    def latest(self):
        return optional_row(self.client.query("""
SELECT generation_id, through_batch, source_checkpoint, baseline_lsn, key_identity
FROM incremental_publication_status
"""))

    def find(self, load_id):
        return optional_row(self.client.query("""
SELECT normalized_rows, published_at FROM incremental_publications WHERE load_id = {load_id:String}
""", parameters={"load_id": load_id}))

    def stage(self, table, rows):
        self.client.insert(
            table_name(table), rows,
            column_names=[*ROW_COLUMNS, *SOURCES[table].fields],
        )

    def verify(self, cut, load_id, expected):
        for table, (count, checksum) in expected.items():
            result = self.client.query(f"""
SELECT count(), sumWithOverflow(payload_checksum)
FROM {table_name(table)}
WHERE generation_id = {{generation:String}} AND load_id = {{load:String}}
""", parameters={"generation": cut.generation, "load": load_id}).first_row
            if tuple(result) != (count, checksum):
                raise RuntimeError(f"Read-back validation failed for {table}.")

    def publish(self, cut, load_id, count):
        self.client.insert("incremental_publications", [[
            cut.generation, load_id, cut.through, cut.checkpoint, cut.baseline, cut.key_identity, count,
        ]], column_names=[
            "generation_id", "load_id", "through_batch", "source_checkpoint",
            "baseline_lsn", "key_identity", "normalized_rows",
        ])


class SparkProjection:
    def __init__(self, config, load_secret):
        self.config = config
        self.secret = load_secret
        self.key_identity = pseudonymize_identifier("incremental-key-check", 0, load_secret)
        self.session = None

    def __call__(self, rows, load_id, generation):
        if not rows:
            return []
        if self.session is None:
            from pyspark.sql import SparkSession
            self.session = (
                SparkSession.builder.appName("students-incremental-warehouse")
                .master(self.config.get("SPARK_BATCH_MASTER", "local[2]"))
                .config("spark.driver.memory", self.config.get("SPARK_BATCH_DRIVER_MEMORY", "768m"))
                .config("spark.ui.enabled", "false").getOrCreate()
            )
            for module in ("incremental_model.py", "warehouse_keys.py"):
                self.session.sparkContext.addPyFile(os.path.join(os.path.dirname(__file__), module))
        secret = self.secret
        return self.session.sparkContext.parallelize(rows, min(2, len(rows))).map(
            lambda row: project_row(row, secret, load_id, generation)
        ).collect()

    def close(self):
        if self.session:
            self.session.stop()


def verify_publication(source, publisher):
    latest = publisher.latest()
    if latest is None:
        raise RuntimeError("Bootstrap the incremental warehouse before verification.")
    generation, through, _, baseline, _ = latest
    expected = {table: (rows, seats, students) for table, rows, seats, students in
                source.analytical_counts(generation, through, baseline)}
    models = {
        "dim_applicant": "Applicants", "dim_class_standing": "ClassStandings",
        "dim_course": "Courses", "dim_degree": "Degrees", "dim_department": "Departments",
        "dim_grade": "Grades", "dim_student": "Students", "dim_term": "Terms",
        "fact_application": "Applicants", "fact_course_offering": "CourseOfferings",
        "fact_course_enrollment": "CourseEnrollments",
    }
    for model, source_table in models.items():
        source_counts = expected.get(source_table, (0, 0, 0))
        expressions, expected_counts = "count()", source_counts[:1]
        if model == "fact_course_offering":
            expressions += ", sum(capacity)"
            expected_counts = source_counts[:2]
        elif model == "fact_course_enrollment":
            expressions += ", uniqExact(student_key)"
            expected_counts = source_counts[::2]
        actual = publisher.client.query(f"SELECT {expressions} FROM incremental_{model}").first_row
        if tuple(actual) != expected_counts:
            raise RuntimeError(f"{model} at sealed batch {through}: expected {expected_counts}, got {actual}.")
        LOGGER.info("%s at sealed batch %d: %s", model, through, actual)
    if publisher.latest() != latest:
        raise RuntimeError("Publication advanced during verification. Try again while the DAG is paused.")


def run(source, relay, publisher, project, *, bootstrap=False):
    if not relay.lock():
        LOGGER.info("An incremental publisher is already active.")
        return None
    try:
        return _run_locked(source, relay, publisher, project, bootstrap=bootstrap)
    finally:
        relay.unlock()


def _run_locked(source, relay, publisher, project, *, bootstrap):
    relay.recover_runs(publisher)
    active = source.active()
    if not active:
        LOGGER.info("No active CDC generation has completed bootstrap.")
        return None
    generation, baseline = active
    latest = publisher.latest()
    if latest and latest[0] == generation:
        after = latest[1]
        if baseline != latest[3]:
            raise RuntimeError("The source baseline changed within an already published generation.")
        if project.key_identity != latest[4]:
            raise RuntimeError("The warehouse HMAC key differs from the published generation.")
    elif not bootstrap:
        LOGGER.info("An explicit bootstrap is required for this active CDC generation.")
        return None
    else:
        after = -1

    seals = source.seals(generation, after, None if after < 0 else MAX_ROUTINE_SEALS)
    if not seals:
        LOGGER.info("No new sealed CDC batches.")
        return None
    if after >= 0 and latest[2] != seals[0].checkpoint:
        raise RuntimeError("The source checkpoint differs from the published incremental checkpoint.")
    cut = Cut(generation, baseline, seals[0].checkpoint, after, seals[-1].batch, project.key_identity)
    load_id = uuid4().hex
    relay.start(load_id, cut)
    phase = "relaying sealed CDC"
    try:
        for index, seal in enumerate(seals, 1):
            relay.receive(source, cut, seal)
            if index % 100 == 0:
                LOGGER.info("Relayed %d of %d selected CDC seals.", index, len(seals))
        phase = "projecting relayed rows"
        expected = {}
        for page in relay.pages(cut):
            grouped = defaultdict(list)
            for table, row in project(page, load_id, generation):
                grouped[table].append(row)
            for table, rows in grouped.items():
                publisher.stage(table, rows)
                count, checksum = expected.get(table, (0, 0))
                expected[table] = (count + len(rows), (checksum + sum(row[7] for row in rows)) & CHECKSUM_MASK)

        phase = "verifying ClickHouse writes"
        publisher.verify(cut, load_id, expected)
        if source.active() != active:
            raise RuntimeError("The active CDC generation changed before publication.")
        phase = "publishing the ClickHouse watermark"
        count = sum(rows for rows, _ in expected.values())
        publisher.publish(cut, load_id, count)
    except Exception as error:
        # A lost response can occur after publication. Recover that receipt at
        # the start of the next run rather than exposing a second candidate.
        if phase != "publishing the ClickHouse watermark":
            relay.finish(load_id, None, f"{phase}: {type(error).__name__}")
        raise
    relay.finish(load_id, count)
    LOGGER.info("Published %d analytical changes through sealed CDC batch %d.", count, cut.through)
    return cut


def main():
    import clickhouse_connect
    import psycopg2

    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--bootstrap", action="store_true", help="Initialize the active CDC generation.")
    action.add_argument("--install", action="store_true", help="Install additive DDL, with PostgreSQL SQL on stdin.")
    action.add_argument("--verify", action="store_true", help="Compare published model counts with the same sealed CDC cut.")
    parser.add_argument("--cdc-database", default=os.getenv("CDC_DB_NAME", "students_cdc"))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = os.environ
    warehouse_database = config["WAREHOUSE_DB_NAME"]
    if not all(IDENTIFIER.fullmatch(db) for db in (args.cdc_database, warehouse_database)):
        parser.error("Database names must be ClickHouse identifiers.")
    if args.cdc_database == warehouse_database:
        parser.error("The CDC and warehouse databases must be different.")
    secret = config["STUDENT_ID_HMAC_KEY"]
    if not secret:
        parser.error("Set the existing warehouse HMAC key.")

    clickhouse = dict(
        host=config["WAREHOUSE_HOST"], port=int(config["WAREHOUSE_HTTP_PORT"]),
        username=config["WAREHOUSE_DB_USER"], password=config["WAREHOUSE_DB_PASSWORD"],
    )
    spark = SparkProjection(config, secret)
    try:
        with (
            closing(psycopg2.connect(
                host=config["STAGING_DB_HOST"], port=config["STAGING_DB_PORT"],
                dbname=config["STAGING_DB_NAME"], user=config["STAGING_DB_USER"],
                password=config["STAGING_DB_PASSWORD"], connect_timeout=10,
            )) as pg,
            clickhouse_connect.get_client(database=args.cdc_database, **clickhouse) as cdc,
            clickhouse_connect.get_client(database=warehouse_database, **clickhouse) as warehouse,
        ):
            if args.install:
                migration = sys.stdin.read()
                if not migration.strip():
                    parser.error("Supply the incremental PostgreSQL migration on stdin.")
                with pg, pg.cursor() as cursor:
                    cursor.execute(migration)
                for statement in ddl():
                    warehouse.command(statement)
            elif args.verify:
                verify_publication(SourceLedger(cdc), ClickHousePublisher(warehouse))
            else:
                run(SourceLedger(cdc), PostgresRelay(pg), ClickHousePublisher(warehouse), spark, bootstrap=args.bootstrap)
    finally:
        spark.close()


if __name__ == "__main__":
    main()
