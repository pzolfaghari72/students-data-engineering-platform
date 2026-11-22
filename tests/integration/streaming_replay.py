"""Run in a Spark container against dedicated, disposable Kafka topics and ClickHouse DB.

The fixture creates/deletes only topics with a random test prefix. Apply the
CDC DDL to an empty scratch database before starting this test. The checkpoint
is written under a temporary container directory, away from the production mount.
"""

import json
import os
import tempfile
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread

import clickhouse_connect
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.utils import StreamingQueryException

import stream_cdc
from students_staging.source_tables import SOURCE_TABLES


PREFIX = "students_fixture_" + uuid.uuid4().hex[:12]
DATABASE, SCHEMA = "Fixture", "dbo"
BASELINE = "00000000:00000000:0002"


class CompletedSnapshot(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({"offsets": [{
            "partition": {"server": PREFIX, "database": DATABASE},
            "offset": {"snapshot": True, "snapshot_completed": True, "commit_lsn": BASELINE},
        }]}).encode()
        self.send_response(200)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


def emit(producer, operation, table, key, after, lsn, marker="false"):
    source = {"db": DATABASE, "schema": SCHEMA, "table": table, "snapshot": marker,
              "commit_lsn": lsn, "change_lsn": lsn, "event_serial_no": 1, "ts_ms": 1700000000000}
    envelope = {"op": operation, "source": source, "after": after}
    producer.send(f"{PREFIX}.{DATABASE}.{SCHEMA}.{table}", key=json.dumps(key).encode(),
                  value=None if operation == "t" else json.dumps(envelope).encode()).get(timeout=15)


def client(config):
    return clickhouse_connect.get_client(
        host=config["WAREHOUSE_HOST"], port=int(config["WAREHOUSE_HTTP_PORT"]),
        username=config["CDC_DB_USER"], password=config["CDC_DB_PASSWORD"],
        database=config["CDC_DB_NAME"],
    )


def stop_after_commits(config, expected, outcome):
    try:
        deadline = time.monotonic() + 100
        with client(config) as ch:
            while time.monotonic() < deadline:
                commits = ch.query("SELECT uniqExact(batch_id) FROM cdc_batch_commits").first_row[0]
                if commits >= expected:
                    session = SparkSession._instantiatedSession
                    if session and session.streams.active:
                        outcome.append("completed")
                        session.streams.active[0].stop()
                        return
                time.sleep(1)
            raise RuntimeError("Timed out waiting for the CDC fixture.")
    except Exception as error:
        outcome.append(error)
        session = SparkSession._instantiatedSession
        if session:
            for query in session.streams.active:
                query.stop()


def main():
    config = os.environ.copy()
    if not config["CDC_DB_NAME"].startswith("students_fixture_"):
        raise RuntimeError("Use a dedicated scratch database named students_fixture_*.")
    with client(config) as ch:
        if ch.query("SELECT count() FROM cdc_events").first_row[0]:
            raise RuntimeError("The CDC fixture database must be empty.")

    broker = config["KAFKA_BOOTSTRAP_SERVERS"]
    admin = KafkaAdminClient(bootstrap_servers=broker)
    topics = [f"{PREFIX}.{DATABASE}.{SCHEMA}.{table}" for table, _ in SOURCE_TABLES]
    server = HTTPServer(("127.0.0.1", 0), CompletedSnapshot)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        admin.create_topics([NewTopic(topic, 1, 1, topic_configs={"cleanup.policy": "delete"})
                             for topic in topics])
        config.update({
            "DEBEZIUM_TOPIC_PREFIX": PREFIX, "SOURCE_DB_NAME": DATABASE, "SOURCE_DB_SCHEMA": SCHEMA,
            "DEBEZIUM_CONNECTOR_NAME": PREFIX, "STREAMING_APP_NAME": PREFIX,
            "STREAMING_QUERY_NAME": PREFIX, "STREAMING_GENERATION_ID": PREFIX,
            "CONNECT_REST_URL": f"http://127.0.0.1:{server.server_port}",
            "STREAMING_BOOTSTRAP_TIMEOUT_SECONDS": "30",
            "STREAMING_MAX_OFFSETS_PER_TRIGGER": "500", "STREAMING_TRIGGER_INTERVAL": "1 second",
        })
        producer = KafkaProducer(bootstrap_servers=broker, retries=8, retry_backoff_ms=500)
        try:
            emit(producer, "r", "Students", {"StudentId": 1}, {"StudentId": 1}, BASELINE, "last")
            emit(producer, "c", "CourseOfferings", {"Id": 1}, {"Capacity": 10}, "00000000:00000000:0003")
            emit(producer, "u", "CourseOfferings", {"Id": 1}, {"Capacity": 11}, "00000000:00000000:0004")
            emit(producer, "d", "Students", {"StudentId": 1}, None, "00000000:00000000:0005")
            emit(producer, "t", "Students", {"StudentId": 1}, None, "")
            producer.flush()

            with tempfile.TemporaryDirectory(prefix=PREFIX) as tmp:
                checkpoint = Path(tmp) / "checkpoint"
                original = stream_cdc.ClickHouseCDC

                class FailAfterSeal(original):
                    def seal(self, *args, **kwargs):
                        super().seal(*args, **kwargs)
                        raise RuntimeError("Fixture interruption after ClickHouse commit.")

                stream_cdc.ClickHouseCDC = FailAfterSeal
                try:
                    stream_cdc.run_stream(config, checkpoint)
                except StreamingQueryException as error:
                    if "Fixture interruption after ClickHouse commit" not in str(error):
                        raise
                else:
                    raise RuntimeError("Expected interruption after sink commit.")
                finally:
                    stream_cdc.ClickHouseCDC = original

                # Replay the failed Spark batch and then apply the new Kafka offsets.
                emit(producer, "c", "CourseOfferings", {"Id": 1}, {"Capacity": 10}, "00000000:00000000:0003")
                emit(producer, "c", "Students", {"StudentId": 1}, {"StudentId": 1}, "00000000:00000000:0006")
                producer.flush()
                outcome = []
                watcher = Thread(target=stop_after_commits, args=(config, 2, outcome), daemon=True)
                watcher.start()
                stream_cdc.run_stream(config, checkpoint)
                watcher.join(timeout=10)
                if outcome != ["completed"]:
                    raise RuntimeError(f"CDC replay fixture did not finish: {outcome}")

            with client(config) as ch:
                assert tuple(ch.query(
                    "SELECT students, offered_seats FROM cdc_business_kpis"
                ).first_row) == (1, 11)
                assert ch.query("SELECT count() FROM cdc_logical_events").first_row[0] == 5
                assert ch.query("SELECT count() FROM cdc_events").first_row[0] == 7
                assert ch.query("SELECT status FROM cdc_pipeline_health").first_row[0] == "ACTIVE"
            print("Spark replay fixture passed: one published generation, five logical events, seven Kafka records.")
        finally:
            producer.close()
    finally:
        server.shutdown()
        server.server_close()
        existing = set(admin.list_topics())
        owned = [topic for topic in topics if topic in existing]
        if owned:
            admin.delete_topics(owned)
        admin.close()


if __name__ == "__main__":
    main()
