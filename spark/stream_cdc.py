"""Load validated Debezium microbatches into the dedicated ClickHouse CDC database."""

from __future__ import annotations

import fcntl
import json
import logging
import os
import time
from contextlib import closing
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

from cdc_events import (
    advance_offsets,
    completed_source_offset,
    fingerprint,
    json_text,
    normalize_debezium_record,
    source_version_order,
)
from students_staging.source_tables import SOURCE_TABLES

LOGGER = logging.getLogger("students_cdc")
EVENT_COLUMNS = [
    "generation_id", "batch_id", "kafka_topic", "kafka_partition", "kafka_offset",
    "source_table", "source_key", "operation", "before_image", "after_image",
    "commit_lsn", "source_version", "snapshot_state", "source_time_ms", "record_hash",
]


def kafka_identity(spark, bootstrap, topics):
    """Use the same Kafka client as Spark, including the broker-assigned topic IDs."""
    jvm = spark.sparkContext._jvm
    properties = jvm.java.util.Properties()
    properties.setProperty("bootstrap.servers", bootstrap)
    names = jvm.java.util.ArrayList()
    for topic in topics:
        names.add(topic)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(properties)
    try:
        unit = jvm.java.util.concurrent.TimeUnit.SECONDS
        cluster = admin.describeCluster().clusterId().get(30, unit)
        descriptions = admin.describeTopics(names).allTopicNames().get(30, unit)
        identity = {}
        for topic in topics:
            description = descriptions.get(topic)
            topic_id = description.topicId().toString()
            if topic_id == "AAAAAAAAAAAAAAAAAAAAAA" or description.partitions().isEmpty():
                raise RuntimeError(f"Kafka has no durable identity/partitions for {topic}.")
            identity[topic] = [
                topic_id, sorted(part.partition() for part in description.partitions())
            ]
        return [cluster, identity]
    finally:
        admin.close()


def kafka_bounds(consumer, partitions):
    beginnings = consumer.beginning_offsets(partitions)
    endings = consumer.end_offsets(partitions)
    return tuple(
        {f"{p.topic}:{p.partition}": offsets[p] for p in partitions}
        for offsets in (beginnings, endings)
    )


def wait_for_snapshot(rest_url, connector, prefix, database, timeout):
    endpoint = f"{rest_url.rstrip('/')}/connectors/{quote(connector, safe='')}/offsets"
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with opener.open(endpoint, timeout=5) as response:
                payload = json.load(response)
        except (URLError, TimeoutError):
            time.sleep(5)
            continue
        matches = [
            item.get("offset") for item in payload["offsets"]
            if item["partition"] == {"server": prefix, "database": database}
        ]
        if len(matches) > 1:
            raise RuntimeError("More than one Connect offset matches the source.")
        if matches and matches[0]:
            complete, baseline = completed_source_offset(matches[0])
            if complete:
                return baseline
        time.sleep(5)
    raise RuntimeError("Debezium did not supply a completed snapshot or live source offset.")


def checkpoint_state(path):
    """Read the persistent Spark query ID and latest committed batch."""
    metadata = path / "metadata"
    commits = path / "commits"
    query_id = json.loads(metadata.read_text())["id"] if metadata.is_file() else ""
    batch_id = max(
        (int(item.name) for item in commits.glob("*") if item.name.isdecimal()),
        default=-1,
    )
    return query_id, batch_id


def checked_checkpoint(path, sealed_query_id, sealed_batch_id):
    query_id, checkpoint_batch = checkpoint_state(path)
    if sealed_query_id and sealed_query_id != query_id:
        raise RuntimeError("The CDC generation belongs to a different Spark checkpoint.")
    if checkpoint_batch > sealed_batch_id or sealed_batch_id > checkpoint_batch + 1:
        raise RuntimeError("The checkpoint and verified ClickHouse progress disagree.")


def event_row(generation, batch_id, event):
    fields = (
        event["kafka_topic"], event["kafka_partition"], event["kafka_offset"],
        event["source_table"], json_text(event["source_key"]), event["operation"],
        json_text(event["before_image"]), json_text(event["after_image"]),
        (event["source_commit_lsn"] or "").lower(), source_version_order(event),
        event["source_snapshot_state"] or "",
        int(event["source_timestamp"].timestamp() * 1000),
    )
    return (generation, batch_id, *fields, fingerprint(fields))


class ClickHouseCDC:
    def __init__(self, client, generation):
        self.client = client
        self.generation = generation

    def query(self, sql, **params):
        return self.client.query(
            sql, parameters={"generation": self.generation, **params}
        ).result_rows

    def initialize(self, lineage, boundary, baseline):
        self.client.insert(
            "cdc_generations", [[self.generation, lineage, boundary, baseline]],
            column_names=["generation_id", "lineage", "bootstrap_end", "source_baseline"],
        )

    def generation_info(self):
        rows = self.query(
            "SELECT DISTINCT lineage, bootstrap_end, source_baseline "
            "FROM cdc_generations WHERE generation_id = {generation:String}"
        )
        if len(rows) > 1:
            raise RuntimeError("Conflicting CDC bootstrap manifests.")
        return rows[0] if rows else None

    def latest_commit(self):
        rows = self.query(
            "SELECT batch_id, checkpoint_id, next_offsets FROM cdc_batch_commits "
            "WHERE generation_id = {generation:String} ORDER BY batch_id DESC LIMIT 1"
        )
        return rows[0] if rows else (-1, "", {})

    def verify_prefix(self, offsets):
        rows = self.query(
            "SELECT kafka_topic, kafka_partition, uniqExact(kafka_offset), "
            "min(kafka_offset), max(kafka_offset) FROM cdc_committed_records "
            "WHERE generation_id = {generation:String} GROUP BY kafka_topic, kafka_partition"
        )
        actual = {f"{topic}:{part}": (count, first, last)
                  for topic, part, count, first, last in rows}
        expected = {part: (end, 0, end - 1) for part, end in offsets.items() if end}
        if actual != expected:
            raise RuntimeError("Stored CDC records do not cover the sealed Kafka prefix.")

    def seal(self, query_id, batch_id, rows, previous, broker_ends):
        hashes = {(row[2], row[3], row[4]): row[-1] for row in rows}
        if len(hashes) != len(rows):
            raise RuntimeError("Spark returned duplicate transport positions in one batch.")
        digest = fingerprint(sorted(hashes.items()))
        existing = self.query(
            "SELECT DISTINCT checkpoint_id, toString(record_digest), next_offsets FROM cdc_batch_commits "
            "WHERE generation_id = {generation:String} AND batch_id = {batch:UInt64}",
            batch=batch_id,
        )
        if existing:
            if len(existing) != 1 or tuple(existing[0][:2]) != (query_id, digest):
                raise RuntimeError("A replayed Spark batch differs from its verified commit.")
            offsets = existing[0][2]
        else:
            offsets = advance_offsets(previous, list(hashes))
            if rows:
                self.client.insert("cdc_events", rows, column_names=EVENT_COLUMNS)

        actual = self.query(
            "SELECT DISTINCT kafka_topic, kafka_partition, kafka_offset, toString(record_hash) "
            "FROM cdc_events WHERE generation_id = {generation:String} AND batch_id = {batch:UInt64}",
            batch=batch_id,
        )
        if {tuple(row) for row in actual} != {(*position, record_hash) for position, record_hash in hashes.items()}:
            raise RuntimeError(f"Stored ClickHouse records failed verification for batch {batch_id}.")

        if not existing:
            live_source_time = max((row[-2] for row in rows if row[7] in {"c", "u", "d"}), default=0)
            self.client.insert(
                "cdc_batch_commits",
                [[self.generation, batch_id, query_id, digest, offsets, broker_ends, len(rows), live_source_time]],
                column_names=[
                    "generation_id", "batch_id", "checkpoint_id", "record_digest",
                    "next_offsets", "broker_end_offsets", "records", "live_source_time_ms",
                ],
            )
        return offsets

    def publish_if_ready(self, boundary, offsets, source_baseline):
        if any(offsets[part] < end for part, end in boundary.items()):
            return
        if self.query(
            "SELECT 1 FROM cdc_publications WHERE generation_id = {generation:String} LIMIT 1"
        ):
            return
        if any(boundary.values()):
            self.verify_prefix(offsets)
            candidates = self.query(
                "SELECT DISTINCT commit_lsn FROM cdc_committed_records "
                "WHERE generation_id = {generation:String} AND operation = 'r' "
                "AND snapshot_state = 'last' ORDER BY commit_lsn DESC"
            )
            if not candidates or (source_baseline and source_baseline != candidates[0][0]):
                raise RuntimeError("The completed initial snapshot has no matching retained last record.")
            baseline = candidates[0][0]
            conflicts = self.query(
                "SELECT 1 FROM cdc_committed_records "
                "WHERE generation_id = {generation:String} AND operation = 'r' "
                "AND commit_lsn = {baseline:String} GROUP BY source_table, source_key "
                "HAVING uniqExact(after_image) > 1 LIMIT 1",
                baseline=baseline,
            )
            if conflicts:
                raise RuntimeError("Conflicting row images exist in the completed initial snapshot.")
        elif source_baseline:
            baseline = source_baseline
        else:
            raise RuntimeError("An empty CDC bootstrap requires explicit snapshot-completion evidence.")

        self.client.insert(
            "cdc_publications", [[self.generation, baseline]],
            column_names=["generation_id", "baseline_lsn"],
        )
        LOGGER.info("Published CDC generation %s.", self.generation)


def run_stream(config, checkpoint):
    import clickhouse_connect
    from kafka import KafkaConsumer, TopicPartition
    from pyspark.sql import SparkSession

    bootstrap = config["KAFKA_BOOTSTRAP_SERVERS"]
    prefix, database, schema = (config[key] for key in (
        "DEBEZIUM_TOPIC_PREFIX", "SOURCE_DB_NAME", "SOURCE_DB_SCHEMA"
    ))
    connector = config["DEBEZIUM_CONNECTOR_NAME"]
    topics = [f"{prefix}.{database}.{schema}.{table}" for table, _ in SOURCE_TABLES]
    spark = (SparkSession.builder.appName(config["STREAMING_APP_NAME"])
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        with closing(KafkaConsumer(
            bootstrap_servers=bootstrap, enable_auto_commit=False, request_timeout_ms=30_000,
        )) as consumer, clickhouse_connect.get_client(
            host=config["WAREHOUSE_HOST"], port=int(config["WAREHOUSE_HTTP_PORT"]),
            username=config["CDC_DB_USER"], password=config["CDC_DB_PASSWORD"],
            database=config["CDC_DB_NAME"],
        ) as client:
            cluster, topic_ids = kafka_identity(spark, bootstrap, topics)
            partitions = [
                TopicPartition(topic, part)
                for topic, (_, parts) in topic_ids.items() for part in parts
            ]
            store = ClickHouseCDC(client, config["STREAMING_GENERATION_ID"])
            lineage = fingerprint([cluster, topic_ids, connector, config["STREAMING_QUERY_NAME"]])
            generation_info = store.generation_info()
            sealed_batch, sealed_id, offsets = store.latest_commit()
            checked_checkpoint(checkpoint, sealed_id, sealed_batch)
            if generation_info is None:
                baseline = wait_for_snapshot(
                    config["CONNECT_REST_URL"], connector, prefix, database,
                    int(config["STREAMING_BOOTSTRAP_TIMEOUT_SECONDS"]),
                )
                beginnings, boundary = kafka_bounds(consumer, partitions)
                if any(beginnings.values()):
                    raise RuntimeError("The full initial Kafka snapshot is no longer retained.")
                store.initialize(lineage, boundary, baseline)
            else:
                saved_lineage, boundary, baseline = generation_info
                if saved_lineage != lineage:
                    raise RuntimeError("Kafka topics or connector lineage changed for this generation.")

            offsets = offsets or dict.fromkeys(boundary, 0)
            beginnings, ends = kafka_bounds(consumer, partitions)
            if any(not beginnings[part] <= offset <= ends[part] for part, offset in offsets.items()):
                raise RuntimeError("Kafka does not retain the saved CDC continuation offsets.")
            if sealed_batch >= 0:
                store.verify_prefix(offsets)
            store.publish_if_ready(boundary, offsets, baseline)

            def process(frame, batch_id):
                nonlocal offsets
                try:
                    rows = []
                    for row in frame.select("topic", "partition", "offset", "key", "value").toLocalIterator():
                        try:
                            event = normalize_debezium_record(
                                topic=row.topic, partition=row.partition, offset=row.offset,
                                key=row.key, value=row.value, topic_prefix=prefix,
                                expected_schema=schema, expected_database=database,
                            )
                            rows.append(event_row(store.generation, batch_id, event))
                        except (TypeError, ValueError, KeyError):
                            raise RuntimeError(
                                f"Invalid CDC record {row.topic}[{row.partition}]@{row.offset}."
                            ) from None
                    query_id, _ = checkpoint_state(checkpoint)
                    if not query_id:
                        raise RuntimeError("Spark did not initialize its checkpoint identity.")
                    _, broker_ends = kafka_bounds(consumer, partitions)
                    offsets = store.seal(query_id, batch_id, rows, offsets, broker_ends)
                    store.publish_if_ready(boundary, offsets, baseline)
                    LOGGER.info("Verified CDC batch %s with %s records.", batch_id, len(rows))
                except Exception as error:
                    LOGGER.error("CDC batch %s failed (%s).", batch_id, type(error).__name__)
                    if isinstance(error, RuntimeError):
                        raise
                    raise RuntimeError("CDC microbatch processing failed.") from None

            query = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", ",".join(topics))
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", config["STREAMING_MAX_OFFSETS_PER_TRIGGER"])
                .load().writeStream.queryName(config["STREAMING_QUERY_NAME"])
                .option("checkpointLocation", str(checkpoint))
                .trigger(processingTime=config["STREAMING_TRIGGER_INTERVAL"])
                .foreachBatch(process).start()
            )
            query.awaitTermination()
    finally:
        spark.stop()


def run():
    config = os.environ
    checkpoint = Path(config["STREAMING_CHECKPOINT_DIR"]).resolve()
    mount = Path("/opt/spark/checkpoints")
    if mount not in checkpoint.parents:
        raise RuntimeError("STREAMING_CHECKPOINT_DIR must be under the persistent checkpoint mount.")
    with (mount / ".cdc-writer.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another CDC writer holds the checkpoint volume lock.") from None
        run_stream(config, checkpoint)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
