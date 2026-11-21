"""Consume Debezium Kafka topics and persist CDC events to PostgreSQL/ClickHouse."""

from __future__ import annotations

import time
import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

import psycopg2
from psycopg2.extras import Json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from students_staging.cdc_loader import (
    _apply_scd2,
    _decode_json,
    advance_consumed_offsets,
    normalize_debezium_record,
    persisted_operation_counts,
    snapshot_boundary_consumed,
    source_version_order,
)
from students_staging.source_tables import SCD2_TABLES, SOURCE_TABLES

LOGGER = logging.getLogger("students_cdc_stream")
EXPECTED_TABLES = [source_table for source_table, _ in SOURCE_TABLES]
EVENT_SCHEMA = StructType(
    [
        StructField("generation_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("source_schema", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("source_key", StringType(), False),
        StructField("before_image", StringType(), False),
        StructField("after_image", StringType(), False),
        StructField("source_lsn", StringType(), False),
        StructField("source_commit_lsn", StringType(), False),
        StructField("source_change_lsn", StringType(), False),
        StructField("source_event_serial_no", StringType(), False),
        StructField("source_snapshot_state", StringType(), False),
        StructField("source_version", StringType(), False),
        StructField("source_timestamp", TimestampType(), False),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
    ]
)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set.")
    return value


def _staging_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=_required_env("STAGING_DB_HOST"),
        port=int(_required_env("STAGING_DB_PORT")),
        dbname=_required_env("STAGING_DB_NAME"),
        user=_required_env("STAGING_DB_USER"),
        password=_required_env("STAGING_DB_PASSWORD"),
    )


def _warehouse_url() -> str:
    return (
        f"jdbc:clickhouse:http://{_required_env('WAREHOUSE_HOST')}:"
        f"{_required_env('WAREHOUSE_HTTP_PORT')}/"
        f"{_required_env('WAREHOUSE_DB_NAME')}"
    )


def _topic_pattern() -> str:
    table_pattern = "|".join(
        re.escape(table) for table in sorted(EXPECTED_TABLES)
    )
    return (
        f"^{re.escape(_required_env('DEBEZIUM_TOPIC_PREFIX'))}\\."
        f"{re.escape(_required_env('SOURCE_DB_NAME'))}\\."
        f"{re.escape(_required_env('SOURCE_DB_SCHEMA'))}\\.({table_pattern})$"
    )


def _has_checkpoint(checkpoint_dir: str) -> bool:
    offsets_dir = Path(checkpoint_dir) / "offsets"
    return offsets_dir.is_dir() and any(offsets_dir.iterdir())


def _validate_kafka_start(checkpoint_dir: str) -> None:
    from kafka import KafkaConsumer, TopicPartition

    bootstrap_servers = _required_env("KAFKA_BOOTSTRAP_SERVERS")
    topic_pattern = re.compile(_topic_pattern())
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        request_timeout_ms=30_000,
    )
    try:
        topics = {
            topic for topic in consumer.topics() if topic_pattern.fullmatch(topic)
        }
        partitions = [
            TopicPartition(topic, partition)
            for topic in sorted(topics)
            for partition in sorted(consumer.partitions_for_topic(topic) or ())
        ]
        if not partitions:
            raise RuntimeError("Debezium topics have no Kafka partitions.")
        beginnings = consumer.beginning_offsets(partitions)
        if not _has_checkpoint(checkpoint_dir):
            retained = [
                f"{partition.topic}[{partition.partition}]={offset}"
                for partition, offset in beginnings.items()
                if offset > 0
            ]
            if retained:
                raise RuntimeError(
                    "A new Spark checkpoint cannot start because Kafka retention "
                    "has already removed records: " + ", ".join(retained)
                )
    finally:
        consumer.close()


def _snapshot_completed() -> bool:
    """Confirm snapshot completion from committed Kafka Connect offsets."""
    connector = quote(_required_env("DEBEZIUM_CONNECTOR_NAME"), safe="")
    base_url = _required_env("CONNECT_REST_URL").rstrip("/")
    endpoint = f"{base_url}/connectors/{connector}/offsets"
    expected_server = _required_env("DEBEZIUM_TOPIC_PREFIX")
    expected_database = _required_env("SOURCE_DB_NAME")

    try:
        opener = build_opener(ProxyHandler({}))
        with opener.open(endpoint, timeout=5) as response:
            payload = json.load(response)
    except (URLError, TimeoutError) as error:
        LOGGER.warning("Cannot read Debezium's saved offsets: %s", error)
        return False

    if (
        not isinstance(payload, dict)
        or not isinstance(payload.get("offsets"), list)
    ):
        raise RuntimeError(
            "Kafka Connect returned an invalid offsets response."
        )

    matches = [
        item.get("offset")
        for item in payload["offsets"]
        if isinstance(item, dict)
        and isinstance(item.get("partition"), dict)
        and item["partition"].get("server") == expected_server
        and item["partition"].get("database") == expected_database
    ]

    if not matches or matches == [None]:
        return False

    if len(matches) != 1 or not isinstance(matches[0], dict):
        raise RuntimeError(
            "Expected one saved offset for the configured source."
        )

    offset = matches[0]

    if not offset:
        return False

    if "snapshot" not in offset or "snapshot_completed" not in offset:
        raise RuntimeError(
            "Saved source offsets no longer contain explicit snapshot "
            "completion evidence, and this generation has no saved "
            "boundary. Verify the retained initial snapshot before "
            "bootstrapping."
        )

    if (
        type(offset["snapshot"]) is not bool
        or type(offset["snapshot_completed"]) is not bool
    ):
        raise RuntimeError(
            "Kafka Connect snapshot flags must be JSON booleans."
        )

    return (
        offset["snapshot"] is True
        and offset["snapshot_completed"] is True
    )


def _ensure_snapshot_boundary(
    query_name: str,
    generation_id: str,
    connector_name: str,
    timeout_seconds: int = 3600,
) -> None:
    """Save a confirmed Kafka boundary before starting the streaming query."""
    connection = _staging_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT snapshot_boundary_captured
                FROM metadata.streaming_generations
                WHERE query_name = %s AND generation_id = %s
                """,
                (query_name, generation_id),
            )
            row = cursor.fetchone()
    finally:
        connection.close()

    if row is None:
        raise RuntimeError(
            "The streaming generation metadata disappeared."
        )

    if row[0]:
        LOGGER.info("Reusing the saved snapshot boundary.")
        return

    LOGGER.info(
        "Waiting for Debezium to persist full-snapshot completion."
    )
    deadline = time.monotonic() + timeout_seconds

    while not _snapshot_completed():
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "Timed out waiting for confirmed snapshot completion."
            )
        time.sleep(5)

    end_offsets = _snapshot_end_offsets()

    if not end_offsets:
        raise RuntimeError(
            "No Debezium table partitions are available for bootstrap."
        )

    _persist_postgres_batch(
        [],
        query_name=query_name,
        generation_id=generation_id,
        connector_name=connector_name,
        snapshot_end_offsets=end_offsets,
    )

    LOGGER.info(
        "Saved snapshot boundary for %s Kafka partitions.",
        len(end_offsets),
    )


def _snapshot_end_offsets() -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        enable_auto_commit=False,
        request_timeout_ms=30_000,
    )
    try:
        topic_pattern = re.compile(_topic_pattern())
        topics = {
            topic for topic in consumer.topics() if topic_pattern.fullmatch(topic)
        }
        partitions = [
            TopicPartition(topic, partition)
            for topic in sorted(topics)
            for partition in sorted(consumer.partitions_for_topic(topic) or ())
        ]
        if not partitions:
            return {}
        return {
            f"{partition.topic}:{partition.partition}": int(offset)
            for partition, offset in consumer.end_offsets(partitions).items()
        }
    finally:
        consumer.close()


def _get_or_create_generation(query_name: str, connector_name: str) -> str:
    connection = _staging_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT generation_id
                FROM metadata.streaming_generations
                WHERE query_name = %s
                """,
                (query_name,),
            )
            row = cursor.fetchone()
            if row:
                generation_id = str(row[0])
            else:
                generation_id = str(uuid.uuid4())
                cursor.execute(
                    """
                    INSERT INTO metadata.streaming_generations
                        (query_name, generation_id, connector_name,
                         expected_tables, completed_snapshot_tables, status)
                    VALUES (%s, %s, %s, %s, '[]'::jsonb, 'BOOTSTRAPPING')
                    """,
                    (
                        query_name,
                        generation_id,
                        connector_name,
                        Json(EXPECTED_TABLES),
                    ),
                )
        connection.commit()
        return generation_id
    finally:
        connection.close()


def _json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str) if value is not None else ""


def _clickhouse_row(generation_id: str, event: dict[str, Any]) -> tuple[Any, ...]:
    return (
        generation_id,
        f"{event['kafka_topic']}:{event['kafka_partition']}:{event['kafka_offset']}",
        event["source_schema"],
        event["source_table"],
        event["operation"],
        _json_text(event["source_key"]),
        _json_text(event["before_image"]),
        _json_text(event["after_image"]),
        event["source_lsn"] or "",
        event["source_commit_lsn"] or "",
        event["source_change_lsn"] or "",
        event["source_event_serial_no"] or "",
        event["source_snapshot_state"] or "",
        source_version_order(event),
        event["source_timestamp"],
        event["kafka_topic"],
        event["kafka_partition"],
        event["kafka_offset"],
    )


def _persist_postgres_batch(
    events: list[dict[str, Any]],
    *,
    query_name: str,
    generation_id: str,
    connector_name: str,
    snapshot_end_offsets: dict[str, int] | None,
) -> bool:
    """Persist a microbatch and report when its generation needs publishing."""
    connection = _staging_connection()
    try:
        with connection.cursor() as cursor:
            if events:
                cursor.execute(
                    """
                    INSERT INTO metadata.pipeline_runs
                        (pipeline_name, run_type, status, execution_context)
                    VALUES ('spark_cdc_stream_to_warehouse', 'INCREMENTAL',
                            'RUNNING', %s)
                    RETURNING run_id
                    """,
                    (
                        Json(
                            {
                                "streaming_query": query_name,
                                "generation_id": generation_id,
                            }
                        ),
                    ),
                )
                run_id = cursor.fetchone()[0]
                cursor.execute(
                    """
                    INSERT INTO metadata.extraction_batches
                        (run_id, source_schema, source_table, capture_instance,
                         extraction_type, status)
                    VALUES (%s, %s, 'CDC_STREAM', %s, 'CDC', 'RUNNING')
                    RETURNING batch_id
                    """,
                    (
                        run_id,
                        _required_env("SOURCE_DB_SCHEMA"),
                        connector_name,
                    ),
                )
                batch_id = cursor.fetchone()[0]

                inserted = updated = deleted = 0
                for event in events:
                    cursor.execute(
                        """
                        INSERT INTO history.cdc_events
                            (source_schema, source_table, connector_name, source_lsn,
                             source_commit_lsn, source_change_lsn,
                             source_event_serial_no, source_snapshot_state,
                             operation, source_key, before_image, after_image,
                             source_timestamp, kafka_topic, kafka_partition,
                             kafka_offset, batch_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s)
                        ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
                        DO NOTHING
                        RETURNING event_id
                        """,
                        (
                            event["source_schema"],
                            event["source_table"],
                            connector_name,
                            event["source_lsn"],
                            event["source_commit_lsn"],
                            event["source_change_lsn"],
                            event["source_event_serial_no"],
                            event["source_snapshot_state"],
                            event["operation"],
                            Json(event["source_key"]),
                            Json(event["before_image"])
                            if event["before_image"] is not None
                            else None,
                            Json(event["after_image"])
                            if event["after_image"] is not None
                            else None,
                            event["source_timestamp"],
                            event["kafka_topic"],
                            event["kafka_partition"],
                            event["kafka_offset"],
                            batch_id,
                        ),
                    )
                    was_inserted = cursor.fetchone() is not None
                    if was_inserted and event["source_table"] in SCD2_TABLES:
                        _apply_scd2(cursor, event, Json)                    
                    insert_delta, update_delta, delete_delta = (
                        persisted_operation_counts(
                            event["operation"],
                            inserted=was_inserted,
                        )
                    )
                    inserted += insert_delta
                    updated += update_delta
                    deleted += delete_delta

                cursor.execute(
                    """
                    UPDATE metadata.extraction_batches
                    SET status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP,
                        extracted_rows = %s, inserted_rows = %s,
                        updated_rows = %s, deleted_rows = %s
                    WHERE batch_id = %s
                    """,
                    (len(events), inserted, updated, deleted, batch_id),
                )
                cursor.execute(
                    """
                    UPDATE metadata.pipeline_runs
                    SET status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )

            cursor.execute(
                """
                SELECT snapshot_end_offsets, consumed_offsets,
                       snapshot_boundary_captured, snapshot_events_complete, status
                FROM metadata.streaming_generations
                WHERE query_name = %s AND generation_id = %s
                FOR UPDATE
                """,
                (query_name, generation_id),
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError("The streaming generation metadata disappeared.")
            known_end_offsets = row[0] or {}
            consumed_offsets = row[1] or {}
            boundary_captured = bool(row[2])
            if snapshot_end_offsets is not None and not boundary_captured:
                known_end_offsets = snapshot_end_offsets
                boundary_captured = True
            consumed_offsets = advance_consumed_offsets(consumed_offsets, events)
            snapshot_complete = bool(row[3]) or snapshot_boundary_consumed(
                known_end_offsets,
                consumed_offsets,
                boundary_captured,
            )
            cursor.execute(
                """
                UPDATE metadata.streaming_generations
                SET snapshot_end_offsets = %s,
                    consumed_offsets = %s,
                    snapshot_boundary_captured = %s,
                    snapshot_events_complete = %s,
                    last_event_at = CASE WHEN %s THEN CURRENT_TIMESTAMP
                                         ELSE last_event_at END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE query_name = %s AND generation_id = %s
                """,
                (
                    Json(known_end_offsets),
                    Json(consumed_offsets),
                    boundary_captured,
                    snapshot_complete,
                    bool(events),
                    query_name,
                    generation_id,
                ),
            )
            cursor.execute(
                """
                SELECT snapshot_events_complete, status
                FROM metadata.streaming_generations
                WHERE query_name = %s AND generation_id = %s
                """,
                (query_name, generation_id),
            )
            snapshot_complete, status = cursor.fetchone()
            publish_ready = bool(snapshot_complete) and status != "ACTIVE"
        connection.commit()
        return publish_ready
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _write_clickhouse_batch(
    spark: SparkSession,
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    frame: DataFrame = spark.createDataFrame(rows, schema=EVENT_SCHEMA)
    (
        frame.write.format("jdbc")
        .option("url", _warehouse_url())
        .option("dbtable", "cdc_events")
        .option("user", _required_env("WAREHOUSE_DB_USER"))
        .option("password", _required_env("WAREHOUSE_DB_PASSWORD"))
        .option("driver", _required_env("WAREHOUSE_JDBC_DRIVER"))
        .option("batchsize", _required_env("WAREHOUSE_WRITE_BATCH_SIZE"))
        .mode("append")
        .save()
    )


def _publish_generation(generation_id: str, query_name: str) -> None:
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=_required_env("WAREHOUSE_HOST"),
        port=int(_required_env("WAREHOUSE_HTTP_PORT")),
        username=_required_env("WAREHOUSE_DB_USER"),
        password=_required_env("WAREHOUSE_DB_PASSWORD"),
        database=_required_env("WAREHOUSE_DB_NAME"),
    )
    try:
        client.insert(
            "warehouse_stream_generations",
            [[query_name, generation_id, "PUBLISHED"]],
            column_names=["query_name", "generation_id", "status"],
        )
    finally:
        client.close()


def run() -> None:
    app_name = _required_env("STREAMING_APP_NAME")
    query_name = _required_env("STREAMING_QUERY_NAME")
    checkpoint_dir = _required_env("STREAMING_CHECKPOINT_DIR")
    connector_name = _required_env("DEBEZIUM_CONNECTOR_NAME")
    max_offsets = int(_required_env("STREAMING_MAX_OFFSETS_PER_TRIGGER"))
    if max_offsets <= 0:
        raise ValueError("STREAMING_MAX_OFFSETS_PER_TRIGGER must be positive.")
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

    generation_id = _get_or_create_generation(query_name, connector_name)

    _ensure_snapshot_boundary(
        query_name,
        generation_id,
        connector_name,
    )

    _validate_kafka_start(checkpoint_dir)
    
    topic_pattern = _topic_pattern()
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", _required_env("KAFKA_BOOTSTRAP_SERVERS"))
        .option("kafka.group.id", _required_env("STREAMING_KAFKA_GROUP_ID"))
        .option("subscribePattern", topic_pattern)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .load()
    )

    def process_batch(frame: DataFrame, batch_id: int) -> None:
        try:
            events: list[dict[str, Any]] = []
            for row in frame.select(
                "topic", "partition", "offset", "key", "value"
            ).toLocalIterator():
                event = normalize_debezium_record(
                    topic=row.topic,
                    partition=row.partition,
                    offset=row.offset,
                    key=_decode_json(row.key),
                    value=_decode_json(row.value),
                    topic_prefix=_required_env("DEBEZIUM_TOPIC_PREFIX"),
                    expected_schema=_required_env("SOURCE_DB_SCHEMA"),
                    expected_database=_required_env("SOURCE_DB_NAME"),
                )
                event["source_version"] = source_version_order(event)
                events.append(event)

            publish_ready = _persist_postgres_batch(
                events,
                query_name=query_name,
                generation_id=generation_id,
                connector_name=connector_name,
                snapshot_end_offsets=None,
            )
            rows = [_clickhouse_row(generation_id, event) for event in events]
            _write_clickhouse_batch(spark, rows)
            if publish_ready:
                _publish_generation(generation_id, query_name)
                connection = _staging_connection()
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            UPDATE metadata.streaming_generations
                            SET status = 'ACTIVE', updated_at = CURRENT_TIMESTAMP
                            WHERE query_name = %s AND generation_id = %s
                            """,
                            (query_name, generation_id),
                        )
                    connection.commit()
                finally:
                    connection.close()
            LOGGER.info(
                "Committed Spark microbatch %s with %s CDC records.",
                batch_id,
                len(events),
            )
        except Exception:
            LOGGER.exception("Spark CDC microbatch %s failed.", batch_id)
            raise

    query = (
        source.writeStream.queryName(query_name)
        .option("checkpointLocation", checkpoint_dir)
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()
