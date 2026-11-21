"""Consume Debezium records into the staging event log and SCD2 store."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from students_staging.source_tables import SCD2_TABLES, SOURCE_TABLES

LOGGER = logging.getLogger(__name__)
POLL_RECORDS = 500


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set.")
    return value


def _decode_json(value: Any) -> Any:
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    return value


def _source_topic_parts(
    topic: str,
    topic_prefix: str,
) -> tuple[str, str, str]:
    parts = topic.rsplit(".", 3)
    if len(parts) != 4 or parts[0] != topic_prefix:
        raise ValueError(f"Unexpected Debezium topic name: {topic}")
    return parts[1], parts[2], parts[3]


def _event_timestamp(source: dict[str, Any], envelope: dict[str, Any]) -> datetime:
    value = source.get("ts_ms")
    if value is None:
        value = envelope.get("ts_ms")
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1_000, tz=UTC)
    if isinstance(value, str):
        try:
            if value.lstrip("-").isdigit():
                return datetime.fromtimestamp(int(value) / 1_000, tz=UTC)
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"Invalid Debezium event timestamp: {value}") from error
        return timestamp.replace(tzinfo=UTC) if timestamp.tzinfo is None else timestamp
    raise ValueError(f"Unsupported Debezium timestamp value: {type(value).__name__}")


def normalize_debezium_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: Any,
    value: Any,
    topic_prefix: str,
    expected_schema: str,
    expected_database: str,
) -> dict[str, Any]:
    """Validate a Kafka record and normalize its Debezium envelope."""
    topic_database, topic_schema, topic_table = _source_topic_parts(
        topic, topic_prefix
    )
    if topic_database.casefold() != expected_database.casefold():
        raise ValueError(f"Unexpected source database in topic {topic}.")
    table_lookup = {source.casefold(): source for source, _ in SOURCE_TABLES}
    source_key = _decode_json(key)
    if isinstance(source_key, dict) and "payload" in source_key:
        source_key = source_key["payload"]
    if not isinstance(source_key, dict):
        raise ValueError(f"Kafka record {topic}[{partition}]@{offset} has no row key.")

    decoded_value = _decode_json(value)
    if decoded_value is None:
        operation = "t"
        envelope: dict[str, Any] = {}
    else:
        if not isinstance(decoded_value, dict):
            raise ValueError(f"Kafka record {topic}[{partition}]@{offset} is not an object.")
        envelope = decoded_value.get("payload", decoded_value)
        if not isinstance(envelope, dict):
            raise ValueError(f"Kafka record {topic}[{partition}]@{offset} has no envelope.")
        operation = envelope.get("op")
        if operation not in {"c", "r", "u", "d"}:
            raise ValueError(
                f"Unsupported Debezium operation {operation!r} in "
                f"{topic}[{partition}]@{offset}."
            )

    source = envelope.get("source") or {}
    if not isinstance(source, dict):
        raise ValueError(f"Debezium source metadata is invalid in {topic}[{partition}]@{offset}.")
    source_schema = source.get("schema") or topic_schema
    source_table = source.get("table") or topic_table
    source_database = source.get("db") or topic_database
    if (
        not isinstance(source_database, str)
        or not isinstance(source_schema, str)
        or not isinstance(source_table, str)
    ):
        raise ValueError(f"Invalid source table metadata in topic {topic}.")
    if source_database.casefold() != expected_database.casefold():
        raise ValueError(f"Unexpected source database {source_database!r} in topic {topic}.")
    if source_schema.casefold() != expected_schema.casefold():
        raise ValueError(f"Unexpected source schema {source_schema!r} in topic {topic}.")
    canonical_table = table_lookup.get(source_table.casefold())
    if canonical_table is None or canonical_table.casefold() != topic_table.casefold():
        raise ValueError(f"Unexpected source table {source_table!r} in topic {topic}.")

    before_image = envelope.get("before")
    after_image = envelope.get("after")
    if before_image is not None and not isinstance(before_image, dict):
        raise ValueError(f"Invalid before image in {topic}[{partition}]@{offset}.")
    if after_image is not None and not isinstance(after_image, dict):
        raise ValueError(f"Invalid after image in {topic}[{partition}]@{offset}.")
    if operation in {"c", "r"} and after_image is None:
        raise ValueError(f"Debezium {operation!r} event has no after image in {topic}.")
    if operation == "u" and after_image is None:
        raise ValueError(f"Debezium update has no after image in {topic}.")

    commit_lsn = source.get("commit_lsn")
    change_lsn = source.get("change_lsn")
    event_serial_no = source.get("event_serial_no")
    snapshot_value = source.get("snapshot")
    return {
        "source_schema": source_schema,
        "source_table": canonical_table,
        "operation": operation,
        "source_key": source_key,
        "before_image": before_image,
        "after_image": after_image,
        "source_lsn": str(commit_lsn or change_lsn)
        if commit_lsn is not None or change_lsn is not None
        else None,
        "source_commit_lsn": str(commit_lsn) if commit_lsn is not None else None,
        "source_change_lsn": str(change_lsn) if change_lsn is not None else None,
        "source_event_serial_no": (
            str(event_serial_no) if event_serial_no is not None else None
        ),
        "source_snapshot_state": (
            str(snapshot_value).lower() if snapshot_value is not None else None
        ),
        "source_timestamp": _event_timestamp(source, envelope),
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }


def source_version_order(event: dict[str, Any]) -> str:
    """Build a fixed-width, lexically sortable SQL Server LSN order key."""
    def lsn_number(value: Any) -> int:
        if value is None:
            return 0
        compact = str(value).replace(":", "")
        if not compact or any(character not in "0123456789abcdefABCDEF" for character in compact):
            raise ValueError(f"Invalid SQL Server LSN value: {value!r}")
        return int(compact, 16)

    commit_lsn = event.get("source_commit_lsn")
    change_lsn = event.get("source_change_lsn") or commit_lsn
    serial_no = event.get("source_event_serial_no")
    serial = int(serial_no) if serial_no is not None else 0
    if serial < 0:
        raise ValueError("Debezium event_serial_no cannot be negative.")
    return (
        f"{lsn_number(commit_lsn):025d}:"
        f"{lsn_number(change_lsn):025d}:"
        f"{serial:020d}"
    )


def advance_consumed_offsets(
    consumed_offsets: dict[str, int],
    events: list[dict[str, Any]],
) -> dict[str, int]:
    """Return monotonic next offsets for all Kafka partitions in a microbatch."""
    updated = dict(consumed_offsets)
    for event in events:
        key = f"{event['kafka_topic']}:{event['kafka_partition']}"
        updated[key] = max(updated.get(key, 0), int(event["kafka_offset"]) + 1)
    return updated


def persisted_operation_counts(
    operation: str,
    inserted: bool,
) -> tuple[int, int, int]:
    """Return insert, update, and delete counts for a newly persisted event."""
    if not inserted:
        return 0, 0, 0
    if operation in {"c", "r"}:
        return 1, 0, 0
    if operation == "u":
        return 0, 1, 0
    if operation == "d":
        return 0, 0, 1
    if operation == "t":
        return 0, 0, 0
    raise ValueError(f"Unsupported CDC operation {operation!r}.")


def snapshot_boundary_consumed(
    end_offsets: dict[str, int],
    consumed_offsets: dict[str, int],
    boundary_captured: bool,
) -> bool:
    """Check that Spark has consumed every Kafka record through the snapshot cut."""
    return boundary_captured and all(
        consumed_offsets.get(key, -1) >= end_offset
        for key, end_offset in end_offsets.items()
    )


def scd2_transition(
    current: dict[str, Any] | None,
    event: dict[str, Any],
) -> tuple[bool, dict[str, Any] | None, bool]:
    """Return whether to add a version, its attributes, and its delete flag."""
    operation = event["operation"]
    if operation == "t":
        return False, None, False
    if operation == "d":
        if current is not None and current["is_deleted"]:
            return False, None, True
        attributes = (
            current["attributes"]
            if current is not None
            else event["before_image"] or {}
        )
        return True, attributes, True

    attributes = event["after_image"]
    if (
        current is not None
        and not current["is_deleted"]
        and current["attributes"] == attributes
    ):
        return False, None, False
    return True, attributes, False


def _staging_connection():
    import psycopg2

    return psycopg2.connect(
        host=_required_env("STAGING_DB_HOST"),
        port=int(_required_env("STAGING_DB_PORT")),
        dbname=_required_env("STAGING_DB_NAME"),
        user=_required_env("STAGING_DB_USER"),
        password=_required_env("STAGING_DB_PASSWORD"),
    )


def _apply_scd2(cursor: Any, event: dict[str, Any], json_value: Any) -> bool:
    source_key = json_value(event["source_key"])
    cursor.execute(
        """
        SELECT version_id, attributes, is_deleted, valid_from
        FROM scd.entity_versions
        WHERE source_schema = %s
          AND source_table = %s
          AND source_key = %s
          AND is_current
        FOR UPDATE
        """,
        (event["source_schema"], event["source_table"], source_key),
    )
    row = cursor.fetchone()
    current = (
        {
            "version_id": row[0],
            "attributes": row[1],
            "is_deleted": row[2],
            "valid_from": row[3],
        }
        if row
        else None
    )
    changed, attributes, is_deleted = scd2_transition(current, event)
    if not changed:
        return False

    valid_from = event["source_timestamp"]
    if current is not None:
        if valid_from <= current["valid_from"]:
            valid_from = current["valid_from"] + timedelta(microseconds=1)
        cursor.execute(
            """
            UPDATE scd.entity_versions
            SET valid_to = %s, is_current = FALSE
            WHERE version_id = %s
            """,
            (valid_from, current["version_id"]),
        )

    cursor.execute(
        """
        INSERT INTO scd.entity_versions
            (source_schema, source_table, source_key, attributes, valid_from,
             is_current, is_deleted, source_lsn)
        VALUES (%s, %s, %s, %s, %s, TRUE, %s, %s)
        """,
        (
            event["source_schema"],
            event["source_table"],
            source_key,
            json_value(attributes or {}),
            valid_from,
            is_deleted,
            event["source_lsn"],
        ),
    )
    return True


def consume_debezium_events(**context: Any) -> dict[str, int]:
    """Consume one bounded Kafka batch and atomically persist its DB offsets."""
    from kafka import KafkaConsumer, TopicPartition
    from psycopg2.extras import Json

    bootstrap_servers = _required_env("KAFKA_BOOTSTRAP_SERVERS")
    topic_prefix = _required_env("DEBEZIUM_TOPIC_PREFIX")
    connector_name = _required_env("DEBEZIUM_CONNECTOR_NAME")
    expected_schema = _required_env("SOURCE_DB_SCHEMA")
    expected_database = _required_env("SOURCE_DB_NAME")
    max_records = int(_required_env("CDC_MAX_RECORDS"))
    max_seconds = int(_required_env("CDC_MAX_SECONDS"))
    if max_records <= 0 or max_seconds <= 0:
        raise ValueError("CDC_MAX_RECORDS and CDC_MAX_SECONDS must be positive.")

    connection = _staging_connection()
    consumer = None
    run_id: int | None = None
    batch_id: int | None = None
    records_read = 0
    events_inserted = 0
    inserted_count = 0
    updated_count = 0
    deleted_count = 0

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO metadata.pipeline_runs
                    (pipeline_name, run_type, status, execution_context)
                VALUES ('debezium_sqlserver_to_staging', 'INCREMENTAL',
                        'RUNNING', %s)
                RETURNING run_id
                """,
                (
                    Json(
                        {
                            "airflow_dag_id": context.get("dag").dag_id
                            if context.get("dag")
                            else "sqlserver_debezium_cdc_to_staging",
                            "airflow_run_id": context.get("run_id"),
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
                (run_id, expected_schema, connector_name),
            )
            batch_id = cursor.fetchone()[0]
        connection.commit()

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=30_000,
        )
        supported_tables = {source.casefold() for source, _ in SOURCE_TABLES}
        topics = []
        for topic in consumer.topics():
            try:
                topic_database, topic_schema, topic_table = _source_topic_parts(
                    topic, topic_prefix
                )
            except ValueError:
                continue
            if (
                topic_database.casefold() == expected_database.casefold()
                and topic_schema.casefold() == expected_schema.casefold()
                and topic_table.casefold() in supported_tables
            ):
                topics.append(topic)
        if not topics:
            raise RuntimeError(
                "No Debezium table topics are available. Register and start the "
                "SQL Server connector, then retry this DAG."
            )

        partitions = [
            TopicPartition(topic, partition)
            for topic in sorted(topics)
            for partition in sorted(consumer.partitions_for_topic(topic) or ())
        ]
        if not partitions:
            raise RuntimeError("Debezium topics exist but have no Kafka partitions.")
        consumer.assign(partitions)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT topic, kafka_partition, next_offset
                FROM metadata.kafka_offsets
                """
            )
            saved_offsets = {
                (row[0], row[1]): row[2] for row in cursor.fetchall()
            }
        beginnings = consumer.beginning_offsets(partitions)
        endings = consumer.end_offsets(partitions)
        next_offsets: dict[TopicPartition, int] = {}
        for partition in partitions:
            checkpoint = saved_offsets.get((partition.topic, partition.partition))
            beginning = beginnings[partition]
            ending = endings[partition]
            if checkpoint is None and beginning > 0:
                raise RuntimeError(
                    f"No saved checkpoint exists for {partition.topic}"
                    f"[{partition.partition}], but Kafka's earliest available "
                    f"offset is {beginning}. Verify retention and re-bootstrap "
                    "the connector before consuming."
                )
            if checkpoint is not None and checkpoint < beginning:
                raise RuntimeError(
                    f"Kafka retention passed saved offset {checkpoint} for "
                    f"{partition.topic}[{partition.partition}]; earliest available "
                    f"offset is {beginning}. Recover the missing events before "
                    "resetting this checkpoint."
                )
            if checkpoint is not None and checkpoint > ending:
                raise RuntimeError(
                    f"Saved offset {checkpoint} is beyond Kafka end offset "
                    f"{ending} for {partition.topic}[{partition.partition}]."
                )
            start_offset = beginning if checkpoint is None else checkpoint
            consumer.seek(partition, start_offset)
            next_offsets[partition] = start_offset

        deadline = time.monotonic() + max_seconds
        normalized_events: list[dict[str, Any]] = []
        while len(normalized_events) < max_records and time.monotonic() < deadline:
            polled = consumer.poll(
                timeout_ms=1_000,
                max_records=min(POLL_RECORDS, max_records - len(normalized_events)),
            )
            if not polled:
                break
            for partition, records in polled.items():
                for record in records:
                    normalized_events.append(
                        normalize_debezium_record(
                            topic=record.topic,
                            partition=record.partition,
                            offset=record.offset,
                            key=_decode_json(record.key),
                            value=_decode_json(record.value),
                            topic_prefix=topic_prefix,
                            expected_schema=expected_schema,
                            expected_database=expected_database,
                        )
                    )
                    next_offsets[partition] = record.offset + 1

        with connection.cursor() as cursor:
            for event in normalized_events:
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
                if cursor.fetchone() is None:
                    continue
                events_inserted += 1
                if event["operation"] in {"c", "r"}:
                    inserted_count += 1
                elif event["operation"] == "u":
                    updated_count += 1
                elif event["operation"] == "d":
                    deleted_count += 1
                if event["source_table"] in SCD2_TABLES:
                    _apply_scd2(cursor, event, Json)

            for partition, next_offset in next_offsets.items():
                cursor.execute(
                    """
                    INSERT INTO metadata.kafka_offsets
                        (topic, kafka_partition, next_offset, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (topic, kafka_partition) DO UPDATE
                    SET next_offset = EXCLUDED.next_offset,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (partition.topic, partition.partition, next_offset),
                )

            records_read = len(normalized_events)
            cursor.execute(
                """
                UPDATE metadata.extraction_batches
                SET status = 'SUCCESS',
                    completed_at = CURRENT_TIMESTAMP,
                    extracted_rows = %s,
                    inserted_rows = %s,
                    updated_rows = %s,
                    deleted_rows = %s
                WHERE batch_id = %s
                """,
                (
                    records_read,
                    inserted_count,
                    updated_count,
                    deleted_count,
                    batch_id,
                ),
            )
            cursor.execute(
                """
                INSERT INTO metadata.table_load_status
                    (source_schema, source_table, last_batch_id, last_success_at,
                     last_status, total_rows_loaded, updated_at)
                VALUES (%s, 'CDC_STREAM', %s, CURRENT_TIMESTAMP, 'SUCCESS',
                        %s, CURRENT_TIMESTAMP)
                ON CONFLICT (source_schema, source_table) DO UPDATE
                SET last_batch_id = EXCLUDED.last_batch_id,
                    last_success_at = EXCLUDED.last_success_at,
                    last_status = 'SUCCESS',
                    total_rows_loaded =
                        metadata.table_load_status.total_rows_loaded
                        + EXCLUDED.total_rows_loaded,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (expected_schema, batch_id, events_inserted),
            )
            cursor.execute(
                """
                UPDATE metadata.pipeline_runs
                SET status = 'SUCCESS', completed_at = CURRENT_TIMESTAMP
                WHERE run_id = %s
                """,
                (run_id,),
            )
        connection.commit()
        LOGGER.info(
            "Persisted %s Debezium records (%s new events) in batch_id=%s",
            records_read,
            events_inserted,
            batch_id,
        )
        return {"records_read": records_read, "events_inserted": events_inserted}
    except Exception as error:
        connection.rollback()
        if run_id is not None and batch_id is not None:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE metadata.extraction_batches
                        SET status = 'FAILED',
                            completed_at = CURRENT_TIMESTAMP,
                            error_message = %s
                        WHERE batch_id = %s
                        """,
                        (f"{type(error).__name__}: {error}", batch_id),
                    )
                    cursor.execute(
                        """
                        UPDATE metadata.pipeline_runs
                        SET status = 'FAILED',
                            completed_at = CURRENT_TIMESTAMP,
                            error_message = %s
                        WHERE run_id = %s
                        """,
                        (f"{type(error).__name__}: {error}", run_id),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                LOGGER.exception("Could not mark CDC batch_id=%s as failed", batch_id)
        LOGGER.exception("Debezium CDC batch failed (batch_id=%s)", batch_id)
        raise
    finally:
        if consumer is not None:
            consumer.close()
        connection.close()
