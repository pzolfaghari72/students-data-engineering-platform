"""Normalize the SQL Server Debezium record contract."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from students_staging.source_tables import SOURCE_TABLES

UTC = timezone.utc
TABLES = {source.casefold(): source for source, _ in SOURCE_TABLES}
LSN = re.compile(r"[0-9A-Fa-f]{8}:[0-9A-Fa-f]{8}:[0-9A-Fa-f]{4}")


def decode_json(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
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
        return datetime.fromtimestamp(0, tz=UTC)
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
    if topic_schema.casefold() != expected_schema.casefold():
        raise ValueError(f"Unexpected source schema in topic {topic}.")
    if topic_database.casefold() != expected_database.casefold():
        raise ValueError(f"Unexpected source database in topic {topic}.")
    source_key = decode_json(key)
    if isinstance(source_key, dict) and "payload" in source_key:
        source_key = source_key["payload"]
    if not isinstance(source_key, dict) or not source_key or any(
        field is None for field in source_key.values()
    ):
        raise ValueError(f"Kafka record {topic}[{partition}]@{offset} has no row key.")

    decoded_value = decode_json(value)
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
    canonical_table = TABLES.get(source_table.casefold())
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
    if operation != "t" and not valid_lsn(commit_lsn):
        raise ValueError(f"Missing or invalid SQL Server commit LSN in {topic}[{partition}]@{offset}.")
    if operation in {"c", "u", "d"} and (
        not valid_lsn(change_lsn) or type(event_serial_no) is not int or event_serial_no < 0
    ):
        raise ValueError(f"Missing or invalid SQL Server change position in {topic}[{partition}]@{offset}.")
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
        f"{0 if event['operation'] == 'r' else 1}:"
        f"{lsn_number(change_lsn):025d}:"
        f"{serial:020d}"
    )


def valid_lsn(value: Any) -> bool:
    return isinstance(value, str) and LSN.fullmatch(value) is not None


def completed_source_offset(offset: dict[str, Any]) -> tuple[bool, str]:
    """Return completion evidence and the baseline LSN if Connect still has it."""
    commit = offset.get("commit_lsn")
    if not valid_lsn(commit):
        return False, ""
    if offset.get("snapshot") is True:
        if offset.get("snapshot_completed") is True:
            return True, commit.lower()
        return False, ""
    if offset.get("snapshot") in (None, False) and valid_lsn(offset.get("change_lsn")):
        # A live offset establishes the transition. The retained 'last' record
        # supplies the baseline LSN once the corresponding Kafka cut is loaded.
        return True, ""
    return False, ""


def json_text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) if value is not None else ""


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json_text(value).encode("utf-8")).hexdigest()


def advance_offsets(previous: dict[str, int], positions: list[tuple[str, int, int]]) -> dict[str, int]:
    """Advance per-partition positions after verifying a contiguous delete-only log."""
    next_offsets = previous.copy()
    for topic, partition, offset in sorted(positions):
        key = f"{topic}:{partition}"
        if key not in previous or offset != next_offsets[key]:
            raise RuntimeError(f"Unexpected Kafka position {key}@{offset}.")
        next_offsets[key] += 1
    return next_offsets
