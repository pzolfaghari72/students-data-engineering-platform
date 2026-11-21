import json
import unittest
from datetime import UTC, datetime

from students_staging.cdc_loader import (
    advance_consumed_offsets,
    normalize_debezium_record,
    persisted_operation_counts,
    scd2_transition,
    snapshot_boundary_consumed,
    source_version_order,
)


class DebeziumRecordTests(unittest.TestCase):
    def test_normalizes_initial_snapshot_record(self) -> None:
        event = normalize_debezium_record(
            topic="students.Students.dbo.Students",
            partition=0,
            offset=12,
            key=b'{"StudentId":17}',
            value=json.dumps(
                {
                    "before": None,
                    "after": {"StudentId": 17, "FirstName": "Ada"},
                    "source": {
                        "schema": "dbo",
                        "table": "Students",
                        "commit_lsn": "00000027:000001d0:0003",
                        "change_lsn": "00000027:000001d0:0004",
                        "event_serial_no": 1,
                        "snapshot": "last",
                    },
                    "op": "r",
                    "ts_ms": "1700000000000",
                }
            ).encode(),
            topic_prefix="students",
            expected_schema="dbo",
            expected_database="Students",
        )

        self.assertEqual(event["source_table"], "Students")
        self.assertEqual(event["operation"], "r")
        self.assertEqual(event["source_key"], {"StudentId": 17})
        self.assertEqual(event["source_lsn"], "00000027:000001d0:0003")
        self.assertEqual(
            event["source_commit_lsn"], "00000027:000001d0:0003"
        )
        self.assertEqual(
            event["source_change_lsn"], "00000027:000001d0:0004"
        )
        self.assertEqual(event["source_event_serial_no"], "1")
        self.assertEqual(event["source_snapshot_state"], "last")
        self.assertLess(
            source_version_order(event),
            source_version_order(
                {
                    **event,
                    "source_change_lsn": "00000027:000001d0:0005",
                }
            ),
        )
        self.assertEqual(
            event["source_timestamp"],
            datetime.fromtimestamp(1_700_000_000, tz=UTC),
        )

    def test_rejects_unexpected_topic_schema(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unexpected source schema"):
            normalize_debezium_record(
                topic="students.Students.other.Students",
                partition=0,
                offset=0,
                key={"StudentId": 17},
                value={"op": "r", "after": {"StudentId": 17}},
                topic_prefix="students",
                expected_schema="dbo",
                expected_database="Students",
            )

    def test_rejects_snapshot_without_after_image(self) -> None:
        with self.assertRaisesRegex(ValueError, "no after image"):
            normalize_debezium_record(
                topic="students.Students.dbo.Students",
                partition=0,
                offset=0,
                key={"StudentId": 17},
                value={"op": "r", "after": None},
                topic_prefix="students",
                expected_schema="dbo",
                expected_database="Students",
            )


class Scd2TransitionTests(unittest.TestCase):
    def test_new_and_changed_rows_create_versions(self) -> None:
        current = {"attributes": {"StudentId": 17}, "is_deleted": False}
        changed, attributes, is_deleted = scd2_transition(
            current,
            {"operation": "u", "after_image": {"StudentId": 17, "City": "Paris"}},
        )

        self.assertTrue(changed)
        self.assertEqual(attributes, {"StudentId": 17, "City": "Paris"})
        self.assertFalse(is_deleted)

    def test_unchanged_rows_and_tombstones_do_not_create_versions(self) -> None:
        current = {"attributes": {"StudentId": 17}, "is_deleted": False}

        self.assertEqual(
            scd2_transition(
                current,
                {"operation": "u", "after_image": {"StudentId": 17}},
            ),
            (False, None, False),
        )
        self.assertEqual(
            scd2_transition(
                current,
                {"operation": "t", "before_image": None, "after_image": None},
            ),
            (False, None, False),
        )

    def test_delete_creates_a_deleted_version(self) -> None:
        current = {"attributes": {"StudentId": 17}, "is_deleted": False}

        self.assertEqual(
            scd2_transition(
                current,
                {"operation": "d", "before_image": {"StudentId": 17}},
            ),
            (True, {"StudentId": 17}, True),
        )


class SnapshotBoundaryTests(unittest.TestCase):
    def test_requires_capture_and_consumption_through_each_partition_end(self) -> None:
        ends = {"students.Students.dbo.Students:0": 15, "students.Students.dbo.Courses:0": 9}
        consumed = {"students.Students.dbo.Students:0": 15}

        self.assertFalse(snapshot_boundary_consumed(ends, consumed, False))
        self.assertFalse(snapshot_boundary_consumed(ends, consumed, True))
        consumed["students.Students.dbo.Courses:0"] = 9
        self.assertTrue(snapshot_boundary_consumed(ends, consumed, True))

    def test_offsets_advance_monotonically_across_replayed_records(self) -> None:
        initial = {"students.Students.dbo.Students:0": 20}
        updated = advance_consumed_offsets(
            initial,
            [
                {"kafka_topic": "students.Students.dbo.Students", "kafka_partition": 0, "kafka_offset": 18},
                {"kafka_topic": "students.Students.dbo.Students", "kafka_partition": 0, "kafka_offset": 21},
            ],
        )

        self.assertEqual(initial["students.Students.dbo.Students:0"], 20)
        self.assertEqual(updated["students.Students.dbo.Students:0"], 22)
        self.assertTrue(snapshot_boundary_consumed({}, {}, True))


class PersistedOperationCountTests(unittest.TestCase):
    def test_replayed_events_are_not_counted_as_new_operations(self) -> None:
        events = [
            ("c", True),
            ("r", True),
            ("u", False),
            ("d", True),
            ("t", True),
        ]
        deltas = [
            persisted_operation_counts(operation, inserted)
            for operation, inserted in events
        ]
        counts = tuple(sum(delta[index] for delta in deltas) for index in range(3))

        self.assertEqual(counts, (2, 0, 1))


if __name__ == "__main__":
    unittest.main()
