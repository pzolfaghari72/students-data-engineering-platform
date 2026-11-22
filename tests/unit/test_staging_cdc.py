import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from cdc_events import advance_offsets, completed_source_offset, normalize_debezium_record, source_version_order
from stream_cdc import ClickHouseCDC, checked_checkpoint, event_row

TOPIC = "students.Students.dbo.Students"
LSN = "00000027:000001d0:0003"


def normalized(operation="r", offset=0, key=None, **extra):
    source = {"db": "Students", "schema": "dbo", "table": "Students", "snapshot": "last",
              "commit_lsn": LSN, "change_lsn": LSN, "event_serial_no": 1}
    envelope = {"op": operation, "source": source, "after": {"StudentId": 17, "Status": "A"}}
    envelope.update(extra)
    return normalize_debezium_record(
        topic=TOPIC, partition=0, offset=offset, key=key or b'{"StudentId":17}',
        value=None if operation == "t" else json.dumps(envelope).encode(),
        topic_prefix="students", expected_schema="dbo", expected_database="Students",
    )


class DebeziumRecordTests(unittest.TestCase):
    def test_snapshot_and_live_position_order(self):
        snapshot, insert = normalized(), normalized("c")
        self.assertEqual(snapshot["source_snapshot_state"], "last")
        self.assertLess(source_version_order(snapshot), source_version_order(insert))
        self.assertLess(source_version_order(insert),
                        source_version_order({**insert, "source_event_serial_no": "2"}))

    def test_delete_and_tombstone_are_separate_records(self):
        delete = normalized("d", after=None, before={"StudentId": 17})
        self.assertEqual(delete["operation"], "d")
        self.assertEqual(delete["before_image"]["StudentId"], 17)
        self.assertEqual(normalized("t")["operation"], "t")
        self.assertEqual(event_row("g", 2, normalized("t")), event_row("g", 2, normalized("t")))

    def test_rejects_unknown_database_in_payload(self):
        with self.assertRaisesRegex(ValueError, "Unexpected source database"):
            normalized(source={"db": "another_db"})

    def test_rejects_missing_image_or_live_change_position(self):
        with self.assertRaisesRegex(ValueError, "no after image"):
            normalized("u", after=None)
        with self.assertRaisesRegex(ValueError, "change position"):
            normalized("u", source={"commit_lsn": LSN})

    def test_compatible_new_field_is_preserved(self):
        value = normalized("u", after={"StudentId": 17, "AddedColumn": "example"})
        self.assertEqual(value["after_image"]["AddedColumn"], "example")

    def test_key_serialization_is_canonical(self):
        a = event_row("g", 1, normalized(key='{"A":1,"Z":2}'))
        b = event_row("g", 1, normalized(key='{"Z":2,"A":1}'))
        self.assertEqual(a, b)


class BootstrapTests(unittest.TestCase):
    def test_durable_completion_and_late_start(self):
        self.assertEqual(completed_source_offset(
            {"snapshot": True, "snapshot_completed": True, "commit_lsn": LSN}
        ), (True, LSN))
        self.assertEqual(completed_source_offset({"commit_lsn": LSN, "change_lsn": LSN}), (True, ""))
        self.assertEqual(completed_source_offset(
            {"snapshot": True, "snapshot_completed": False, "commit_lsn": LSN}
        ), (False, ""))
        self.assertEqual(completed_source_offset({"commit_lsn": LSN}), (False, ""))

    def test_partitions_advance_without_order_or_input_mutation(self):
        begin = {f"{TOPIC}:0": 0, f"{TOPIC}:1": 3}
        self.assertEqual(advance_offsets(begin, [(TOPIC, 0, 1), (TOPIC, 1, 3), (TOPIC, 0, 0)]),
                         {f"{TOPIC}:0": 2, f"{TOPIC}:1": 4})
        self.assertEqual(begin[f"{TOPIC}:0"], 0)
        with self.assertRaisesRegex(RuntimeError, "Unexpected Kafka position"):
            advance_offsets(begin, [(TOPIC, 0, 2)])


class SinkReplayTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock()
        self.store = ClickHouseCDC(self.client, "g")
        self.row = event_row("g", 0, normalized())
        self.before = {f"{TOPIC}:0": 0}
        self.after = {f"{TOPIC}:0": 1}
        self.receipt = (TOPIC, 0, 0, self.row[-1])

    def response(self, *rows):
        return SimpleNamespace(result_rows=list(rows))

    def test_seals_only_after_verified_insert(self):
        self.client.query.side_effect = [self.response(), self.response(self.receipt)]
        self.assertEqual(self.store.seal("q", 0, [self.row], self.before, self.after), self.after)
        self.assertEqual([call.args[0] for call in self.client.insert.call_args_list],
                         ["cdc_events", "cdc_batch_commits"])

    def test_missing_sink_row_cannot_advance_progress(self):
        self.client.query.side_effect = [self.response(), self.response()]
        with self.assertRaisesRegex(RuntimeError, "failed verification"):
            self.store.seal("q", 0, [self.row], self.before, self.after)
        self.client.insert.assert_called_once()
        self.assertEqual(self.client.insert.call_args.args[0], "cdc_events")

    def test_existing_commit_is_verified_and_not_reinserted(self):
        from cdc_events import fingerprint
        digest = fingerprint([((TOPIC, 0, 0), self.row[-1])])
        self.client.query.side_effect = [
            self.response(("q", digest, self.after)),
            self.response(self.receipt),
        ]
        self.assertEqual(self.store.seal("q", 0, [self.row], self.before, self.after), self.after)
        self.client.insert.assert_not_called()

    def test_checkpoint_allows_pending_sink_commit_but_refuses_another_query(self):
        with tempfile.TemporaryDirectory() as scratch:
            path = Path(scratch)
            (path / "metadata").write_text('{"id":"q"}')
            (path / "commits").mkdir()
            (path / "commits" / "0").touch()
            checked_checkpoint(path, "q", 1)
            with self.assertRaisesRegex(RuntimeError, "different Spark checkpoint"):
                checked_checkpoint(path, "another", 1)
            with self.assertRaisesRegex(RuntimeError, "disagree"):
                checked_checkpoint(path, "", -1)
