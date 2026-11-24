"""Fast checks for the incremental model's input and dashboard contracts."""

from datetime import date
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import unittest

from cdc_events import fingerprint, source_version_order
from incremental_model import SOURCES, narrow_image, project_row, typed_value
from incremental_warehouse import Seal, checked_events
from students_staging.source_tables import SOURCE_TABLES
from warehouse_keys import pseudonymize_identifier


ROOT = Path(__file__).resolve().parents[2]
LSN = "00000001:00000001:0001"


def source_row(offset, *, name="Computer Science", operation="c"):
    version = source_version_order({
        "operation": operation, "source_commit_lsn": LSN,
        "source_change_lsn": LSN, "source_event_serial_no": 1,
    })
    return (
        "topic", 0, offset, f"hash-{offset}", "Departments", '{"DepartmentCode":"CS"}',
        version, LSN, 1, operation, json.dumps({"DepartmentCode": "CS", "DepartmentName": name}),
    )


def seal(rows):
    positions = {(row[0], row[1], row[2]): row[3] for row in rows}
    return Seal(0, "checkpoint", fingerprint(sorted(positions.items())), len(positions))


class InputTests(unittest.TestCase):
    def test_all_assessed_tables_have_relay_specs(self):
        self.assertEqual(set(SOURCES), {source for source, _ in SOURCE_TABLES})

    def test_replayed_logical_change_keeps_earliest_transport(self):
        rows = [source_row(5), source_row(6)]
        self.assertEqual(len(checked_events(rows, seal(rows), LSN)), 1)

    def test_duplicate_transport_position_fails_closed(self):
        rows = [source_row(5), source_row(5, name="Another")]
        with self.assertRaisesRegex(RuntimeError, "transport position"):
            checked_events(rows, seal(rows), LSN)

    def test_conflicting_logical_version_fails_closed(self):
        rows = [source_row(5), source_row(6, name="Another")]
        with self.assertRaisesRegex(RuntimeError, "conflicting analytical"):
            checked_events(rows, seal(rows), LSN)

    def test_rejects_unsealed_event_range(self):
        rows = [source_row(5), source_row(6)]
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            checked_events(rows[:1], seal(rows), LSN)

    def test_excludes_early_live_event_and_tombstone(self):
        rows = [source_row(5), source_row(6, operation="t")]
        self.assertEqual(checked_events(rows, seal(rows), "00000002:00000001:0001"), [])

    def test_empty_seal(self):
        self.assertEqual(checked_events([], seal([]), LSN), [])

    def test_projects_no_unused_personal_attributes(self):
        image = {
            "StudentId": 17, "StudentStatus": "A", "DegreePursued": 19,
            "ClassStandingCode": None, "EnrollmentTerm": "FA2015", "FirstName": "Example",
        }
        minimized = narrow_image("Students", {"StudentId": 17}, image)
        self.assertNotIn("FirstName", minimized)
        version = source_version_order({"operation": "r", "source_commit_lsn": LSN})
        _, row = project_row(("Students", version, LSN, 0, "r", minimized, 0), "secret", "run", "gen")
        self.assertEqual(row[8], pseudonymize_identifier("student", 17, "secret"))

    def test_debezium_decimal_and_date_encodings(self):
        self.assertEqual(typed_value("Credits", "Decimal(3, 1)", "Hg==", ""), Decimal("3.0"))
        self.assertEqual(typed_value("Credits", "Decimal(3, 1)", "/w==", ""), Decimal("-0.1"))
        self.assertEqual(typed_value("Gpa", "Decimal(3, 2)", 3.65, ""), Decimal("3.65"))
        self.assertEqual(typed_value("ApplicationReceivedDate", "Date", 10, ""), date(1970, 1, 11))


class DashboardTests(unittest.TestCase):
    def test_render_preserves_panels_and_filters_and_updates_all_targets(self):
        spec = importlib.util.spec_from_file_location(
            "incremental_dashboard", ROOT / "scripts/render_incremental_dashboard.py",
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        source = json.loads((ROOT / "grafana/dashboards/enrollment-performance.json").read_text())
        dashboard = module.render(json.loads(json.dumps(source)), preview=True)
        self.assertEqual(dashboard["title"], "Incremental CDC— Student Enrollment, Capacity & Grades")
        panels = [panel for panel in dashboard["panels"] if panel.get("targets")]
        self.assertEqual(len(panels), 18)
        self.assertEqual([item["gridPos"] for item in source["panels"]],
                         [item["gridPos"] for item in dashboard["panels"]])
        for panel in panels:
            for target in panel["targets"]:
                self.assertIn("incremental_", target["rawSql"])
                self.assertIsNone(module.MODEL_TABLE.search(target["rawSql"]))
        for before, after in zip(source["templating"]["list"], dashboard["templating"]["list"]):
            self.assertEqual(before["current"], after["current"])
            self.assertIn("incremental_", after["query"])
        self.assertIn(dashboard["uid"], dashboard["links"][0]["url"])
        self.assertEqual(json.dumps(dashboard).count("/d/students-enrollment-kpis"), 1)
        self.assertEqual(dashboard["links"][-1]["title"], "Batch Snapshot")
        self.assertIn("?var-term=${__data.fields", json.dumps(dashboard))
