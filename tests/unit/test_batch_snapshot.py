import importlib.util
import sys
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock, Mock, patch

from students_staging.source_tables import SOURCE_TABLES


def load_extraction_module():
    # Host unit tests do not install the Airflow image's providers or DB clients.
    psycopg2 = ModuleType("psycopg2")
    psycopg2.sql = Mock()
    extras = ModuleType("psycopg2.extras")
    extras.Json, extras.execute_values = Mock(), Mock()
    airflow_errors = ModuleType("airflow.exceptions")
    airflow_errors.AirflowException = RuntimeError
    dependencies = {
        "psycopg2": psycopg2, "psycopg2.extras": extras,
        "pyodbc": ModuleType("pyodbc"), "airflow.exceptions": airflow_errors,
    }
    filename = Path(__file__).resolve().parents[2] / "airflow/dags/students_staging/loader.py"
    spec = importlib.util.spec_from_file_location("snapshot_loader_fixture", filename)
    loader = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, dependencies):
        spec.loader.exec_module(loader)
    return loader


class SnapshotFinalizationTests(unittest.TestCase):
    def setUp(self):
        self.loader = load_extraction_module()
        self.connection = MagicMock()
        self.loader._staging_connection = Mock(return_value=self.connection)
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.cursor.fetchall.return_value = [
            (table, index, "SUCCESS", True) for index, (table, _) in enumerate(SOURCE_TABLES, 1)
        ]
        self.manifest = [{"source_table": table, "batch_id": idx}
                         for table, idx, _, _ in self.cursor.fetchall.return_value]

    def finish(self, manifest):
        task = Mock()
        task.xcom_pull.side_effect = [42, manifest]
        return self.loader.finish_pipeline_run(ti=task)

    def test_success_returns_the_exact_run_id(self):
        self.assertEqual(self.finish(self.manifest), 42)
        self.assertEqual(self.cursor.execute.call_args.args[1], ("SUCCESS", None, 42))

    def test_all_tables_written_without_extraction_completion_fails(self):
        with self.assertRaisesRegex(RuntimeError, "completion manifest"):
            self.finish(None)
        self.assertEqual(self.cursor.execute.call_args.args[1][0], "PARTIAL")

    def test_manifest_from_wrong_batch_cannot_publish(self):
        self.manifest[0]["batch_id"] = 999
        with self.assertRaises(RuntimeError):
            self.finish(self.manifest)
        self.connection.commit.assert_called_once()

    def test_duplicate_or_unreconciled_rows_cannot_publish(self):
        self.cursor.fetchall.return_value.append(self.cursor.fetchall.return_value[0])
        with self.assertRaises(RuntimeError):
            self.finish(self.manifest)
