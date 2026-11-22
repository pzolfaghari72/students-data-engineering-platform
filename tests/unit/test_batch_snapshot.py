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
    pg_errors = ModuleType("psycopg2.errors")
    pg_errors.UndefinedColumn = type("UndefinedColumn", (Exception,), {})
    pg_errors.UndefinedTable = type("UndefinedTable", (Exception,), {})
    extras = ModuleType("psycopg2.extras")
    extras.Json = Mock()
    airflow_errors = ModuleType("airflow.exceptions")
    airflow_errors.AirflowException = RuntimeError
    dependencies = {
        "psycopg2": psycopg2, "psycopg2.errors": pg_errors, "psycopg2.extras": extras,
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

    def test_old_staging_schema_fails_before_creating_a_run(self):
        for error in (self.loader.UndefinedColumn, self.loader.UndefinedTable):
            with self.subTest(error=error.__name__):
                self.cursor.reset_mock()
                self.cursor.execute.side_effect = error
                with self.assertRaisesRegex(RuntimeError, "staging-schema-init"):
                    self.loader.create_pipeline_run(run_id="manual_fixture")
                self.cursor.execute.assert_called_once()
        self.connection.commit.assert_not_called()

    def test_copy_preserves_null_empty_and_escaped_source_text(self):
        for value, expected in (
            (None, r"\N"), ("", ""), (r"\N", r"\\N"), ("a\tb\nc\rd", r"a\tb\nc\rd"),
        ):
            with self.subTest(value=value):
                self.assertEqual(self.loader._copy_field(value), expected)

    def test_snapshot_isolation_is_set_before_opening_the_transaction(self):
        self.cursor.fetchone.return_value = None
        source = MagicMock(autocommit=True)
        source_cursor = source.cursor.return_value.__enter__.return_value
        task = Mock()
        task.xcom_pull.return_value = 42

        def set_isolation(statement):
            self.assertTrue(source.autocommit)
            self.assertEqual(statement, "SET TRANSACTION ISOLATION LEVEL SNAPSHOT")

        def load_table(*args, **kwargs):
            self.assertIs(kwargs["source_connection"], source)
            self.assertFalse(source.autocommit)

        source_cursor.execute.side_effect = set_isolation
        self.loader.pyodbc.connect = Mock(return_value=source)
        with (
            patch.dict("os.environ", {"SOURCE_DB_SNAPSHOT_ISOLATION": "SNAPSHOT"}),
            patch.object(self.loader, "_sqlserver_connection_string", return_value="source"),
            patch.object(self.loader, "load_source_table", side_effect=load_table) as load,
        ):
            self.loader.load_consistent_snapshot(ti=task)

        self.loader.pyodbc.connect.assert_called_once_with("source", autocommit=True)
        self.assertEqual(load.call_count, len(SOURCE_TABLES))
        source.commit.assert_called_once()

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
