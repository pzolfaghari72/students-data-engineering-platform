"""Shared SQL Server-to-PostgreSQL staging load and metadata operations."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

import psycopg2
import pyodbc
from airflow.exceptions import AirflowException
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from students_staging.source_tables import SOURCE_TABLES

LOGGER = logging.getLogger(__name__)

DAG_ID = "sqlserver_to_postgres_staging"
FETCH_SIZE = 5_000
LOG_EVERY_ROWS = 50_000


def _required_env(name: str) -> str:
    """Return a required environment setting without logging its value."""
    value = os.getenv(name)
    if not value:
        raise AirflowException(f"Required environment variable {name} is not set.")
    return value


def _quote_odbc_value(value: str) -> str:
    """Escape a value for use inside an ODBC connection-string brace pair."""
    return "{" + value.replace("}", "}}") + "}"


def _sqlserver_connection_string() -> str:
    """Build the SQL Server ODBC connection string from Compose settings."""
    driver = _required_env("SOURCE_DB_ODBC_DRIVER")
    host = _required_env("SOURCE_DB_HOST")
    port = _required_env("SOURCE_DB_PORT")
    database = _required_env("SOURCE_DB_NAME")
    user = _required_env("SOURCE_DB_USER")
    password = _required_env("SOURCE_DB_PASSWORD")
    encrypt = _required_env("SOURCE_DB_ENCRYPT")
    trust_certificate = _required_env("SOURCE_DB_TRUST_SERVER_CERTIFICATE")
    timeout = _required_env("SOURCE_DB_CONNECTION_TIMEOUT")
    isolation = _required_env("SOURCE_DB_SNAPSHOT_ISOLATION")
    if isolation != "SNAPSHOT":
        raise AirflowException(
            "SOURCE_DB_SNAPSHOT_ISOLATION must be SNAPSHOT for consistent extracts."
        )

    return (
        f"DRIVER={_quote_odbc_value(driver)};"
        f"SERVER={_quote_odbc_value(f'{host},{port}')};"
        f"DATABASE={_quote_odbc_value(database)};"
        f"UID={_quote_odbc_value(user)};"
        f"PWD={_quote_odbc_value(password)};"
        f"Encrypt={_quote_odbc_value(encrypt)};"
        f"TrustServerCertificate={_quote_odbc_value(trust_certificate)};"
        f"Connection Timeout={_quote_odbc_value(timeout)};"
    )


def _staging_connection() -> psycopg2.extensions.connection:
    """Connect to the staging database over the internal Compose network."""
    return psycopg2.connect(
        host=_required_env("STAGING_DB_HOST"),
        port=int(_required_env("STAGING_DB_PORT")),
        dbname=_required_env("STAGING_DB_NAME"),
        user=_required_env("STAGING_DB_USER"),
        password=_required_env("STAGING_DB_PASSWORD"),
    )


def _quote_sqlserver_identifier(identifier: str) -> str:
    """Quote a SQL Server schema or table identifier."""
    return "[" + identifier.replace("]", "]]") + "]"


def _run_id_from_context(context: dict[str, Any]) -> int:
    """Read the pipeline run ID created by the start task."""
    run_id = context["ti"].xcom_pull(task_ids="create_pipeline_run")
    if not isinstance(run_id, int):
        raise AirflowException("The pipeline run ID is missing from task XCom.")
    return run_id


def create_pipeline_run(**context: Any) -> int:
    """Create one metadata row shared by all table loads in this DAG run."""
    connection = _staging_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO metadata.pipeline_runs
                    (pipeline_name, run_type, status, execution_context)
                VALUES (%s, 'FULL', 'RUNNING', %s)
                RETURNING run_id
                """,
                (
                    DAG_ID,
                    Json(
                        {
                            "airflow_dag_id": DAG_ID,
                            "airflow_run_id": context["run_id"],
                        }
                    ),
                ),
            )
            run_id = cursor.fetchone()[0]
        connection.commit()
        LOGGER.info("Created pipeline run_id=%s", run_id)
        return run_id
    finally:
        connection.close()


def load_source_table(
    source_table: str,
    target_table: str,
    source_connection: pyodbc.Connection,
    **context: Any,
) -> dict[str, int | str]:
    """Copy one table in bounded chunks using the supplied source transaction."""
    run_id = _run_id_from_context(context)
    source_schema = _required_env("SOURCE_DB_SCHEMA")
    batch_id: int | None = None
    source_cursor: pyodbc.Cursor | None = None
    target_connection = _staging_connection()
    loaded_rows = 0
    extracted_at = datetime.now(UTC)

    try:
        # Create the batch first so even a source connection failure is traceable.
        with target_connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO metadata.extraction_batches
                    (run_id, source_schema, source_table, extraction_type, status)
                VALUES (%s, %s, %s, 'SNAPSHOT', 'RUNNING')
                RETURNING batch_id
                """,
                (run_id, source_schema, source_table),
            )
            batch_id = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO metadata.table_load_status
                    (source_schema, source_table, last_batch_id,
                     last_status, total_rows_loaded, updated_at)
                VALUES (%s, %s, %s, 'RUNNING', 0, CURRENT_TIMESTAMP)
                ON CONFLICT (source_schema, source_table) DO UPDATE
                SET last_batch_id = EXCLUDED.last_batch_id,
                    last_status = 'RUNNING',
                    updated_at = CURRENT_TIMESTAMP
                """,
                (source_schema, source_table, batch_id),
            )
        target_connection.commit()
        LOGGER.info(
            "Starting source table %s.%s into raw.%s (batch_id=%s)",
            source_schema,
            source_table,
            target_table,
            batch_id,
        )

        qualified_source = (
            f"{_quote_sqlserver_identifier(source_schema)}."
            f"{_quote_sqlserver_identifier(source_table)}"
        )
        source_cursor = source_connection.cursor()
        source_cursor.execute(f"SELECT COUNT_BIG(*) FROM {qualified_source}")
        source_rows_expected = int(source_cursor.fetchone()[0])
        source_cursor.execute(f"SELECT * FROM {qualified_source}")
        if source_cursor.description is None:
            raise AirflowException(
                f"SQL Server returned no columns for {source_schema}.{source_table}."
            )

        source_columns = [column[0] for column in source_cursor.description]
        insert_columns = source_columns + ["_batch_id", "_extracted_at"]
        insert_statement = sql.SQL(
            "INSERT INTO {}.{} ({}) VALUES %s"
        ).format(
            sql.Identifier("raw"),
            sql.Identifier(target_table),
            sql.SQL(", ").join(sql.Identifier(column) for column in insert_columns),
        ).as_string(target_connection)

        while True:
            source_rows = source_cursor.fetchmany(FETCH_SIZE)
            if not source_rows:
                break

            target_rows = [
                tuple(row) + (batch_id, extracted_at)
                for row in source_rows
            ]
            try:
                with target_connection.cursor() as cursor:
                    execute_values(
                        cursor,
                        insert_statement,
                        target_rows,
                        page_size=1_000,
                    )
                    cursor.execute(
                        """
                        UPDATE metadata.extraction_batches
                        SET extracted_rows = extracted_rows + %s,
                            inserted_rows = inserted_rows + %s
                        WHERE batch_id = %s
                        """,
                        (len(target_rows), len(target_rows), batch_id),
                    )
                target_connection.commit()
            except Exception:
                target_connection.rollback()
                raise

            loaded_rows += len(target_rows)
            if loaded_rows % LOG_EVERY_ROWS < len(target_rows):
                LOGGER.info(
                    "Loaded %s rows from %s.%s (batch_id=%s)",
                    loaded_rows,
                    source_schema,
                    source_table,
                    batch_id,
                )

        # Mark the batch complete and update the latest successful table status.
        with target_connection.cursor() as cursor:
            cursor.execute(
                sql.SQL('SELECT count(*) FROM raw.{} WHERE "_batch_id" = %s').format(
                    sql.Identifier(target_table)
                ),
                (batch_id,),
            )
            staged_rows = int(cursor.fetchone()[0])
            if not source_rows_expected == loaded_rows == staged_rows:
                raise AirflowException(
                    f"Reconciliation failed for {source_table} batch {batch_id}: "
                    f"source={source_rows_expected}, fetched={loaded_rows}, staged={staged_rows}."
                )
            cursor.execute(
                """
                UPDATE metadata.extraction_batches
                SET status = 'SUCCESS',
                    completed_at = CURRENT_TIMESTAMP,
                    source_rows_expected = %s,
                    inserted_rows = %s
                WHERE batch_id = %s
                """,
                (source_rows_expected, staged_rows, batch_id),
            )
            cursor.execute(
                """
                INSERT INTO metadata.table_load_status
                    (source_schema, source_table, last_batch_id, last_success_at,
                     last_status, total_rows_loaded, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'SUCCESS', %s, CURRENT_TIMESTAMP)
                ON CONFLICT (source_schema, source_table) DO UPDATE
                SET last_batch_id = EXCLUDED.last_batch_id,
                    last_success_at = EXCLUDED.last_success_at,
                    last_status = 'SUCCESS',
                    total_rows_loaded =
                        metadata.table_load_status.total_rows_loaded
                        + EXCLUDED.total_rows_loaded,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (source_schema, source_table, batch_id, loaded_rows),
            )
        target_connection.commit()
        LOGGER.info(
            "Completed source table %s.%s: %s rows loaded (batch_id=%s)",
            source_schema,
            source_table,
            loaded_rows,
            batch_id,
        )
        return {
            "source_table": source_table,
            "batch_id": batch_id,
            "rows_loaded": loaded_rows,
        }
    except Exception as error:
        if batch_id is not None:
            try:
                target_connection.rollback()
                with target_connection.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE metadata.extraction_batches
                        SET status = 'FAILED',
                            completed_at = CURRENT_TIMESTAMP,
                            error_message = %s
                        WHERE batch_id = %s
                        """,
                        (type(error).__name__, batch_id),
                    )
                    cursor.execute(
                        """
                        INSERT INTO metadata.table_load_status
                            (source_schema, source_table, last_batch_id,
                             last_status, total_rows_loaded, updated_at)
                        VALUES (%s, %s, %s, 'FAILED', 0, CURRENT_TIMESTAMP)
                        ON CONFLICT (source_schema, source_table) DO UPDATE
                        SET last_batch_id = EXCLUDED.last_batch_id,
                            last_status = 'FAILED',
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (source_schema, source_table, batch_id),
                    )
                target_connection.commit()
            except Exception:
                target_connection.rollback()
                LOGGER.exception(
                    "Could not mark batch_id=%s as failed", batch_id
                )
        LOGGER.error(
            "Failed loading source table %s.%s (batch_id=%s, error=%s)",
            source_schema,
            source_table,
            batch_id,
            type(error).__name__,
        )
        raise AirflowException(f"Snapshot extraction failed for {source_table}.") from None
    finally:
        if source_cursor is not None:
            source_cursor.close()
        target_connection.close()


def load_consistent_snapshot(**context: Any) -> list[dict[str, int | str]]:
    """Extract every source table through one SQL Server snapshot transaction."""
    run_id = _run_id_from_context(context)
    staging = _staging_connection()
    try:
        with staging.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM metadata.extraction_batches WHERE run_id = %s LIMIT 1",
                (run_id,),
            )
            if cursor.fetchone() is not None:
                raise AirflowException("Start a new snapshot run to retry extraction.")
    finally:
        staging.close()
    results: list[dict[str, int | str]] = []
    connection = None
    try:
        connection = pyodbc.connect(_sqlserver_connection_string(), autocommit=False)
        with connection.cursor() as cursor:
            cursor.execute(
                f"SET TRANSACTION ISOLATION LEVEL "
                f"{_required_env('SOURCE_DB_SNAPSHOT_ISOLATION')}"
            )
        for source_table, target_table in SOURCE_TABLES:
            results.append(
                load_source_table(
                    source_table,
                    target_table,
                    source_connection=connection,
                    **context,
                )
            )
        connection.commit()
        return results
    except Exception as error:
        if connection is not None:
            connection.rollback()
        LOGGER.error("Snapshot run %s failed (%s).", run_id, type(error).__name__)
        raise AirflowException(f"Snapshot run {run_id} failed.") from None
    finally:
        if connection is not None:
            connection.close()


def finish_pipeline_run(**context: Any) -> int:
    """Set the overall run status after every table task has finished."""
    run_id = _run_id_from_context(context)
    results = context["ti"].xcom_pull(task_ids="load_consistent_snapshot")
    # The extractor returns its manifest only after closing the source transaction.
    manifest = {
        (item["source_table"], item["batch_id"]) for item in (results or ())
    }
    expected_tables = {source_table for source_table, _ in SOURCE_TABLES}
    connection = _staging_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT source_table, batch_id, status,
                       source_rows_expected = extracted_rows
                       AND source_rows_expected = inserted_rows AS reconciled
                FROM metadata.extraction_batches
                WHERE run_id = %s
                """,
                (run_id,),
            )
            rows = cursor.fetchall()
            recorded_batches = len(rows)
            verified = {
                (table, batch_id)
                for table, batch_id, status, reconciled in rows
                if status == "SUCCESS" and reconciled
            }
            successful_batches = len(verified)

            if (
                recorded_batches == len(expected_tables)
                and manifest == verified
                and {table for table, _ in verified} == expected_tables
            ):
                status = "SUCCESS"
                error_message = None
            elif successful_batches:
                status = "PARTIAL"
                error_message = (
                    f"{successful_batches} of {len(SOURCE_TABLES)} table loads "
                    f"reconciled; {recorded_batches} recorded. "
                    "The extraction completion manifest is missing or invalid."
                )
            else:
                status = "FAILED"
                error_message = (
                    f"No table loads succeeded; {recorded_batches} batches "
                    "were recorded."
                )

            cursor.execute(
                """
                UPDATE metadata.pipeline_runs
                SET status = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    error_message = %s
                WHERE run_id = %s AND status <> 'SUCCESS'
                """,
                (status, error_message, run_id),
            )
        connection.commit()
        LOGGER.info(
            "Pipeline run_id=%s finished with status=%s (%s/%s tables succeeded)",
            run_id,
            status,
            successful_batches,
            len(SOURCE_TABLES),
        )

        if status != "SUCCESS":
            raise AirflowException(
                error_message
                or f"Pipeline run_id={run_id} finished with status={status}."
            )
        return run_id
    finally:
        connection.close()
