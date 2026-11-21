# Source Extraction

The batch extractor uses `pyodbc` with Microsoft ODBC Driver 18 inside the
Airflow image. It executes `SELECT *` over the explicit table list in
`students_staging/source_tables.py`; it does not use JDBC or an incremental
watermark.

## Execution characteristics

- The Airflow DAG is manually triggered and finite.
- One ODBC connection is shared across the ordered 15-table loop.
- Rows are fetched in chunks of 5,000; PostgreSQL `execute_values` inserts
  bounded pages and commits each fetched chunk.
- Every table gets a `metadata.extraction_batches` row. Raw records retain
  `_batch_id` and `_extracted_at`; earlier batches are not replaced.
- The source SQL transaction commits after all 15 tables complete. If an
  exception occurs, the source transaction rolls back; already committed
  PostgreSQL chunks remain associated with a failed batch.

The implementation is in `airflow/dags/students_staging/loader.py`. It quotes
SQL Server identifiers and constructs its ODBC connection from required
environment variables. No source-specific connection credential belongs in
code. See the [snapshot consistency guide](./snapshot-consistency.md) before
running against a live database.
