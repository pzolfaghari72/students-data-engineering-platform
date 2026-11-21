# Batch Architecture

The batch path is a finite workflow with two manually triggered Airflow DAGs.
The first extracts all assessed tables through one SQL Server `SNAPSHOT`
transaction. After the staging results are verified, the second runs Spark to
transform a successful PostgreSQL snapshot into a ClickHouse warehouse
generation.

## Current implementation

1. `sqlserver_to_postgres_staging` creates a pipeline-run row.
2. One bounded Python task opens one ODBC connection with autocommit disabled,
   sets the configured isolation level to `SNAPSHOT`, and reads the 15 tables
   sequentially using that same connection.
3. Each table is written in batches to its typed `raw` table and tracked in
   `metadata.extraction_batches`; a final task records `SUCCESS`, `PARTIAL`,
   or `FAILED`.
4. An operator separately triggers `postgres_to_clickhouse_warehouse` after
   confirming the staging run and all expected table batches succeeded.
5. Its Spark task selects the latest successful full run, reads the source
   tables required by the nine dimensions and three facts, and validates that
   all joins preserve each fact's source-row grain.
6. Spark appends versioned rows to new `*_v2_snapshots` tables and inserts
   `warehouse_model_loads_v2` last. Published views select only that completed
   model snapshot; existing physical tables are retained.

The SQL Server setting `ALLOW_SNAPSHOT_ISOLATION` is currently reported OFF,
so the extraction code is prepared but source execution is externally
blocked. A partial batch can leave committed rows and is not valid warehouse
input.

The Airflow scheduler image includes Java 17 and PySpark 3.5.6. Spark uses
`local[2]` with a 768 MiB driver heap. The existing one-shot Compose Spark
service remains available for manual warehouse runs. It is an alternative to
the warehouse DAG; do not run both at the same time.

Implementation: `airflow/dags/sqlserver_to_postgres_staging.py`,
`airflow/dags/postgres_to_clickhouse_warehouse.py`,
`airflow/dags/students_staging/loader.py`,
`airflow/dags/students_staging/source_tables.py`,
`spark/load_warehouse.py`, and the two PostgreSQL/ClickHouse DDL directories.
See the [canonical batch diagrams](./diagrams/batch-pipeline.md) and
[snapshot consistency](../04-data-ingestion/snapshot-consistency.md).
