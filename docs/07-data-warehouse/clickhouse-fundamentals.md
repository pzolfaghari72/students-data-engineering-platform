# ClickHouse Fundamentals

ClickHouse is the analytical store for the enrollment model. PostgreSQL is
the mutable operational staging/control database; ClickHouse is optimized for
analytical scans and serves the Grafana queries.

The batch DDL uses the `MergeTree` family with explicit `ORDER BY` keys and
append-only snapshots. The CDC DDL also uses `MergeTree`, not
`ReplacingMergeTree`: duplicate physical CDC rows can remain after a retry.
Query-time `argMax` and `row_number` logic derives current rows without
waiting for asynchronous background merges.

The batch tables and CDC event tables are distinct. Batch views select the
latest completed snapshot; streaming views select the active generation.
Neither view makes batch and CDC writes transactionally consistent. See
[table grains](./fact-and-dimension-tables.md) and
[publication/reconciliation](./data-publication-and-reconciliation.md).

Implementation: `db/warehouse_clickhouse/warehouse_ddl.sql`,
`db/warehouse_clickhouse/cdc_warehouse_ddl.sql`, and
`spark/load_warehouse.py` / `spark/stream_cdc.py`.
