# ClickHouse fundamentals

ClickHouse is a column-oriented analytical database. It can read only the columns used in a query, compress similar values and process aggregates across many rows. These properties fit enrollment counts, capacity summaries and grade distributions.

The project's physical data and metadata tables use the MergeTree engine. MergeTree stores inserts in sorted parts and merges parts in the background. The `ORDER BY` tuple supports access patterns and data skipping. It does not enforce uniqueness. The batch tables sort around snapshot IDs and analytical keys. CDC event storage sorts around generation, Spark batch and transport position.

Views expose the stable interfaces used by Grafana. A batch view filters to the latest valid publication marker. A CDC view selects sealed records in the active generation, then derives current state or aggregates. This makes the publication rule explicit rather than relying on background merges. Writer accounts handle their own database, and the read role receives specific serving views.

Investigate slow queries by comparing scans, grouping size, thread limits, memory and disk spill. A columnar store can still use substantial memory to group millions of distinct keys. [MergeTree reference](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree) · [Practical resource limits](../03-infrastructure/resource-management.md)
