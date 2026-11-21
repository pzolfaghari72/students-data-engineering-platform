# Event Processing

The normalization helper is `normalize_debezium_record()` in
`airflow/dags/students_staging/cdc_loader.py`. It validates topic database,
schema and table against the assessed table list and extracts the keyed
Debezium envelope.

| Field | Meaning and use |
| --- | --- |
| `op` | `c`, `r`, `u`, `d`; identifies create, snapshot read, update or delete. A null Kafka value maps to tombstone `t`. |
| `key` | Source primary key; required and retained as JSON. |
| `before`, `after` | Row images; snapshots/creates/updates require an after image. Deletes use before image for tombstone/version attributes. |
| `source.commit_lsn` | Transaction commit version from the source connector. |
| `source.change_lsn` | Source change ordering value used when available. |
| `source.event_serial_no` | Additional intra-transaction event ordering value. |
| `source.snapshot` | Snapshot marker retained as text, such as `last`. |
| `source.ts_ms` / envelope `ts_ms` | Timestamp for event history/validity; a wall-clock fallback exists if both are missing. |
| Kafka topic/partition/offset | Stable transport identity and deduplication key. |

`source_version_order()` converts LSN components into a fixed-width sortable
string for query-time ClickHouse ordering. Do not sort separate topic
partitions by Kafka offset alone, and do not treat event timestamps as a
global transaction order. PostgreSQL SCD2 logic is implemented by the legacy
Airflow consumer; Spark streaming writes history and ClickHouse CDC events,
not PostgreSQL `scd.entity_versions`.

The test cases are in `tests/unit/test_staging_cdc.py`; they validate selected
normalization, ordering and transition behavior, not live serialization or
source connector output.
