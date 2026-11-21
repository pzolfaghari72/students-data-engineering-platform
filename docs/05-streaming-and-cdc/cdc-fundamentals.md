# Change Data Capture Fundamentals

Change Data Capture (CDC) records source inserts, updates and deletes from a
database change mechanism. It is not the same as periodically selecting all
rows, and it is not the same as warehouse Slowly Changing Dimensions (SCD).

| Term | Meaning in this project |
| --- | --- |
| SQL Server CDC | Source feature that records changes in capture tables and advances through log sequence numbers (LSNs). |
| Debezium snapshot | Initial read used to establish a connector baseline; distinct from Airflow's full snapshot transaction. |
| Kafka event | Keyed JSON envelope in a source table topic with operation, row images and source metadata. |
| Kafka offset | Position within one topic partition; it is not a SQL Server LSN and does not order all topics globally. |
| SCD Type 2 | A warehouse/staging history model that preserves versions over validity intervals. The legacy Airflow CDC consumer applies this in PostgreSQL for selected entities. |
| Tombstone | Kafka record with a key and null value, used for compaction semantics; code normalizes it as `t` and does not create an SCD2 version. |

The configured event operations are `c` (create), `r` (snapshot read), `u`
(update), `d` (delete) and `t` (tombstone). The Airflow consumer stores them
in PostgreSQL; Spark streaming is configured to store event history in
PostgreSQL and ClickHouse. Neither is validated end-to-end in the current
runtime. See [event processing](./event-processing.md) and the canonical
[CDC diagrams](../02-architecture/diagrams/streaming-pipeline.md).
