# ADR-006 — Checkpointed CDC with verified ClickHouse publication

**Status:** Accepted

Use Debezium on Kafka Connect with an initial snapshot and SQL Server CDC positions. Keep ordered schema history and Connect state in Kafka. A single Spark query reads the configured table topics and resumes through its own durable checkpoint.

Seal a microbatch only after reconciling ClickHouse with its Kafka positions and normalized record hashes. Publish the generation after all saved bootstrap offsets pass the same check. Distinguish a Kafka-position replay from a repeated SQL Server change at a new position. Query views deduplicate logical events and choose the latest nondeleted source version. Deprecate the PostgreSQL CDC sink and Airflow CDC consumer.

The sink and Spark checkpoint are independent writes. The serving contract comes from verifying seals and deduplicating outcomes across replay. A new baseline requires an explicit new lineage if the original cannot be reproduced safely.
