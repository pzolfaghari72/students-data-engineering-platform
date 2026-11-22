# Architecture

The [README diagram](../../README.md) shows the two independently runnable data paths. They share a SQL Server source, a ClickHouse server and Grafana. Each path owns its write credentials, publication criteria, data model and freshness.

| Component | Responsibility |
|---|---|
| Airflow | Runs the full batch extract, records completion and triggers warehouse processing with the exact staging run ID. |
| PostgreSQL | Hosts batch raw data, batch execution metadata and a separate Airflow metadata database. |
| Spark Batch | Transforms staged tables into a star schema and checks persisted ClickHouse counts, distinct grain keys and selected aggregates before publication. |
| Debezium and Kafka Connect | Snapshot the SQL Server tables, capture ongoing changes and persist source offsets and schema history through Kafka. |
| Kafka | Retains the 15 keyed data topics for replay. Connect internal topics and ordered schema history have separate policies. |
| Spark Structured Streaming | Owns one checkpointed CDC query, verifies Kafka offsets and ClickHouse writes, and publishes a CDC generation. |
| ClickHouse | Serves the batch star schema and the separate CDC event, current-state and aggregate views. |
| Grafana | Serves live CDC KPIs and health plus an independent batch analytics dashboard. |

## Batch

A full extraction opens one SQL Server `SNAPSHOT` transaction. Each PostgreSQL table batch is tagged with its extraction batch ID. Independent chunk commits keep large writes manageable. The finalizer compares the completed extraction manifest to the 15 reconciled table batches. A failed run retains diagnostic rows but never becomes an input to the warehouse DAG.

The warehouse DAG processes a specific successful `source_run_id` and serializes supported publishing attempts. Each attempt appends under a fresh `snapshot_id`. Spark checks source-to-fact counts, joins and grain, then reads ClickHouse to check counts and exact distinct keys for each candidate table. It also checks capacity and fact totals. It writes the manifest last. The views resolve the highest published source-run ID. Unpublished parts and later publications of older source runs leave the current snapshot visible.

## CDC

SQL Server CDC tables, Kafka Connect source offsets, and Spark offsets represent different recovery boundaries. The stream first verifies Kafka cluster and topic IDs, all intended partitions, and the retained initial snapshot. It accepts committed snapshot completion from Connect or a live source position backed by the retained final snapshot record. ClickHouse stores the observed Kafka end offsets and publishes only when all those offsets are verified. An earlier partial baseline is excluded using the completed snapshot LSN. A conflicting baseline fails the query.

Spark writes each microbatch, verifies its retained Kafka coordinates and record hashes, then seals the batch in ClickHouse. A retry uses the same batch ID and verifies an existing seal. Unsealed writes stay outside serving views. Physical retries can leave identical copies. Logical events collapse repeated source versions even when Debezium sends them at new Kafka offsets. Current state uses SQL Server LSNs and the event serial for ordering. Tombstones count toward delivery and deletes remove the key from current state.

The finite SQL Server and broker retention windows bound catch-up. Normal recovery preserves Spark’s checkpoint, Kafka topic lineage, and ClickHouse metadata as a set. The query refuses offsets outside the retained range or a restored sink behind its checkpoint. The [runbook](../10-operations/operational-runbook.md) covers legacy migration and an isolated rebootstrap.

## Serving and reconciliation

The two ClickHouse databases can be deployed independently on the same server. Grafana can query approved aggregate and health views in the CDC database and published dimensional views in the batch database. Raw CDC row images are private.

`make reconcile` displays the current counts and their lineage for three shared grains. Batch counts stay fixed between full snapshots. CDC reflects verified changes since its own baseline, so compare close time boundaries or a quiescent source before investigating differences. [Data model](data-model.md)
