# Data flow and lineage

| Boundary | Data crossing it | Identity the receiving step preserves |
|---|---|---|
| SQL Server → PostgreSQL | Full typed source rows in a single snapshot transaction | Source schema/table/key, staging run and extraction batch |
| PostgreSQL → Spark Batch | Raw tables with an explicit batch filter per table | `source_run_id` and each `_batch_id` |
| Spark Batch → ClickHouse Batch | Candidate dimensions and facts | Snapshot ID, table grain and final publication marker |
| SQL Server → Connect → Kafka | Debezium envelopes and keyed tombstones | Source transaction position, source row key, topic/partition/offset |
| Kafka → Spark → ClickHouse CDC | Normalized record, verification seal and generation metadata | Kafka cluster/topic IDs, Spark checkpoint identity, batch ID and logical source version |
| ClickHouse → Grafana | Batch views or CDC aggregates/health | Most recent valid batch publication or active CDC generation |

The dashboard reader has separate provisioned datasources. The two data models meet only in read-only comparisons. When investigating an unexpected total, start from the publication reference and trace backward to its staging run or source topic positions before comparing counts.

[Batch diagram](diagrams/batch-pipeline.md) · [CDC diagram](diagrams/streaming-pipeline.md) · [Reconciliation contract](../07-data-warehouse/data-publication-and-reconciliation.md)
