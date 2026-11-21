# Kafka Topics, Partitions and Offsets

The connector topic shape is
`<DEBEZIUM_TOPIC_PREFIX>.<SOURCE_DB_NAME>.<SOURCE_DB_SCHEMA>.<table>`.
For example, the configured pattern is documented without copying local
endpoint values. The one-node broker defaults new topics to the configured
partition count (the template uses one); changing partition count later can
change per-key ordering assumptions.

The Connect worker uses three compacted internal topics for configuration,
offsets and status. In the observed cluster these had 1, 25 and 5 partitions,
respectively, replication factor 1 and seven-day `retention.ms` observation;
those internal topic settings are not source-topic guarantees. The current
cluster had no source table topics.

| Position | Owner | Persistence |
| --- | --- | --- |
| Connect source offsets | Debezium/Kafka Connect | `connect_offsets` topic in `kafka-data` |
| Airflow consumer offset | Airflow CDC DAG | `metadata.kafka_offsets` stores the next offset per topic/partition |
| Spark offset progress | Spark Structured Streaming | Named checkpoint volume; Spark uses it rather than Kafka group commits for recovery |
| Source change version | SQL Server/Debezium | Commit/change LSN and event serial fields stored in PostgreSQL and ClickHouse event rows |

When retention passes a saved consumer offset, the code fails rather than
silently skipping missing data (`failOnDataLoss=true` for Spark). Kafka
offsets only order records within one partition. Use the actual source LSN
metadata for source ordering and validate topic lineage before replay. See the
[CDC readiness warning](../13-validation/cdc-readiness-report.md).
