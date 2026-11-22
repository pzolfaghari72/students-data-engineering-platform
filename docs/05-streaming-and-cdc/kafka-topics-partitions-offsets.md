# Kafka topics, partitions and offsets

A Debezium table produces a topic named `<prefix>.<database>.<schema>.<table>`. With template defaults, the enrollment topic is `students.Students.dbo.CourseEnrollments`. The source allowlist drives topic preparation and Spark subscription, including tables that might yield zero snapshot rows.

| Topic family | Default example | Policy and consumers |
|---|---|---|
| Table data | `students.Students.dbo.CourseEnrollments` | Delete retention, one partition by default, read by the Spark Kafka source |
| Connect configuration, offsets and status | `connect_configs`, `connect_offsets`, `connect_statuses` | Compacted internal state for the Connect worker |
| Debezium schema history | `schema-history.students` | One partition and complete retained history for the connector |

Data topic retention defaults to seven days in the template. The [topic setup script](../../scripts/prepare_cdc_topics.sh) creates missing topics and checks existing policies. It reports mismatches for review. The retained data log expects contiguous positions and does not use compaction.

Offsets are ordered within a partition. The end offset is the next position; an interval of positions from 10 through 14 is represented by beginning 10 and end 15. Across partitions the saved cut is a map of ends. Increasing a partition count or recreating a same-name topic can change routing or topic identity. Spark validates actual Kafka cluster and topic identifiers before resuming. The ClickHouse serving model orders source changes by LSN/version.

[Apache Kafka topic settings](https://kafka.apache.org/39/generated/topic_config.html) · [Recovery boundaries](checkpoints-and-recovery.md)
