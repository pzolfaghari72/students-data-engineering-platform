# Kafka Fundamentals in This Project

Kafka is the durable, replayable event log between Debezium and consumers.
The local cluster is a single-node KRaft broker/controller, not a resilient
multi-broker deployment.

| Concept | Actual use |
| --- | --- |
| Broker | `kafka` service exposes an internal listener for Compose and a loopback host listener. |
| Topic | Debezium creates table topics under the configured prefix; Connect also uses internal config/offset/status topics. |
| Key | Debezium serializes source primary-key fields as JSON without embedded schemas. Keys normally route rows with the same key to the same partition. |
| Partition | Ordered append-only log segment; ordering is per partition, not across tables/topics. |
| Offset | Monotonically increasing position in one partition. Spark checkpoints source offsets; the Airflow consumer stores next offsets in PostgreSQL. |
| Consumer | Airflow Kafka Python consumer or Spark Kafka source. They are separate consumers and write different state. |
| Retention | Kafka log retention is configured through the environment; event retention is finite, so it bounds replay time. |

Source event key/value shape and safe inspection guidance are in
[topics, partitions and offsets](./kafka-topics-partitions-offsets.md) and
[event processing](./event-processing.md). Do not reset Connect offsets or
delete topics to resolve a consumer problem.
