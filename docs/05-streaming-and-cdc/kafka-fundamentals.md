# Kafka fundamentals

Kafka is a durable, append-oriented message log. Producers send keyed records; a broker stores them in topics split into partitions. A consumer can read each partition from an offset, process future records and replay retained history. The record value can carry an event envelope or a keyed null value.

Kafka decouples source capture from Spark availability. A Spark restart can continue through a recorded position instead of asking SQL Server to repeat the full extraction. Retention is finite, so replay depends on the continued availability of the relevant records and the same topic lineage.

The project runs a single KRaft broker/controller service. KRaft maintains Kafka metadata without ZooKeeper. A named volume holds broker data and cluster metadata. Replication factor one matches this local single-host setup and provides no independent node redundancy.

For a health check first verify the listener from the client network and list expected topics. Then inspect retention and beginning/end offsets. Broker reachability confirms transport availability; source capture and sink publication require their own checks. [Topic layout](kafka-topics-partitions-offsets.md) · [Network listeners](../03-infrastructure/services-and-networking.md)
