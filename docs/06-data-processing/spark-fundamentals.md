# Spark fundamentals

Apache Spark executes computations on partitioned datasets. The driver plans actions, executors run tasks, and Spark SQL/DataFrames describe transformations, joins and aggregates. A transformation builds a plan. An action such as a count, collect or write executes work. Joins and grouping can shuffle rows between partitions.

This repository uses Spark 3.5.6 in two contexts. Airflow invokes a local Spark batch process to join typed staging tables and write a star schema. A separate Spark container runs Structured Streaming to process Kafka microbatches. Local execution reduces deployment complexity while retaining familiar Spark partition, JDBC and execution-plan concepts.

Read partitions and JDBC fetch size affect database connections and memory. Shuffle partitions affect stages with joins and aggregations. In the batch loader final result frames are persisted to disk while counts and writes reuse their plans. Streaming uses bounded microbatches because its callback materializes normalized rows on the driver.

For a slow job, identify whether it is waiting on inputs, shuffles, validation or sink writes. For a startup failure check Java, `spark-submit`, Python compatibility and JVM artifacts. [Batch loader](spark-batch.md) · [Streaming processing](../05-streaming-and-cdc/spark-structured-streaming.md)
