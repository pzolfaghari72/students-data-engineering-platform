# Spark Structured Streaming

`spark/stream_cdc.py` is an independent, long-running Spark application. It
uses the Spark Kafka source, a persistent checkpoint, a ten-second
processing-time trigger and `foreachBatch`; Airflow is not its runtime.

## Microbatch flow

1. Match Debezium topics with the configured database/schema/table pattern.
2. Fail if no topics exist or Kafka retention has moved past a fresh
   checkpoint's beginning offsets.
3. Normalize each keyed JSON envelope and retain Kafka position plus SQL
   Server commit/change LSN and event serial metadata.
4. Persist raw event history and streaming-generation offsets in a PostgreSQL
   transaction.
5. Append the event rows to ClickHouse `cdc_events`.
6. Publish the new ClickHouse generation only after Debezium snapshot
   completion and captured per-partition offsets have been consumed.

The image definition is `spark/Dockerfile`; it uses the Spark 3.5.6 base image
and installs the Python dependencies listed in `spark/requirements.txt`.
PostgreSQL and ClickHouse JDBC drivers and the Spark Kafka connector are Maven
artifacts configured in Compose, not Python packages. It exposes no monitoring
endpoint. Compose runs `local[2]` with two shuffle
partitions, a 768 MiB driver and a 1.5 GiB container limit. The complete
configuration is in `docker-compose.yml` and the central
[environment reference](../01-getting-started/environment-configuration.md).

This code has not been started in the recorded validation. PostgreSQL commit
followed by ClickHouse append is not atomic; ClickHouse may contain duplicate
physical rows after a replay. See
[checkpoint recovery](./checkpoints-and-recovery.md) before operating it.
