# Spark Structured Streaming

Structured Streaming represents a stream as incrementally processed input to a DataFrame query. In microbatch mode Spark reads a bounded range of Kafka offsets and invokes a sink callback for that work. The trigger interval defines scheduling cadence. Batch size, transformations and sink latency influence how long each trigger actually takes.

The project's Spark 3.5.6 stream runs independently of Airflow. It subscribes to the 15 allowlisted table topics and uses `STREAMING_MAX_OFFSETS_PER_TRIGGER` and `STREAMING_TRIGGER_INTERVAL`. The selected Kafka fields are read through the DataFrame, and each bounded batch is normalized and verified in the current Python application.

The `foreachBatch` callback advances expected contiguous offsets, writes ClickHouse records, compares the stored positions and hashes, and inserts a seal. A replayed batch is compared with the existing seal, including checkpoint identity and digest. The callback can fail after the ClickHouse seal but before Spark commits its checkpoint. The next run safely verifies the existing work.

A generation publishes only after its saved cut is covered and the completed baseline is validated. That marker controls dashboard visibility. In the recorded deployment the dataset was replayed from retained Kafka records into the dedicated CDC database. [Implementation evidence](../12-implementation/implementation-status.md#live-cdc-cutover)

[Spark Structured Streaming 3.5.6](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) · [Stream source](../../spark/stream_cdc.py) · [Checkpoint recovery](checkpoints-and-recovery.md)
