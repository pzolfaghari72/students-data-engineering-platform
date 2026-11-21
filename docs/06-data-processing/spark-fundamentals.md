# Spark Fundamentals

The project uses local Spark in containers rather than a standalone cluster.
Spark builds DataFrames lazily; an action such as `count()` or a JDBC write
executes the plan. Joins can shuffle data, which is why the current batch job
uses only two shuffle partitions and bounded JDBC partitions.

| Concept | Project example |
| --- | --- |
| Driver | The `spark-submit` process running `load_warehouse.py` or `stream_cdc.py`. |
| Executor | Local worker threads in `local[2]`; not separate distributed nodes. |
| DataFrame | Typed JDBC reads and derived fact/dimension frames. |
| Transformation | Select, HMAC projection and reference joins in `build_warehouse_frames()`. |
| Action | Counts used for row preservation and JDBC appends. |
| Shuffle | Required by many joins/groupings; deliberately bounded locally. |
| Batch | One finite read/transform/write of selected PostgreSQL batches. |
| Structured Streaming | Kafka microbatches with checkpoint offsets and `foreachBatch`. |

Spark only reads the six batch tables needed for the enrollment model. It
does not query SQL Server directly. Resource limits and separate execution
paths are documented in [Spark batch](./spark-batch.md) and
[Structured Streaming](../05-streaming-and-cdc/spark-structured-streaming.md).
