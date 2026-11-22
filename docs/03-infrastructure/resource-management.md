# Resource management

This deployment shares a single host between several JVMs, two storage engines and a dashboard process. JVM heap, container resident memory, page cache and disk usage are different constraints. WSL2 also adds a host-side memory boundary. Observe both Docker and the Linux host during a replay.

| Lever | Effect and trade-off |
|---|---|
| `SPARK_STREAM_MEMORY_LIMIT`, `SPARK_STREAM_DRIVER_MEMORY`, `SPARK_STREAM_CPUS` | Set the container envelope and driver/runtime budgets. Leave headroom for Python, native libraries and I/O buffers. |
| `STREAMING_MAX_OFFSETS_PER_TRIGGER`, `STREAMING_TRIGGER_INTERVAL` | Cap microbatch input and set scheduling cadence. Larger batches can improve catch-up throughput while increasing driver work and peak memory. |
| `KAFKA_HEAP_OPTS`, `CONNECT_HEAP_OPTS` | Control the respective JVM heaps. Keep broker retention compatible with disk space and replay requirements. |
| `SPARK_BATCH_READ_PARTITIONS`, `SPARK_BATCH_FETCH_SIZE`, `SPARK_BATCH_WRITE_SIZE` | Bound reader and writer work. Increasing parallel reads increases PostgreSQL connections and load. |
| `CLICKHOUSE_DASHBOARD_GROUP_BY_BYTES` | Sets the external aggregation threshold for the CDC writer and dashboard reader. The roles limit query threads to two. Spilling trades disk I/O for lower memory pressure. |

```bash
docker stats --no-stream
docker system df
```

If throughput falls, compare verified offsets over equal intervals and inspect container restarts, RAM, swap and free disk. Avoid simultaneous full batch backfills and CDC bootstraps on a constrained host. A ClickHouse restart interrupted final CDC publication after all records were sealed during the documented cutover. Bounding aggregation and resuming from the checkpoint allowed publication. [Observed cutover](../12-implementation/implementation-status.md#live-cdc-cutover)
