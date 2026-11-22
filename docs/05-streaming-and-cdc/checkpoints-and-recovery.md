# Checkpoints and recovery

A checkpoint persists Spark query identity and progress outside the container. Here it is in a named local volume under `CDC_CHECKPOINT_DIR`. The CDC database independently records verified sink progress. A consistent recovery uses the compatible checkpoint, generation, broker/topic identities and ClickHouse data.

| State | Recovery behavior |
|---|---|
| Worker failed before a seal | Records may remain physically present. The same batch can be retried. Consumers read sealed work. |
| Seal exists but Spark did not commit | The same batch is re-read and checked against the stored digest, physical records and query ID. |
| ClickHouse is behind Spark, or the checkpoint belongs to another query | Startup fails closed. Restore the compatible state or plan a new baseline. |
| Kafka no longer retains the needed offset, or the topic ID changed | The identity or bounds check blocks continuation. A same-name recreated topic does not restore lineage. |
| New generation, missing complete initial history | Bootstrap requires retained coverage from offset zero or an explicit new connector/topic/checkpoint lineage. |

The writer lock in the checkpoint volume prevents two running instances from using the shared volume in the supported deployment. The startup check accepts sealed progress at the Spark commit or one batch ahead, subject to matching query identity. Before clearing anything, record the configured checkpoint path, broker positions and latest ClickHouse progress. The [streaming runbook](../10-operations/streaming-execution.md) provides checks and escalation paths.

[Kafka source behavior in Spark](https://spark.apache.org/docs/3.5.6/structured-streaming-kafka-integration.html) · [Backup boundaries](../10-operations/backup-and-recovery.md)
