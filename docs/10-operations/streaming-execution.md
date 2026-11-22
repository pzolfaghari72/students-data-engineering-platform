# Streaming execution

Use `make streaming-up` after the [SQL Server readiness review](../05-streaming-and-cdc/sql-server-cdc.md). Validate the producer, consumer and sink in that order. Connect can be up without an active connector task. A running Spark job can still be bootstrapping.

```bash
docker compose --profile cdc --profile streaming ps
docker compose --profile cdc --profile streaming logs --tail=80 spark-cdc-stream
```

Inspect the configured connector's REST status, such as `http://localhost:18083/connectors/students-sqlserver/status` with template settings. Check task state and source history when Kafka offsets are stale. Avoid logging raw event payloads. In the CDC database, query the health interface with an approved reader.

```sql
SELECT generation_id, active_generation, status, verified_bootstrap_records,
       bootstrap_records, last_verified_at, offsets_behind_at_last_batch
FROM cdc_pipeline_health;
```

| Observation | Interpretation and response |
|---|---|
| `BOOTSTRAPPING`, offsets advancing | The generation is consuming the retained cut. Previous active data may still serve. |
| `BOOTSTRAPPING`, all offsets reached | Progress coverage is complete. Check source marker validation, memory pressure and the publication step. |
| `ACTIVE`, no changes in last batch time | Compare the broker, connector and worker process statuses. A quiet source can be healthy. |
| Sink or worker interruption | Use the paired checkpoint and generation. A sealed microbatch may be verified again on resume. |
| Retained offsets or topic lineage mismatch | Inspect the boundaries. Use compatible restored state or an approved new baseline. |

A CDC cutover may require adjusting the checkpoint volume root ownership for the Spark user. Keep the prior checkpoint intact. See [upgrade procedure](operational-runbook.md) and [checkpoint semantics](../05-streaming-and-cdc/checkpoints-and-recovery.md).
