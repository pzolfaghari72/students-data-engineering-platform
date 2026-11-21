# Integration Testing

Configuration checks, screenshots, and container health are useful but do not prove complete end-to-end data correctness. Use the [2026-11-21 validation summary](../13-validation/validation-update-2026-11-21.md) to separate observed successful checks from work that remains.

| Test | Required evidence | Last known state |
| --- | --- | --- |
| Full SQL Server snapshot | Source isolation/permissions, successful run and all 15 batches, same-source-boundary reconciliation | Successful staged full snapshot existed; live SQL Server catalog/Agent not rechecked in latest report |
| Warehouse full load | Current Spark DAG task success, full-run ID, ClickHouse publication marker and fact/dimension counts | Views populated; Java/Spark verified; newly patched task execution not independently evidenced |
| Debezium/Kafka | Connector + task RUNNING, topic/partition continuity and source signal | Kafka and Connect healthy; 15 Kafka topic boundaries observed |
| CDC initial snapshot | Debezium metric + each captured partition end offset consumed | `snapshot_events_complete=t`, 100% offsets on 2026-11-21 |
| CDC generation publication | PostgreSQL active status and matching ClickHouse publication/active-generation views | **Not yet confirmed after 100%**; supplied status was `BOOTSTRAPPING` |
| CDC create/update/delete | Controlled source mutations, matching Kafka/PG/CH identities and current-state results | Not proven by the supplied snapshot-only checks |
| Recovery and replay | Checkpoint-preserving restart; measured duplicate handling and correct current state | Not fully verified |
| Incremental warehouse SCD2 | Attribute-change versions, close/open validity, deletes, fact updates, saved watermark, retry safety | **Not implemented in the batch analytical model** |
| Grafana | Datasource test and configured KPI queries against published views | Datasource working and dashboard panels observed; some formatting may be a local change |
| Secrets and privacy | Redacted screenshots, no `.env` or raw identifying records in reports | Continue to protect sensitive source data |

## Safe verification approach

Read current state first. Avoid rerunning full extraction, re-registering Debezium, clearing topics/checkpoints, or unpausing Airflow CDC only to obtain a screenshot. If source mutation testing is needed, use an approved synthetic record and verify the existing stream is active before changing test data.

The earlier [2026-11-16 test results](../13-validation/test-results.md) remain a historical record; a new test is needed before upgrading an unverified result to a pass.
