# Final Project Readiness Report

**Evidence date:** 2026-11-16. This is a point-in-time local development
assessment, not a production-readiness claim.

**Current configuration note:** infrastructure monitoring was subsequently
removed to simplify the project. Historical monitoring results below describe
the evidence collected on the report date; Grafana remains for ClickHouse
business dashboards only.

## Executive summary

The batch warehouse, PostgreSQL staging/history, and Kafka/Connect
infrastructure are present. PostgreSQL, ClickHouse, Kafka, and Connect were
healthy or reachable during validation. A published ClickHouse batch
generation exists. The CDC connector has not been registered, Spark streaming
has not run, and CDC rows have not reached ClickHouse. Grafana and Prometheus
were not started.

The two main source/lineage gates remain unresolved: SQL Server snapshot
isolation is disabled, and the 710,000 PostgreSQL CDC history rows plus three
saved offsets do not match any source topics in the current Kafka cluster.
An old Airflow CDC run also remains unresolved. Do not start a competing
consumer or register a new connector until these are addressed.

## Readiness matrix

| Area | Status | Verified evidence | Remaining work |
| --- | --- | --- | --- |
| SQL Server snapshot extraction | Blocked by External Dependency | Extraction code uses one `SNAPSHOT` transaction across the serialized table reads; source is reachable. | DBA must enable/verify `ALLOW_SNAPSHOT_ISOLATION`; re-run the workflow. |
| PostgreSQL staging and metadata | Implemented and Validated | PostgreSQL healthy; `raw`, `metadata`, `history`, and `scd` schemas exist. Metadata records show 15 successful snapshot batches. | Validate a fresh end-to-end snapshot; add tested backup/retention. |
| Historical batch warehouse | Implemented and Validated | ClickHouse healthy; latest published generation has 3,724,654 fact rows, 119,463 student rows, and 59,279 offering rows. | Reconcile against a fresh source snapshot. |
| Kafka and Connect infrastructure | Implemented and Validated | Broker health, metadata, Connect REST, plugin availability, and metrics endpoints passed. No source topics or connector exist. | Resolve source permissions and prior history/offset lineage before registration. |
| SQL Server CDC source readiness | Blocked by External Dependency | Database CDC is enabled and 15 capture instances were found. | DBA must verify Agent/capture/cleanup jobs, retention, and connector permissions. |
| PostgreSQL CDC history | Partially Implemented | 710,000 event rows and three saved offsets exist; event-position uniqueness is configured. | Reconcile those records with Kafka; preserve a single active consumer owner. |
| Airflow CDC consumer | Blocked by External Dependency | DAG is paused; an old DagRun is `running` and its consumer task is `up_for_retry`. | Resolve through Airflow's supported control plane before starting Airflow. |
| Spark Structured Streaming | Implemented but Not Validated | Kafka source, checkpoint, event normalization, PostgreSQL persistence, and ClickHouse sink code exist. The replay counter correction passes unit tests. | Start only after source, lineage, and consumer gates; exercise restart/replay. |
| ClickHouse CDC warehouse | Implemented but Not Validated | CDC event/current-state/SCD2 DDL exists; current CDC event count is zero. | Validate insert/update/delete, replay, generation publication, and reconciliation. |
| Grafana business dashboards | Implemented but Not Validated | ClickHouse datasource and dashboard provisioning exist. | Start Grafana and validate actual dashboard queries. |
| Infrastructure observability | Removed after report date | The historical validation observed Prometheus and exporters; these services and configs are no longer part of the project. | None. Validate Grafana business-dashboard queries separately. |
| Integration and end-to-end tests | Not Implemented | Unit tests and manual procedures exist; `make test` passes 13 tests. | Add controlled integration tests after source and consumer gates are ready. |
| Backup and restore | Not Implemented | Named volumes provide persistence, not backup. | Add and verify database-specific backup/restore procedures. |

Status definitions: **Implemented and Validated** means the relevant
functionality has runtime or test evidence; **Implemented but Not Validated**
means code/configuration exists without a successful runtime test;
**Partially Implemented** means the path exists but has material completeness
or integration gaps; **Blocked by External Dependency** means an external
operator or infrastructure action is required; **Not Implemented** means no
working capability is present.

## Corrections made in this assessment

Spark's PostgreSQL CDC history insert used `ON CONFLICT DO NOTHING` but
incremented operation counters for every replayed Kafka event. It now uses
`RETURNING event_id` and counts only newly persisted records. A focused test
covers replayed updates, inserts, deletes, snapshot records, and tombstones.
The change passed Python compilation and the 13-test unit suite. Streaming
runtime behavior remains unvalidated.

## Blockers and correctness limits

1. **SQL Server snapshot isolation:** `ALLOW_SNAPSHOT_ISOLATION` is `OFF`.
   The existing cross-table snapshot transaction cannot be validated until a
   DBA enables and verifies it.
2. **CDC lineage:** PostgreSQL contains 710,000 CDC events and three saved
   offsets, while Kafka has no source-table topics and Connect has no source
   connector. Do not reset offsets or recreate topics to bypass this mismatch.
3. **SQL Server Agent and retention:** the configured login cannot inspect
   Agent job state or CDC retention. A DBA must verify capture/cleanup jobs
   and the connector's required permissions.
4. **Consumer ownership:** an old Airflow CDC DagRun remains `running`, with
   its task `up_for_retry`. Keep the DAG paused until the run is resolved.
5. **Dual-sink retry semantics:** Spark commits PostgreSQL before appending
   to ClickHouse. If the ClickHouse write partially succeeds and the batch is
   retried, duplicate physical ClickHouse event rows may remain. Current-state
   projections deduplicate by source ordering, but this is not end-to-end
   exactly-once processing.
6. **Resource constraint:** the latest check shows about 1.7 GiB available.
   Spark and    monitoring images were not cached at the time of this assessment. Those
   services have since been removed; do not start or build those
   stacks on this host until resources are available.

## Recommended next actions

1. Ask the DBA to verify CDC jobs/retention and enable snapshot isolation.
2. Reconcile PostgreSQL's saved event/offset lineage with the Kafka cluster;
   preserve existing history and offsets during the decision.
3. Resolve the old Airflow DagRun via the supported Airflow control plane,
   keeping its CDC DAG paused.
4. On a sufficiently provisioned host, run the batch snapshot and validate
   counts before bringing up the CDC connector.
5. Register Debezium only after connector permissions and source topic/offset
   behavior are verified. Confirm actual snapshot completion and event
   metadata before starting Spark.
6. Validate Spark restart/replay and ClickHouse projections, then validate
   Grafana business-dashboard queries.

See the [implementation status](./implementation-status.md),
[runtime validation](../13-validation/runtime-validation-report.md), and
[CDC readiness evidence](../13-validation/cdc-readiness-report.md) for
details. No Git commit or push was made.
