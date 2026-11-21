# CDC Infrastructure Readiness Report (2026-11-16)

Validation date: 2026-11-16
Scope: start and validate Kafka/Connect without resetting CDC offsets or
disturbing the PostgreSQL and ClickHouse core services.

Historical evidence note: infrastructure monitoring and exporter services
mentioned below were subsequently removed. Kafka Connect's JMX signal remains
available internally for Spark snapshot-completion gating; it is not a
Prometheus monitoring service.

## Executive result

Kafka and Kafka Connect are running and their APIs are reachable. The SQL
Server endpoint is reachable, CDC is enabled, and all 15 expected tables have
capture instances. **Do not register Debezium or start Spark yet.**

There are two important blockers:

1. The source login cannot inspect SQL Server Agent/capture-job state. The
   default Debezium Agent-status check requires `VIEW SERVER STATE` (or a DBA
   configured alternative). CDC retention is consequently unverified.
2. PostgreSQL already contains 710,000 CDC events and three saved Kafka
   offsets, but the currently reachable Kafka cluster has no source-table
   topics and Connect has no registered connectors. Creating new topics from
   offset zero could collide with PostgreSQL's unique Kafka-position keys.
   The prior Kafka topic/offset lineage must be recovered or reconciled before
   bootstrap.

The Airflow CDC DAG was unpaused in persisted metadata. It is now paused.
However, an old DagRun remains `running` with its task `up_for_retry`; resolve
that run using Airflow's supported control plane before starting Airflow or
Spark. No Airflow or Spark process was started during this validation.

## Services and core-platform safety

| Validation | Status | Result |
| --- | --- | --- |
| PostgreSQL container | PASS | Running and healthy; restart count 0. Existing `postgres-data` volume remains mounted. |
| ClickHouse container | PASS | Running and healthy; restart count 0. Existing `clickhouse-data` volume remains mounted. |
| Compose configuration identity | PASS | Live PostgreSQL and ClickHouse config hashes match the rendered current config. |
| Requested full CDC-profile command | FAIL | `docker compose --profile cdc up -d --build` initially stopped during an interrupted Kafka-exporter image download; it started no services in that attempt. |
| Kafka and Connect startup | PASS | Started explicitly after the image pull failure. Only Kafka/Connect containers were touched; PostgreSQL and ClickHouse were not recreated. Existing named volumes were retained. |
| Kafka exporter | PASS | Image pull retry succeeded; container is running and exposes broker/topic metrics. |
| Connect health exporter | PASS | Container is running; metric reports the configured connector as down because no connector is registered. |
| Airflow services | NOT RUN | No Airflow service was started. |
| Spark streaming service | NOT RUN | No Spark streaming consumer or checkpoint was started/created. |

Compose profile inspection showed that the unqualified `cdc` profile included
the one-shot connector-registration service. It is now assigned to the
`tools` profile, so starting CDC infrastructure does not implicitly register
or update a source connector. The registration helper now uses create-only
behavior and does not modify an existing connector or its offsets.

## Kafka and Kafka Connect

| Validation | Status | Result |
| --- | --- | --- |
| Kafka broker health | PASS | Compose health check is healthy. |
| Bootstrap connectivity / metadata | PASS | `kafka-broker-api-versions.sh` returned broker API metadata. One broker is reported by the exporter. |
| Kafka topic listing | PASS | Current topics are `__consumer_offsets`, `connect_configs`, `connect_offsets`, and `connect_statuses`; no Debezium source-table topics exist. |
| Kafka Connect REST | PASS | REST API responds with Kafka Connect version 3.7.0. |
| Debezium SQL Server plugin | PASS | `io.debezium.connector.sqlserver.SqlServerConnector` version 2.7.3.Final is installed. |
| Connector config validation | PASS | The SQL Server connector configuration passes the Connect plugin validator. Validation did not create or change a connector. |
| Registered Debezium connectors | PASS | Connector list is empty; no connector existed to preserve or inspect. |
| Connect internal topic partitioning | PASS | `connect_configs`: 1 partition; `connect_offsets`: 25; `connect_statuses`: 5. All use replication factor 1 and compact cleanup. |
| Internal topic retention | PASS | All three internal topics use compact cleanup and show `retention.ms=604800000` (7 days). This is the observed configuration; no topic settings were changed. |
| Connect JMX metrics endpoint | PASS | The exporter endpoint responds. No snapshot-completion sample exists because no connector/snapshot is running. |
| Debezium event flow | NOT RUN | No source connector or source-table topic exists; no source event was produced or consumed. |

## SQL Server CDC readiness

The checks used the configured ODBC Driver 18 connection, without printing
credentials or modifying source data.

| Validation | Status | Result |
| --- | --- | --- |
| TCP connectivity | PASS | Configured SQL Server endpoint accepted a TCP connection. |
| ODBC driver and database authentication | PASS | Configured driver is installed; connection to `Students` succeeded. |
| SQL Server version | PASS | Server reports version `16.0.1210.3`. |
| Database CDC enabled | PASS | `Students.is_cdc_enabled = 1`. |
| Expected capture instances | PASS | All 15 assessed `dbo` tables have one capture instance; all use `students_debezium_cdc_reader`; net changes are disabled. |
| Connector login mapped to CDC role | PASS | Current login is a CDC-reader role member; it is not `sysadmin` or `db_owner`. |
| Source and CDC table SELECT | PASS | Current principal can select from `dbo.Students` and `cdc.dbo_Students_CT`. |
| Database-state/definition permissions | BLOCKED | Current principal reports no `VIEW DATABASE STATE` and no `VIEW DEFINITION`. Ask the DBA to confirm the minimum permissions required for the installed connector version. |
| SQL Server Agent status | BLOCKED | The service DMV query returned SQL permission error 42000. [Debezium 2.7 documents](https://debezium.io/documentation/reference/2.7/connectors/sqlserver.html#_no_maximum_lsn_recorded_in_the_database) that the default Agent-status check needs `VIEW SERVER STATE`; alternatively the DBA can configure a safe custom status query. |
| CDC capture/cleanup jobs and retention | BLOCKED | `sys.sp_cdc_help_jobs` was not accessible to this login. The DBA must verify the capture job is running and record cleanup retention before connector startup. |
| Snapshot-isolation setting | FAIL | `ALLOW_SNAPSHOT_ISOLATION` is `OFF` and RCSI is `OFF`. The existing Airflow cross-table `SNAPSHOT` transaction is not ready until the DBA enables and verifies snapshot isolation. This finding is separate from Debezium's own initial snapshot. |

Read-only DBA checks are in
`db/source_assessment/sqlserver_cdc_prerequisites.sql`.
The existing CDC enablement script at
`db/staging_postgres/enable_source_cdc.sql` is not run by this validation.

## Debezium status and snapshot progress

| Validation | Status | Result |
| --- | --- | --- |
| Connector configuration validation | PASS | Plugin validation endpoint accepted the configured SQL Server connector settings. |
| Connector registration | BLOCKED | Not attempted: source Agent/job permissions and CDC retention are unverified, and existing PostgreSQL history is not reconciled with the current Kafka topic set. |
| Connector/task status | NOT RUN | No connector is registered; there is no task status to report. |
| Initial snapshot start/completion | NOT RUN | No connector ran. Snapshot completion is not inferred from plugin availability or REST health. |
| Snapshot metrics | NOT RUN | JMX is reachable, but there is no connector snapshot to measure. |
| Kafka source event metadata | NOT RUN | No source event is available to inspect. |

The configured connector uses `snapshot.mode=initial`. Its plugin configuration
was validated only; no connector offsets, topics, source tables, or data were
changed.

## Existing CDC state and reconciliation warning

| Validation | Status | Result |
| --- | --- | --- |
| PostgreSQL CDC history | PASS | `history.cdc_events` contains 710,000 rows: Applicants 486,794; CourseEnrollments 223,194; ClassStandings 12. Latest capture timestamp is 2026-11-15 15:07:32 +03:30. |
| PostgreSQL saved Kafka offsets | PASS | Three rows exist: Applicants partition 0 next offset 486,794; ClassStandings partition 0 next offset 12; CourseEnrollments partition 0 next offset 223,194. |
| Spark generation metadata | PASS | No streaming generation is present. |
| Matching Kafka source topics | FAIL | No source topics are present in the current Kafka cluster. |
| ClickHouse CDC event rows | PASS | `cdc_events` count is 0. |

PostgreSQL enforces a unique key on `(kafka_topic, kafka_partition,
kafka_offset)`. Do not register a new connector against these existing
history rows until the DBA/platform owner determines whether the original
Kafka topics/offsets can be restored or whether a separately approved
reconciliation/bootstrap generation is required. Do not reset Connect offsets
or delete/recreate topics to work around this mismatch.

## Airflow consumer guard

| Validation | Status | Result |
| --- | --- | --- |
| CDC DAG paused | PASS | Persisted `is_paused` was changed from false to true. |
| Airflow scheduler/API running | NOT RUN | No Airflow service is running, so no task is currently consuming Kafka. |
| Existing DagRun state | BLOCKED | A pre-existing run is still marked `running`; its `consume_debezium_events` task is `up_for_retry`. Do not start Airflow or Spark until this run is safely resolved through Airflow's supported control plane. The run/task state was not manually rewritten. |

## Spark Structured Streaming readiness

| Validation | Status | Result |
| --- | --- | --- |
| Streaming code and entry point | PASS | `spark/stream_cdc.py` is configured as a separate Spark 3.5.6 streaming application. |
| Kafka/Spark dependency declaration | PASS | Compose supplies `spark-sql-kafka-0-10_2.12:3.5.6`, PostgreSQL JDBC, and ClickHouse JDBC packages. |
| Persistent checkpoint mount | PASS | Compose maps the configured checkpoint directory to the named `spark-streaming-checkpoints` volume. No checkpoint was created or inspected. |
| PostgreSQL event idempotency | PASS | Event inserts use the unique Kafka position constraint with `ON CONFLICT DO NOTHING`; generation offsets advance monotonically. |
| ClickHouse replay idempotency | FAIL | ClickHouse uses append-only `MergeTree`; a microbatch replay after PostgreSQL commit but before ClickHouse success can append duplicate physical events. State views group/argMax by source key and position, but the event log itself is not exactly-once. |
| Spark image/runtime test | NOT RUN | No Spark image build or streaming process was started because connector topics and source prerequisites are not verified. |
| Spark recovery/replay test | NOT RUN | Requires a valid Kafka source stream and a controlled failure/restart exercise. |

The stream code commits PostgreSQL before writing ClickHouse. A ClickHouse
write failure therefore retries the Spark microbatch: PostgreSQL's unique
constraint makes that side idempotent, but the ClickHouse append can duplicate
rows. This validation does not claim end-to-end exactly-once behavior.

## External blockers and next stage

1. DBA: confirm SQL Server Agent and CDC capture/cleanup jobs, capture retention,
   and provide the minimum connector permissions. The default Debezium Agent
   status check needs `VIEW SERVER STATE`, unless a safe custom query is
   configured.
2. DBA: enable and verify `ALLOW_SNAPSHOT_ISOLATION` for the existing batch
   snapshot transaction if that workflow is to be used.
3. Platform owner: reconcile the 710,000 persisted PostgreSQL events and three
   saved offsets with the absence of source topics in the current Kafka
   cluster. Preserve all existing Kafka/PostgreSQL data during this decision.
4. Airflow operator: resolve the old running DagRun and retrying consumer task
   through the Airflow control plane, keeping the CDC DAG paused.
5. Only after these blockers are resolved:
   - Verify `docker compose --profile cdc up -d --build` and all services.
   - Register only if the connector list is still empty:
     `docker compose --profile cdc --profile tools run --rm debezium-register`
   - Verify connector/task status, the SQL Server snapshot metrics, snapshot
     completion, source-table topics, and actual event envelopes.
   - Start `spark-cdc-stream` only after event topics and the historical
     position/reconciliation plan are verified.

## Changes and validation commands

Changes made for safe startup:

- Moved the one-shot `debezium-register` service to the `tools` profile.
- Fixed the Kafka Connect JMX agent path so the Connect JVM can start.
- Made connector registration create-only: existing connector configuration
  and offsets are left untouched.
- Fixed the Connect plugin-validation request shape.
- Added focused registration unit tests and this report/read-only DBA check.
- Updated the registration commands in the Makefile and documentation.

### Follow-up audit (2026-11-16)

The following checks were read-only; no services were started or stopped
during this follow-up.

| Validation | Status | Result |
| --- | --- | --- |
| Core service health | PASS | PostgreSQL and ClickHouse remained healthy. |
| Kafka and Connect | PASS | Broker metadata and Connect REST responded; no source topics or registered connector were present. |
| Connector health metric | PASS | Exporter returned `debezium_connector_up=0`, matching the empty connector list. |
| ClickHouse native metrics | PASS | `/metrics` responded with ClickHouse metric samples. |
| PostgreSQL history/offset counts | PASS | 710,000 CDC event rows and three saved offsets remained present. |
| PostgreSQL pipeline metadata | PASS | 143 `SUCCESS`, five `FAILED`, and three `RUNNING` runs; 15 snapshot batches were `SUCCESS`. |
| ClickHouse batch generation | PASS | Latest published generation has 3,724,654 fact rows, 119,463 student rows, and 59,279 offering rows. CDC event count remains zero. |
| Modified CDC Python syntax and tests | PASS | `py_compile` passed for modified modules; `make test` passed 13 tests. |
| Spark, Grafana, Prometheus runtime | NOT RUN | About 1.7 GiB host memory was available; Spark and several monitoring images were not cached. No resource-heavy service was started. |

The Spark PostgreSQL sink's audit counters now count only records returned by
`INSERT ... ON CONFLICT DO NOTHING RETURNING event_id`. A focused unit test
covers replayed operations. This is not a streaming integration test and does
not change the separate ClickHouse duplicate-write limitation.

Commands run (outcomes are recorded above):

```sh
make test
docker compose --env-file .env config --quiet
docker compose --profile cdc up -d --build
docker compose --profile cdc up -d --build kafka connect
docker compose --profile cdc up -d kafka-exporter connect-health-exporter
```

The first full-profile invocation was blocked by a transient registry download
failure; targeted service startup and a retry subsequently brought Kafka,
Connect, and both exporters up. No Git commit or push was made.
