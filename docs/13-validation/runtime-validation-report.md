# Runtime Validation Report

**Validation date:** 2026-11-16. Results describe the inspected local runtime
at that time, not an ongoing health assertion.

**Current configuration note:** Prometheus and infrastructure exporter
services referenced by the historical results have since been removed.
Grafana remains for ClickHouse-backed business dashboards.

## Tests and configuration

| Check | Result | Evidence |
| --- | --- | --- |
| Unit tests | PASS | `make test`: 13 tests passed, including the CDC replay-counter case. |
| Modified Python syntax | PASS | `py_compile` passed for the streaming entry point, CDC loader, and affected test. |
| Local environment validation | PASS | `scripts/validate_environment.py` passed without printing values. |
| Required environment key coverage | PASS | 91 required keys were represented in `.env.example`; no missing required keys. Optional Airflow tuning values have Compose defaults. |
| Application connection fallback scan | PASS | No `os.getenv(..., default)` or `os.environ.get(..., default)` connection-setting fallbacks were found in the scanned Python sources. |
| Compose configuration | PASS | `docker compose --env-file .env config --quiet` and explicit all-profile configuration passed. `.env` values were not printed. |
| Debezium plugin validation | PASS | The SQL Server connector config was accepted by the installed Connect plugin; no connector was registered. |
| Scoped whitespace check | PASS | CDC code and documentation touched in this follow-up have no trailing whitespace. |
| Repository-wide `git diff --check` | FAIL | It reports carriage-return whitespace on existing changed lines in `scripts/generate_secrets.py` and `scripts/validate_environment.py`. These unrelated files were not normalized; the scoped check passed. |

## Service and data checks

| Check | Result | Evidence |
| --- | --- | --- |
| PostgreSQL | PASS | Running and healthy; `raw`, `metadata`, `history`, and `scd` schemas exist. |
| ClickHouse | PASS | Running and healthy; latest published batch generation contains 3,724,654 fact rows, 119,463 student rows, and 59,279 offering rows. |
| ClickHouse metrics endpoint | PASS | Native `/metrics` endpoint responded with ClickHouse metric samples. |
| Kafka broker and metadata | PASS | Broker health and metadata requests succeeded. Current topics are only Connect internal topics and `__consumer_offsets`. |
| Kafka Connect REST/plugin | PASS | REST API and SQL Server Debezium plugin were reachable. Connector list is empty. |
| Connector health metric | PASS | Exporter responded and reported `debezium_connector_up=0`, consistent with no registered connector. |
| PostgreSQL CDC history/offsets | PASS | 710,000 history rows and three saved-offset rows. |
| ClickHouse CDC event table | PASS | Query returned zero CDC event rows. This is not evidence of event ingestion. |
| SQL Server connectivity/CDC | PASS | Authentication and read-only checks succeeded; database CDC is enabled and 15 capture instances exist. |
| SQL Server Agent/retention | BLOCKED | The configured login lacks permission to inspect Agent jobs and CDC retention. |
| SQL Server snapshot isolation | FAIL | `ALLOW_SNAPSHOT_ISOLATION` is OFF. |
| Airflow | BLOCKED | Services are stopped. Persisted state includes one old CDC DagRun marked `running` and its task `up_for_retry`; keep the DAG paused until resolved. |
| Spark streaming | NOT RUN | No connector/source topics exist; no Spark consumer or checkpoint was started. |
| Grafana/Prometheus | NOT RUN | Dashboard queries and Prometheus scrape targets were not tested. |

## Runtime and safety

During this follow-up, no services were started or stopped, no images were
built, and no database, Kafka, connector, volume, or checkpoint state was
changed. PostgreSQL, ClickHouse, Kafka, Connect, and their existing exporters
were already running. No SQL Server writes, Kafka topic/offset resets, or
Debezium registration were performed.

The host reported about 7.6 GiB total memory and 1.7 GiB available. The
running core/CDC containers used roughly 1.6 GiB. Spark and several monitoring
images were not cached. Airflow, Spark, Grafana, and Prometheus were therefore
not started or built; this avoids an unsafe resource spike on the current
host.

PostgreSQL metadata reported 143 successful, five failed, and three running
pipeline runs; 15 snapshot batches were successful. These stored statuses are
not a claim that the latest code path was executed during this validation.

## Probes and limitations

- The first Kafka CLI probe omitted the internal listener port; retrying
  against the configured internal listener succeeded.
- The local Python environment did not provide the PostgreSQL client module
  used by an initial probe; read-only checks were completed with `psql` inside
  the existing PostgreSQL container.
- This Compose version does not support `--all-profiles`; explicit profile
  flags were used for configuration validation.
- `docker compose --profile cdc up -d --build` previously encountered a
  registry image-download failure. Targeted Kafka/Connect/exporter startup
  subsequently succeeded; no startup was attempted in this follow-up.

No Debezium initial snapshot, source event flow, Spark batch/stream runtime,
checkpoint restart, ClickHouse CDC application, Grafana query, or complete
source-to-dashboard integration was performed. See the [CDC readiness
report](./cdc-readiness-report.md) and [final readiness
report](../12-implementation/final-project-report.md) for the next gates.
