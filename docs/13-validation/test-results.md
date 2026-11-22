# Test Results

**Recorded:** 2026-11-16.

These are historical results. At the time, the explicit-profile Compose check
included the monitoring profile; Prometheus and infrastructure exporter
services have since been removed. Grafana is retained for business dashboards.

| Command/check | Result | What it covers |
| --- | --- | --- |
| `make test` | PASS — 13 tests | CDC parsing/validation, source ordering, selected SCD2 transitions, offset boundary behavior, replay-safe CDC operation counts, and create-only connector registration with mocked HTTP. |
| `py_compile` on modified CDC modules | PASS | Spark streaming entry point, CDC loader, and focused unit test compile. |
| `docker compose --env-file .env config --quiet` | PASS | Local Compose interpolation and schema parsing. Secret values were not displayed. |
| Compose config with explicit profiles | PASS (historical) | Core, batch, CDC, streaming, monitoring, and tools profile configuration parsed at the time. This does not validate the current profile configuration or start services. |
| Environment validation | PASS | Required local configuration was validated without printing credential values. |
| Debezium plugin validator | PASS | Connector config accepted by installed SQL Server plugin; no registration occurred. |
| Focused `git diff --check` | PASS for readiness-task files | Whitespace check scoped to files changed in that task. |

Not proven by these checks: current Airflow snapshot execution against SQL
Server, CDC connector snapshot completion, event delivery, Spark
checkpoint/restart, dual-sink replay, ClickHouse analytics from CDC, Grafana
query results, or backup/restore. See
[Integration Testing](../09-development-and-testing/integration-testing.md)
for the next safe validation gates.
