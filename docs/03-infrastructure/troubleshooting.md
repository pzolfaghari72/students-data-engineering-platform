# Infrastructure Troubleshooting

Use the least invasive diagnostic first. For commands see [Operational Runbook](../10-operations/operational-runbook.md).

| Symptom | Possible cause | First check | Safe action |
| --- | --- | --- | --- |
| Compose missing variable | `.env` missing or placeholder | `docker compose config --quiet` | Correct the specific local environment key without exposing secrets. |
| Service exits or restarts | Config error, failed dependency, memory | Service-specific status and recent logs | Fix the root cause; do not delete named volumes. |
| Host cannot reach database | Wrong host port, loopback-only binding | Compose port mapping; host vs container endpoint | Use host published port for host tools, service DNS inside containers. |
| Container DNS unavailable | Incorrect network or service absent | `docker compose ps -a` | Start only the service required for the task. |
| Image / Maven download fails | External registry unavailable | Build/download error output | Retry only affected image/package fetch, not database initialization. |
| CPU or memory pressure | Too many simultaneous workloads | `docker stats --no-stream` | Avoid overlapping batch Spark job with active streaming load. |
| Data appears missing | Wrong Docker project/volume or wrong published generation | Named volumes, ClickHouse publication markers | Confirm state before any cleanup; never run `down -v` to troubleshoot. |
| CDC at 100% but `BOOTSTRAPPING` | Boundary consumed, publication not yet confirmed | PostgreSQL generation status and ClickHouse active-generation views | Inspect read-only; do not clear checkpoints or re-register Debezium. |

The [2026-11-16 CDC readiness report](../13-validation/cdc-readiness-report.md) documented blockers at that date. A newer [2026-11-21 validation update](../13-validation/validation-update-2026-11-21.md) observed running streaming and consumed snapshot offsets. Treat both reports as dated observations; neither is a substitute for current read-only checks.
