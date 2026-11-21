# Infrastructure Troubleshooting

Use the least invasive diagnostic first. Common commands are in the
[operational runbook](../10-operations/operational-runbook.md).

| Symptom | Possible cause | Diagnostic | Resolution |
| --- | --- | --- | --- |
| Compose reports missing variable | `.env` absent, placeholder or malformed | Run environment validation and `docker compose config --quiet`; do not print secret values. | Correct the named key in local `.env`. |
| Container exits or restarts | Invalid config, dependency unavailable, OOM | Inspect service status and bounded service logs. | Fix root cause; avoid deleting volumes. |
| Host tool cannot reach database | Wrong published port or service only bound to loopback | Compare `.env` with Compose mapping; use internal service DNS only from containers. | Connect to the host-published port from host tools. |
| Service DNS fails | Wrong Compose network/profile or service not running | Inspect Compose service/network state. | Start the required profile and dependencies. |
| Image build/download fails | Registry, Maven or plugin download unavailable | Inspect build output and retry only the failed image fetch. | Restore allowed network access; do not reset databases to retry image builds. |
| Container OOM / WSL memory pressure | Too many optional profiles/workloads | Inspect Docker stats and host/WSL allocation. | Stop optional services and avoid concurrent Spark jobs. |
| Core data seems missing after recreation | Wrong project name/volume or destructive volume removal | Inspect named-volume attachment; do not run cleanup commands. | Restore the correct volume or approved backup; avoid recreating state. |
The current external CDC blockers are not Docker failures; see the dated
[CDC readiness report](../13-validation/cdc-readiness-report.md).
