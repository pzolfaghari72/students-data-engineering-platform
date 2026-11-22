# Infrastructure troubleshooting

Read `make status` and service logs first. Use the checkpoint and database metadata to distinguish a startup failure from an application fault.

| Symptom | Narrow the issue |
|---|---|
| Docker socket or daemon unavailable | Check distribution integration, Docker context and local account access. |
| One-shot service exited | Inspect its exit code and logs. Zero can mean schema creation or registration completed successfully. |
| UI works but queries error | Confirm the datasource UID and database view exist, credentials are current, and the account has the intended read grants. Preserve persisted Grafana datasource identity on upgrades. |
| Spark runtime preflight fails | Compare the running Airflow scheduler image with the configured build and check Java, `spark-submit` and application mounts. |
| Spark cannot open its writer lock | Check ownership of the existing checkpoint volume root. Apply the [upgrade procedure](../10-operations/operational-runbook.md) while the worker is stopped. |
| Kafka healthy flag or Connect health fails | Check the internal listener and broker CLI, then Connect connector and task status. Service health is weaker evidence than pipeline publication. |
| CDC reads reach 100% while still bootstrapping | Check final baseline validation and publication marker creation. If ClickHouse restarted, inspect memory and retry with the same paired checkpoint and generation. |
| Build or artifact resolution fails | Confirm registry/Maven access and compare the declared dependency version with the active image. |

The [runbook](../10-operations/operational-runbook.md) describes state-preserving recovery. The [validation history](../13-validation/README.md) preserves older failure cases with their original deployment context.
