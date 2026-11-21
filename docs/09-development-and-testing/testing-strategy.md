# Testing Strategy

Validation is layered; each layer proves a different property.

| Layer | Current check | Proves | Does not prove |
| --- | --- | --- | --- |
| Static configuration | `docker compose config --quiet` | Compose interpolates and parses for the local environment. | Services start or integrations work. |
| Unit | `make test` | Selected CDC parsing, offset-boundary, SCD2 transition and connector-registration behavior. | SQL Server, Kafka, Airflow or Spark integration. |
| Runtime health | Compose health checks / REST endpoints | Selected process endpoints are responsive. | Initial snapshot completion, warehouse correctness or complete pipeline. |
| Component integration | Not fully automated | Requires live source, databases and broker. | No single current test covers all sources/sinks. |
| End to end | Not validated in current readiness report | Would require source INSERT/UPDATE/DELETE and restart tests through warehouse/Grafana. | The current code/configuration must not be presented as end-to-end certified. |

See the focused [unit test guide](./unit-testing.md), the
[integration test plan](./integration-testing.md), and dated
[test results](../13-validation/test-results.md).
