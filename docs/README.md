# Project documentation

Read the sections in order for a learning path, or start with the part of the system you want to run, inspect or explain.

| Section | Guides |
|---|---|
| 01 · Foundation | [Data engineering concepts and reading path](01-foundation/data-engineering.md) |
| 02 · Architecture | [System architecture and flows](02-architecture/overview.md), [database models and ERDs](02-architecture/data-model.md), [engineering decisions and evolution](02-architecture/decisions.md) |
| 03 · Infrastructure | [Local setup and environment](03-infrastructure/setup.md), [Compose services, network and persistent state](03-infrastructure/services.md) |
| 04 · Data pipelines | [Batch snapshots](04-data-pipelines/batch.md), [CDC streaming](04-data-pipelines/cdc.md), [publication and reconciliation](04-data-pipelines/reconciliation.md) |
| 05 · Data platform | [SQL Server source and PostgreSQL staging](05-data-platform/source-and-staging.md), [ClickHouse analytics and dimensional model](05-data-platform/warehouse.md) |
| 06 · Observability and quality | [Grafana and metrics](06-observability-and-quality/grafana.md), [testing and implementation evidence](06-observability-and-quality/testing-and-validation.md) |
| 07 · Operations | [Operational runbook](07-operations/runbook.md) |
| Visual assets and evidence | [Visual gallery](assets/README.md), earlier validation at `55843c3:docs/assets/validation-2026-11-21.md` |

The [project README](../README.md) is the entry point for local execution. The [project status](06-observability-and-quality/testing-and-validation.md#implementation-status) records dated observations and the remaining end-to-end checks.

## Earlier documentation

The foundation guide carries forward the learning roadmap. Infrastructure and operations collect the earlier setup, service and recovery guides. Data pipeline and platform guides bring together the ingestion, processing and warehouse material. The engineering decisions retain the original ADR IDs, and the gallery identifies which captures still describe the running design.

Use Git to retrieve the original wording or earlier implementation evidence. Revision `99306b6` contains the earlier technical context, learning phases and individual ADRs. It describes earlier deployment states, so use the guides above for current instructions.

```bash
git ls-tree -r --name-only 99306b6 -- docs/11-learning-path docs/12-implementation docs/13-validation docs/adr
git show 99306b6:docs/PROJECT_TECHNICAL_CONTEXT.md
git show 99306b6:docs/adr/ADR-003-etl-strategy.md
```
