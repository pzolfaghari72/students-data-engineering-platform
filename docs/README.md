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
| Visual assets and evidence | [Visual gallery](assets/README.md), [earlier validation record](assets/validation-2026-11-21.md) |

The [project README](../README.md) is the entry point for local execution. The [project status](06-observability-and-quality/testing-and-validation.md#implementation-status) identifies what has run and what still needs source-specific acceptance.
