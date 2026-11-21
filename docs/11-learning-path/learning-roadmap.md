# Learning Roadmap

Work through the platform in this order. The first stages can run locally;
source CDC and complete streaming exercises require external access and are
currently blocked by the readiness findings.

| Phase | Topic | Practical outcome |
| --- | --- | --- |
| 1 | Architecture | Explain components, data paths and current validation limits. |
| 2 | Infrastructure | Start/inspect only core services and understand persistence. |
| 3 | Batch pipeline | Trace a consistent source snapshot through staging and warehouse. |
| 4 | CDC/streaming | Explain events, offsets and recovery; integration needs DBA/source readiness. |
| 5 | Warehouse | Query fact grain, snapshot publication and CDC projections. |
| 6 | Business analytics | Understand KPI definitions and dashboard query semantics. |

Each phase has a focused worksheet:
[Phase 1](./phase-01-architecture.md),
[Phase 2](./phase-02-infrastructure.md),
[Phase 3](./phase-03-batch-pipeline.md),
[Phase 4](./phase-04-streaming-pipeline.md),
[Phase 5](./phase-05-data-warehouse.md), and
[Phase 6](./phase-06-business-analytics.md). Do not execute source-mutating
exercises without explicit owner approval.
