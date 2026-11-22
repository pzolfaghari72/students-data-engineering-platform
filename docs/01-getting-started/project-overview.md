# Project overview

The Student Data Engineering Platform turns records from an external SQL Server database into reproducible academic analytics. The source models applications, students, course offerings and enrollment attempts. The analytical questions include how many seats were offered by term, which departments have the most enrollment demand, and how recorded grades are distributed.

| Need | Data product | Freshness |
|---|---|---|
| Compare terms, departments, capacity and grades under a stable set of inputs | Batch dimensional warehouse | Most recent verified full source run |
| View current source totals and CDC health | CDC current-state aggregates | Most recent verified Spark microbatch in the active generation |
| Diagnose disagreement between the two paths | Read-only reconciliation | Two explicit publication references |

A full snapshot is also a recovery and comparison point. The CDC path avoids repeating the full extract for a change to one record. The pipelines share an analytical server and frontend while retaining separate writers, data models and recovery state.

This project models a single-host development and portfolio deployment. Databases, broker and checkpoints persist in Docker volumes. Airflow and Spark own different pieces of work. No external scheduler, metrics cluster or stream-to-batch dependency is needed.

[System design](../02-architecture/architecture-overview.md) · [Assessment](../04-data-ingestion/sql-server-source.md) · [Current evidence](../12-implementation/implementation-status.md)
