# Learning Roadmap

This project teaches a full snapshot and a separate continuously checkpointed CDC stream. As of the 2026-11-21 checks, the streaming **snapshot boundary** has been consumed, although the generation's final publication state still needs confirmation. It is no longer accurate to describe CDC as never started.

| Phase | Topic | Practical outcome |
| --- | --- | --- |
| 1 | Architecture | Distinguish full-snapshot analytics from streaming CDC event paths. |
| 2 | Infrastructure | Start and inspect only the required services; understand named-volume persistence. |
| 3 | Batch pipeline | Trace 15 SQL Server source tables through PostgreSQL staging and a ClickHouse published snapshot. |
| 4 | CDC / streaming | Understand Debezium envelopes, Kafka offsets, Spark checkpoints, generation publication and multi-sink retries. |
| 5 | Warehouse | Query nine dimensions / three facts; contrast batch snapshots with CDC current-state and SCD2 projections. |
| 6 | Business analytics | Interpret the 18-panel Grafana Student Enrollment, Capacity & Grades dashboard accurately. |

Read [Phase 1](./phase-01-architecture.md), [Phase 2](./phase-02-infrastructure.md), [Phase 3](./phase-03-batch-pipeline.md), [Phase 4](./phase-04-streaming-pipeline.md), [Phase 5](./phase-05-data-warehouse.md) and [Phase 6](./phase-06-business-analytics.md).

The remaining engineering work is to verify CDC activation and change/replay behavior, then (if desired) build a **new, safe incremental SCD2 analytical loader**. Do not schedule the existing full-snapshot warehouse DAG as if it were already that incremental loader. Source mutations require approval and a controlled training dataset.
