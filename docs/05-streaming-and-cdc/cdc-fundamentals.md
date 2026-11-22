# Change data capture fundamentals

Change data capture (CDC) turns committed source changes into records that another system can process. A log-based approach follows source change positions and avoids rescanning an entire table to discover every update. SQL Server exposes CDC change tables, and Debezium turns them into a keyed event stream.

| Concept | Meaning in this project |
|---|---|
| Baseline | Initial keyed row images used to reconstruct the source's starting state |
| Source position | SQL Server commit/change LSNs and an event serial ordered within source lineage |
| Transport position | Kafka topic, partition and offset identifying a delivery |
| Current state | Latest source image per key in the published CDC generation, excluding keys whose latest event is a delete |
| Replay | Processing retained deliveries again after interruption, with an existing seal used to verify prior writes |

For example, a capacity update keeps the same offering key and carries a newer source version. Processing the update twice at the same transport position changes neither the counted offering nor its latest capacity twice. If Debezium delivers the same source version at a new Kafka position, the logical event view can still recognize the repeated change.

Batch continues to provide separately verified full snapshots and a dimensional model. A difference between batch and CDC may show a later source change, a lagging stream or a reconciliation defect. The publication references explain which case can be tested. [SQL Server capture](sql-server-cdc.md) · [Event identity and examples](event-processing.md)
