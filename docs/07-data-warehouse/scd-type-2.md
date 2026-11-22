# Slowly changing dimension Type 2

A Type 2 dimension gives a business entity multiple versions. Each version has a version key and is valid over a defined interval. An as-of fact join selects the dimension version valid at the fact's time. This lets historical analytics answer questions such as which department a student belonged to when an event occurred.

| Representation | Example | Status here |
|---|---|---|
| Full snapshot | Dimension rows grouped by `snapshot_id` | Implemented in the batch warehouse |
| Current state | Latest qualifying CDC version for a source key | Implemented as a CDC view |
| Event history | Ordered changes for a key in the active generation | Implemented as a CDC view over verified records |
| Type 2 dimension | Per-key version rows and business validity intervals | Educational concept for the current architecture |

For example, two snapshots can show an attribute changing between Friday and Monday. A Type 2 interval would specify when each version took effect and allow as-of joins. CDC event history preserves changes but needs a separately designed business effective time, late-arrival policy and fact linkage to serve that role.

The earlier implementation experimented with PostgreSQL history and SCD structures. Older volume contents and screenshots are historical evidence. The current pipeline publishes separate batch and CDC models. [Historical screenshot](../14-screenshots/README.md#retired-airflow-cdc-consumer) · [Model decision](../adr/ADR-002-dimensional-modeling.md)
