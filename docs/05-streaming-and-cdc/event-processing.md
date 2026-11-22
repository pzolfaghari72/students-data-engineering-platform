# CDC event processing

Connect serializes a record key and value as JSON without the surrounding schema by default. The value contains operation code, source metadata and before/after images when available. The key identifies a source row, including composite keys where needed. The normalizer checks the configured database, schema and table, validates keys and LSN shapes, and canonicalizes JSON for stable fingerprints. [Normalizer](../../spark/cdc_events.py)

| Input | Normalized behavior |
|---|---|
| `r` snapshot read | Requires an after image. Serving selects reads from the verified completed snapshot LSN. |
| `c` create | A later source version can introduce a key or reintroduce a deleted one. |
| `u` update | Requires an after image. The latest valid source version determines current state. |
| `d` delete | Kept as a logical change. A latest delete excludes the key from current state. |
| Kafka null-valued record | A keyed tombstone counts toward transport coverage. The serving view filters it out of source changes. |

A transport record has a topic, partition and offset within the generation's verified broker lineage. A logical event is grouped by active generation, source table, canonical key, operation and source version. Spark fingerprints normalized content to compare the stored transport record; the ClickHouse view groups repeated logical source events even when their Kafka offsets differ.

**Example.** A course offering appears in the initial boundary with capacity 10. A later source update increases it to 12. An older update delivered again may occupy a newer Kafka offset, but its older source LSN leaves capacity at 12. A later delete drops the offering from the KPI. A tombstone can follow the delete without creating a second logical change.

Malformed keys, unknown tables, unsafe envelopes and unexpected positions fail processing. Use the topic and offset in the failure message, then investigate the record under authorized access. Compatible new fields can be preserved in the payload. Changes to the key or a field used in analytics require explicit validation. [CDC views](../07-data-warehouse/cdc-warehouse-design.md) · [Replay fixture](../../tests/integration/streaming_replay.py)
