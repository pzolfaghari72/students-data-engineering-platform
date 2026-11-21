# CDC Warehouse Design

The CDC DDL is a separate append-only event model. It does not merge CDC rows
into the current batch star schema.

| Object | Engine / semantics | Purpose |
| --- | --- | --- |
| `cdc_events` | `MergeTree`, ordered by generation/source/table/Kafka position | Preserves event envelope, source images, source LSN/version and Kafka identity. |
| `warehouse_stream_generations` | `MergeTree` publication records | Marks a generation as published after snapshot-boundary processing. |
| `active_stream_generations` | `argMax` query-time view | Selects the latest published generation by publication timestamp. |
| `cdc_current_state` | `argMax` by source version and Kafka coordinates | Derives latest nondeleted row per source key for the active generation. |
| `cdc_course_enrollments` | Filter over current-state view | Exposes current enrollment records; it is not joined into the batch fact view. |
| `cdc_student_scd2` | Window-function view over Student events | Derives Student validity periods, current and deleted flags. |

Deletes remain in the event log; current-state views exclude a latest delete
or tombstone. Since MergeTree does not enforce a unique event-position key,
retries may add physical duplicates. The Student view includes an
event-position deduplication window; the generic current-state view relies on
latest-value selection and matching duplicate payloads. Do not depend on
background merges for correctness.

`CourseEnrollments` is a mutable fact: the current batch representation
replaces the full published snapshot, while CDC events represent row changes
in a separate projection. A unified fact requires an approved bootstrap,
dedupe and reconciliation strategy; that integration is not yet validated.
