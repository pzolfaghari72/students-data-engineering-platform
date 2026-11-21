# Transformations

This page maps transformation logic to code rather than proposing a new
semantic layer.

| Transformation | Implementation | Result |
| --- | --- | --- |
| Select source batches | `_latest_snapshot_batches()` | Only the latest successful full run is selected; required table batches are checked. |
| Student pseudonymization | `hmac(..., STUDENT_ID_HMAC_KEY, "SHA-256")` | The environment-managed HMAC key links student dimension and fact rows; the same key preserves pseudonyms across loads until rotated. Protect it as sensitive linkage data. |
| Enrollment enrichment | `build_warehouse_frames()` | Joins enrollment to offering, student, course, term and optional grade. |
| GPA attributes | Grade points and `IncludeInGpa` are carried to fact | The dashboard computes a weighted example; not an approved institutional definition. |
| CDC normalization | `normalize_debezium_record()` | Verifies topic/source metadata and preserves LSNs and Kafka positions. |
| Source version order | `source_version_order()` | Builds a sortable source version string from LSN/serial fields for CDC views. |

No separate DataFrame transformation package or business-rule registry is
implemented. Changes to grade, repeat-course or enrollment semantics require
source-owner approval and tests that establish expected grain/results.
