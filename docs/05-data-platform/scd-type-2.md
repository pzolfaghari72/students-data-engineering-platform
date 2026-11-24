# Incremental dimension history

Eight attribute-bearing dimensions expose `incremental_dim_*_history`. The calendar is derived deterministically from application dates and exposes the existing `dim_date` grain as `incremental_dim_date`.

The warehouse keeps typed source mutations and their source ordering keys. A dimension's history uses change points from its own key and its reference relationships. For example, editing a degree level reprojects the effective description for the degrees and class standings that referred to it. Each point is evaluated against the related rows as of that SQL Server commit LSN. Equivalent consecutive projections are collapsed. The previous version ends when the next distinct projection starts.

| Column | Meaning |
|---|---|
| Existing `*_key` | Stable business-key surrogate, compatible with the Batch model |
| `version_key` | Stable hash of that key and the beginning source position |
| `valid_from`, `valid_to` | UTC event times for the version's start and next boundary |
| `valid_from_position`, `valid_to_position` | Authoritative half-open interval on SQL Server commit LSNs |
| `is_current` | Latest known version for the key |
| `is_deleted` | Closed-out current entity or a missing required relationship at that boundary |

Within a single SQL Server commit, the final change order defines the visible source row. Two commits may have the same event timestamp. Use the position columns for unambiguous as-of queries. A late event can insert or adjust an earlier version boundary. The newest source version still defines the current row. A delete creates a tombstone version, and reactivation creates a later current version with the same business-key surrogate. No data observed before the retained CDC baseline is available for this history.

For example, inspect the current version and preceding history for a selected pseudonymous student key:

```sql
SELECT student_status, valid_from, valid_to, is_current, is_deleted
FROM incremental_dim_student_history
WHERE student_key = {student_key:String}
ORDER BY valid_from_position;
```

Facts use the existing stable dimension keys. Current facts resolve their lookups from the same incremental publication as their direct source rows. This keeps grade points, course credits and term or department relationships current when a lookup changes. ClickHouse stores typed baselines and append-only changes, and history views resolve intervals at query time. Grafana reads current projections. It does not need to evaluate the history windows. [Warehouse models](warehouse.md)
