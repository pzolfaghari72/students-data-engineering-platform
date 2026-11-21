# Spark Batch

The entry point is `spark/load_warehouse.py`. It reads PostgreSQL metadata and
raw batches through JDBC, constructs nine dimensions and three facts, and
writes a ClickHouse snapshot. It does not read or modify the CDC event log.

The loader requires the latest successful full run and successful batches for
its 13 model inputs. `StudentProfiles` is intentionally excluded pending
source-relationship validation. The job pseudonymizes applicant/student
identifiers, excludes direct identifiers and date of birth, builds a calendar
from application-received dates, and validates dimension/fact key uniqueness,
dimension row counts and fact row-count preservation before writing. JDBC
reads use a bounded fetch size so large tables stream instead of filling the
driver heap.

Every attempt creates a new UUID `snapshot_id`. Rows are appended to
versioned MergeTree tables; `warehouse_model_loads_v2` is inserted last as the
completion marker. Failed writes can leave unpublished rows but do not switch
the current model views. Existing physical tables remain for rollback, and
there is no cleanup job.

Airflow runs the job with local Spark from the scheduler container. This
avoids Docker-socket access and extra services. The existing one-shot Compose
Spark service remains a manual alternative; do not run it concurrently with
the Airflow task.

Runtime invocation and resource settings are in
[Batch Execution](../10-operations/batch-execution.md); model details are in
[Fact and Dimension Tables](../07-data-warehouse/fact-and-dimension-tables.md).
