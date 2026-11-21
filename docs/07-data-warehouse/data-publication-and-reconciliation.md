# Data Publication and Reconciliation

Batch and streaming use different publication markers.

## Batch generation

Spark creates a new UUID, writes the nine dimension and three fact snapshots
to versioned physical tables, verifies unique dimension keys and source-row
count preservation, then inserts `warehouse_model_loads_v2`. Published views
select the latest completed model snapshot. Original physical tables are not
dropped or truncated. A failed attempt can leave unpublished rows; there is
no automatic cleanup.

The original `warehouse_loads` marker and physical tables remain for rollback.
The model views now select only the v2 publication marker; restoring the old
model requires restoring the corresponding view definitions, not deleting
data.

## Streaming generation

Spark persists generation metadata in PostgreSQL. Once the Debezium snapshot
completion metric is true, it captures Kafka end offsets and waits for
consumption through each partition's captured end. It writes CDC event rows to
ClickHouse and then inserts a `PUBLISHED` generation record; PostgreSQL status
is marked active afterward. The implementation is configured but untested,
and no distributed transaction spans these writes.

## Reconciliation needed before bootstrap

The readiness check observed existing PostgreSQL history/offsets and no
matching source topics in the current Kafka cluster. A new connector could
recreate positions that collide with the unique PostgreSQL key. Preserve the
existing database, Kafka volumes, connector offsets and checkpoints; first
identify the original topic lineage or approve a new isolated generation and
reconciliation. Do not call a count match alone proof of correctness.

The [CDC readiness report](../13-validation/cdc-readiness-report.md) records
the observed counts and blockers. A future validation should compare source
table counts/keys, event positions, current projections, delete behavior,
and published warehouse rows at a documented boundary.
