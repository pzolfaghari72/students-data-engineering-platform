# Batch execution

Start from the [batch DAG flow](../02-architecture/diagrams/batch-pipeline.md). Trigger `sqlserver_to_postgres_staging` in Airflow to start a new full extract. Verify the two DAGs are runnable and follow the same staging run ID through the warehouse trigger. Choose a controlled interval for the source reads.

In the staging database, use a read-only query with the actual run ID substituted for the example value.

```sql
SELECT source_table, status, source_rows_expected, extracted_rows, inserted_rows
FROM metadata.extraction_batches
WHERE run_id = 123
ORDER BY source_table;
```

Check for all 15 eligible table batches and reconciled counts. The warehouse uses its own run with that explicit source ID. For a warehouse-only retry use `make warehouse-load RUN_ID=123` with the actual successful staging ID. Re-run the extract as a new staging run if extraction failed.

In the batch ClickHouse database, inspect the published state.

```sql
SELECT source_run_id, snapshot_started_at, completed_at,
       fact_application_rows, fact_course_offering_rows, fact_enrollment_rows
FROM batch_publication_status;
```

An older retained publication can lack `snapshot_started_at`. Use the available publication references and do not substitute its publication time for the source boundary. Check Grafana and the comparison query only after verifying the manifest. [Runbook](operational-runbook.md) · [Batch validity rules](../06-data-processing/data-quality.md)
