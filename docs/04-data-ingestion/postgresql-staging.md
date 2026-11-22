# PostgreSQL staging

PostgreSQL hosts two separate databases in this deployment. The staging database keeps typed source snapshots and ingestion metadata. The Airflow database holds scheduler and application state. PostgreSQL fits staging because its constraints, transactions and JDBC access provide a clear reload boundary.

| Schema or object | Grain and purpose |
|---|---|
| `raw.*` | One source row per extraction batch. The primary key is the source key plus `_batch_id`. |
| `metadata.pipeline_runs` | One staged execution, with status, timing and Airflow execution context. |
| `metadata.extraction_batches` | One recorded table extraction attempt and its source/fetched/stored counts. A foreign key identifies its run. |
| `metadata.table_load_status` | Last recorded result per source table. Use the run manifest for whole-snapshot eligibility. |

`_extracted_at` records a table-level extraction timestamp. The `_batch_id` foreign key links each raw row to the run lineage. Batch scope allows a later extract to store the same source key independently. Source foreign keys are explained in the source ERD. Spark validates the joins used by the analytical model.

Staging is still sensitive even though it is typed. Use aggregate verification and avoid selecting names, emails or addresses for a demonstration. The current CDC writer uses ClickHouse. Old `history` and `scd` structures visible in screenshots may remain on migrated volumes. [Staging DDL](../../db/staging_postgres/staging_ddl) · [Recovered staging capture](../14-screenshots/README.md#databases-and-warehouse)
