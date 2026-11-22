# Local environment setup

Use Linux, Docker with Compose v2 and Python 3.11 or 3.12. On Windows, use WSL2 and a Docker engine accessible from the distribution. Keep the checkout on the Linux filesystem for bind-mount and permissions behavior. The environment needs registry, Python package and Maven access for setup and first execution. A database administrator supplies the approved external SQL Server endpoint and reader.

```bash
make install
make init
# Update the SQL Server endpoint and approved reader in the private .env file.
make compose-config
make test
```

[.env.example](../../.env.example) is the reference for variable names and defaults. `make install` prepares the host Python environment. `make init` creates missing local settings and generates secrets. Keep `.env` and interpolated Compose output private, and preserve credentials with their volumes. Changing `STUDENT_ID_HMAC_KEY` changes published identifiers.

| Configuration | Key settings |
|---|---|
| Source | `SOURCE_DB_HOST`, `SOURCE_DB_PORT`, `SOURCE_DB_NAME`, `SOURCE_DB_SCHEMA` and approved reader. Use a trusted TLS certificate. |
| CDC source identity | Set `SOURCE_CDC_USER` and `SOURCE_CDC_PASSWORD` together for a separate reader. Empty values fall back to the batch source credentials. |
| Databases | `STAGING_DB_NAME`, `AIRFLOW_DB_NAME`, `WAREHOUSE_DB_NAME`, `CDC_DB_NAME`. Keep the ClickHouse databases and writer identities distinct. |
| Batch | `BATCH_SNAPSHOT_SCHEDULE` is optional. `SPARK_BATCH_DRIVER_MEMORY` controls local driver sizing. |
| CDC | Keep the connector name, topic prefix, generation, query name and checkpoint directory stable through a routine restart. Size retention and maximum offsets per trigger for the host. |

Run the mode-specific validators before startup.

```bash
.venv/bin/python scripts/validate_environment.py --mode batch
.venv/bin/python scripts/validate_environment.py --mode cdc
```

See the [source readiness guide](../05-data-platform/source-and-staging.md) and [operational runbook](../07-operations/runbook.md). If connectivity fails, test it from the Airflow or Connect service network. Browser access to Airflow confirms only the host-to-UI route.
