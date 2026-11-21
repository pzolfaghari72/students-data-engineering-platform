# Unit Testing

The test suite uses Python `unittest` and runs without starting platform
services. `make test` sets `PYTHONPATH=airflow/dags` and invokes discovery in
`tests/unit`.

| Test file | Coverage |
| --- | --- |
| `tests/unit/test_staging_cdc.py` | Debezium snapshot normalization; invalid source schema/missing image rejection; LSN ordering; selected SCD2 changes/deletes/tombstones; monotonic Kafka offsets and snapshot-boundary guard. |
| `tests/unit/test_debezium_registration.py` | Existing connector is not modified; absent connector is created by POST; 404 handling; flat plugin-validator payload. |

The tests use synthetic records and mocked HTTP; they do not prove compatibility
with a live SQL Server connector or ClickHouse. The dated count and command are
in [test results](../13-validation/test-results.md).
