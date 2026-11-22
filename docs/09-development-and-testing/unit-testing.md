# Unit testing

Run `make test` from the repository root. The Makefile invokes standard-library `unittest` with `airflow/dags` and `spark` on `PYTHONPATH`. The tests can run without Docker and avoid the live source.

| File | Coverage |
|---|---|
| `test_batch_snapshot.py` | Exact run identity, valid extraction manifest, and rejection of missing, duplicate or unreconciled batches |
| `test_debezium_registration.py` | Connector create-or-check behavior, REST response contracts, matching table filters and lineage drift |
| `test_staging_cdc.py` | Envelope parsing, ordering, schema conditions, bootstrap evidence, contiguous offsets, sink seals and replay |
| `test_warehouse_keys.py` | HMAC key stability, entity namespace separation and rejection of an empty secret |

Mocks prove local decision paths. The runtime fixtures use Spark and actual data services to exercise connector boundaries and SQL. Do not compare the current test count with the number in an older report without accounting for suite changes. [Integration fixture instructions](integration-testing.md)
