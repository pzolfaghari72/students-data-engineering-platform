# Testing strategy

| Layer | Question and evidence |
|---|---|
| Static | Does Compose interpolate and do definitions resolve? Check config, DAG imports, env settings and datasource UIDs. |
| Unit | Do parsing, lineage checks, state transitions and failure outcomes behave with small synthetic inputs? Run `make test`. |
| Service integration | Do actual PostgreSQL, Spark, Kafka, ClickHouse and Grafana interfaces honor the model? Run the disposable fixtures. |
| Live read-only | Do the deployed services expose the expected publication and read access? Inspect offsets, publisher state, manifests and panel queries. |
| External source acceptance | Can an authorized fixture demonstrate a fresh staged batch and source-side CDC mutations across the deployed route? Run the source checklist with DBA approval. |

A healthy container can still lack the correct view or publication. A passing isolated replay does not establish that source capture permissions or retention work on a new instance. Record results with dates and specify inputs, scope and limits.

Use synthetic rows for mutations and effect assertions. Test both successful cases and interruptions around each visibility boundary. [Unit coverage](unit-testing.md) · [Integration acceptance](integration-testing.md) · [Dated validation](../13-validation/README.md)
