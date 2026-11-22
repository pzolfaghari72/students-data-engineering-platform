# Data quality and reconciliation

Data quality is enforced at system boundaries. A passing row count is a useful control and says little about key shape, duplicated grain or the correctness of a measure. The loader combines count, identity and selected-value checks before readers see new data.

| Boundary | Implemented check | Failure outcome |
|---|---|---|
| SQL Server → staging | Source count, fetched rows and stored count for each of the 15 table batches | Failed or partial staging run cannot trigger publication |
| Staging → Spark | Explicit successful run and validated batches; checks on model joins, grain and optional reference values | Warehouse task fails before a new publication |
| Spark → ClickHouse batch | Read back counts and distinct grain keys per candidate table, plus selected aggregates | Candidate remains unpublished |
| Kafka → CDC | Topic and partition lineage, contiguous offset progression, record-shape validation | Batch fails before advancing verified progress |
| Spark → ClickHouse CDC | Compare persisted transport positions and hashes with input, then seal | Unsealed data stays out of serving views |
| CDC generation | Coverage through saved end offsets and a verified baseline with consistent row images | Generation remains bootstrapping |

The cross-database reconciliation query compares three separately validated business grains. Use its timestamps and lineage to make comparisons at meaningful boundaries. A count match is not row-by-row verification. [Publication and reconciliation](../07-data-warehouse/data-publication-and-reconciliation.md) · [Integration checks](../09-development-and-testing/integration-testing.md)
