# Debezium SQL Server Connector

Debezium runs inside Kafka Connect and converts SQL Server CDC records into
Kafka topic events. `debezium/register_connector.py` builds configuration
only from required environment variables, validates it with the installed
plugin, and creates the connector only if it does not already exist. Existing
connector configuration/offsets are deliberately left untouched.

## Configured behavior

| Setting | Project behavior |
| --- | --- |
| Connector class | `io.debezium.connector.sqlserver.SqlServerConnector` |
| Captured tables | Explicit 15-table `dbo` include list from `SOURCE_TABLES` |
| Snapshot | `snapshot.mode=initial`; not an Airflow snapshot boundary |
| Serialization | Kafka Connect JSON converter, schema embedding disabled |
| Data topics | `<topic.prefix>.<database>.<schema>.<table>` |
| Schema history | Dedicated internal Kafka topic from `DEBEZIUM_SCHEMA_HISTORY_TOPIC` |
| Registration | POST create-only; existing connector is only checked for health |
| Completion | Helper's RUNNING status is not snapshot completion; JMX metric is required |

Connect REST, the SQL Server plugin and plugin configuration validation passed
in the recorded readiness check. The connector was not registered because
source permissions and the prior PostgreSQL/Kafka lineage mismatch remain
unresolved. See [readiness](../13-validation/cdc-readiness-report.md) and
the implementation at `debezium/register_connector.py`.
