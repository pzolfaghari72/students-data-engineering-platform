# Debezium and Kafka Connect

Kafka Connect runs reusable connectors as workers and tasks. A source task obtains changes from an external system and writes Kafka records. Connect stores its configuration, source offsets and task status in dedicated topics. Debezium supplies the SQL Server connector and its envelope format.

The local deployment uses a Debezium 2.7.3 Connect image. The source task obtains SQL Server row images and emits records for the configured table list. A fresh connector uses the initial snapshot settings. The checked-in registration helper validates the plugin configuration and creates a connector when absent. Existing configuration is checked for lineage drift and left intact. New registrations can use the Connect environment config provider for the source password.

The `RUNNING` state returned by Connect identifies the connector/task lifecycle. Snapshot completion comes from the connector's committed offsets and the retained terminal snapshot record used by Spark. JMX snapshot metrics provide diagnostics. They are not the application's publication gate.

For diagnosis, read the REST status for the configured connector name, check the source and broker, then inspect durable offsets and Spark progress. Keep the connector name, prefix and schema history paired during ordinary recovery. [Connector registration](../../debezium/register_connector.py) · [Debezium 2.7 SQL Server reference](https://debezium.io/documentation/reference/2.7/connectors/sqlserver.html) · [Runtime checks](../10-operations/streaming-execution.md)
