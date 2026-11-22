import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from debezium import register_connector


class ConnectorRegistrationTests(unittest.TestCase):
    @patch.dict(
        "os.environ",
        {
            "CONNECT_REST_URL": "http://connect:8083",
            "CONNECTOR_HEALTH_TIMEOUT_SECONDS": "10",
        },
        clear=True,
    )
    @patch("debezium.register_connector._wait_for_connector")
    @patch("debezium.register_connector._wait_for_connect")
    @patch("debezium.register_connector._validate_connector_config")
    @patch("debezium.register_connector._existing_connector_config")
    @patch("debezium.register_connector._connector_config")
    @patch("debezium.register_connector._request_json")
    def test_existing_connector_is_not_updated(
        self,
        request_json,
        connector_config,
        existing_config,
        validate_config,
        wait_for_connect,
        wait_for_connector,
    ) -> None:
        connector_config.return_value = ("students", "students", {"key": "value"})
        existing_config.return_value = {"key": "existing"}

        register_connector.register_connector()

        existing_config.assert_called_once_with(
            "http://connect:8083", "students"
        )
        validate_config.assert_not_called()
        request_json.assert_not_called()
        wait_for_connector.assert_called_once_with(
            "http://connect:8083", "students", 10
        )

    @patch.dict(
        "os.environ",
        {
            "CONNECT_REST_URL": "http://connect:8083",
            "CONNECTOR_HEALTH_TIMEOUT_SECONDS": "10",
        },
        clear=True,
    )
    @patch("debezium.register_connector._wait_for_connector")
    @patch("debezium.register_connector._wait_for_connect")
    @patch("debezium.register_connector._validate_connector_config")
    @patch("debezium.register_connector._existing_connector_config")
    @patch("debezium.register_connector._connector_config")
    @patch("debezium.register_connector._request_json")
    def test_missing_connector_is_created_without_putting_config(
        self,
        request_json,
        connector_config,
        existing_config,
        validate_config,
        wait_for_connect,
        wait_for_connector,
    ) -> None:
        config = {"key": "value"}
        connector_config.return_value = ("students", "students", config)
        existing_config.return_value = None

        register_connector.register_connector()

        validate_config.assert_called_once_with(
            "http://connect:8083", "students", config
        )
        request_json.assert_called_once_with(
            "http://connect:8083/connectors",
            method="POST",
            payload={"name": "students", "config": config},
        )
        wait_for_connector.assert_called_once_with(
            "http://connect:8083", "students", 10
        )

    def test_detects_topic_lineage_drift(self):
        with self.assertRaisesRegex(RuntimeError, "topic.prefix"):
            register_connector.check_existing_config({"topic.prefix": "other"}, {"topic.prefix": "students"})

    def test_accepts_equivalent_existing_table_filter(self):
        register_connector.check_existing_config(
            {"table.include.list": "dbo.Students,dbo.Grades"},
            {"table.include.list": r"dbo\.Grades,dbo\.Students"},
        )

    @patch("debezium.register_connector._request_json")
    def test_missing_connector_config_returns_none(self, request_json) -> None:
        request_json.side_effect = HTTPError(
            "http://connect:8083/connectors/students/config",
            404,
            "not found",
            {},
            None,
        )

        self.assertIsNone(
            register_connector._existing_connector_config(
                "http://connect:8083", "students"
            )
        )

    @patch("debezium.register_connector._request_json")
    def test_plugin_validation_receives_flat_connector_config(
        self,
        request_json,
    ) -> None:
        config = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": "source",
        }
        request_json.return_value = {"configs": []}

        register_connector._validate_connector_config(
            "http://connect:8083", "students", config
        )

        request_json.assert_called_once_with(
            "http://connect:8083/connector-plugins/"
            "io.debezium.connector.sqlserver.SqlServerConnector/config/validate",
            method="PUT",
            payload={"name": "students", **config},
        )


if __name__ == "__main__":
    unittest.main()
