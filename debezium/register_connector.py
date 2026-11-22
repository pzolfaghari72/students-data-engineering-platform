"""Register the SQL Server Debezium connector using environment settings."""

from __future__ import annotations

import json
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from students_staging.source_tables import SOURCE_TABLES


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set.")
    return value


def _boolean(name: str) -> str:
    value = _required(name).strip().lower()
    if value in {"true", "1", "yes", "y"}:
        return "true"
    if value in {"false", "0", "no", "n"}:
        return "false"
    raise ValueError(f"{name} must be a boolean value.")


def _request_json(
    endpoint: str,
    *,
    method: str = "GET",
    payload: dict[str, object] | None = None,
) -> object:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    with urlopen(request, timeout=30) as response:
        body = response.read()
    return json.loads(body) if body else {}


def _wait_for_connect(rest_url: str, timeout_seconds: int) -> None:
    endpoint = f"{rest_url}/connectors"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with urlopen(endpoint, timeout=5):
                return
        except (URLError, TimeoutError):
            time.sleep(2)
    raise RuntimeError("Kafka Connect REST API did not become available in time.")


def _connector_config() -> tuple[str, str, dict[str, str]]:
    connector_name = _required("DEBEZIUM_CONNECTOR_NAME")
    topic_prefix = _required("DEBEZIUM_TOPIC_PREFIX")
    source_schema = _required("SOURCE_DB_SCHEMA")
    snapshot_mode = _required("DEBEZIUM_SNAPSHOT_MODE")
    if snapshot_mode != "initial":
        raise ValueError(
            "DEBEZIUM_SNAPSHOT_MODE must be 'initial' for a fresh bootstrap."
        )
    isolation = os.getenv("DEBEZIUM_SNAPSHOT_ISOLATION_MODE", "snapshot")
    if isolation not in {"snapshot", "repeatable_read"}:
        raise ValueError("Unsupported Debezium snapshot isolation mode.")
    tables = ",".join(
        rf"{re.escape(source_schema)}\.{re.escape(source_table)}"
        for source_table, _ in SOURCE_TABLES
    )
    config = {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "database.hostname": _required("SOURCE_DB_HOST"),
        "database.port": _required("SOURCE_DB_PORT"),
        "database.user": _required("SOURCE_DB_USER"),
        "database.password": "${env:CDC_SOURCE_PASSWORD}",
        "database.names": _required("SOURCE_DB_NAME"),
        "database.encrypt": _boolean("SOURCE_DB_ENCRYPT"),
        "database.trustServerCertificate": _boolean(
            "SOURCE_DB_TRUST_SERVER_CERTIFICATE"
        ),
        "topic.prefix": topic_prefix,
        "table.include.list": tables,
        "snapshot.mode": snapshot_mode,
        "snapshot.isolation.mode": isolation,
        "include.schema.changes": "false",
        "schema.history.internal.kafka.bootstrap.servers": _required(
            "KAFKA_BOOTSTRAP_SERVERS"
        ),
        "schema.history.internal.kafka.topic": _required(
            "DEBEZIUM_SCHEMA_HISTORY_TOPIC"
        ),
        "schema.history.internal.store.only.captured.tables.ddl": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    }
    return connector_name, topic_prefix, config


def _validate_connector_config(
    rest_url: str,
    connector_name: str,
    config: dict[str, str],
) -> None:
    plugin = config["connector.class"]
    response = _request_json(
        f"{rest_url}/connector-plugins/{plugin}/config/validate",
        method="PUT",
        payload={"name": connector_name, **config},
    )
    if not isinstance(response, dict):
        raise RuntimeError("Kafka Connect returned an invalid validation response.")
    invalid = [
        item.get("value", {}).get("name", "unknown")
        for item in response.get("configs", [])
        if item.get("value", {}).get("errors")
    ]
    if invalid:
        raise RuntimeError(
            "Kafka Connect rejected connector configuration for: "
            + ", ".join(sorted(set(invalid)))
        )


def _existing_connector_config(
    rest_url: str,
    connector_name: str,
) -> dict[str, object] | None:
    endpoint = f"{rest_url}/connectors/{connector_name}/config"
    try:
        response = _request_json(endpoint)
    except HTTPError as error:
        if error.code == 404:
            return None
        raise RuntimeError(
            f"Could not inspect connector configuration (HTTP {error.code})."
        ) from error
    if not isinstance(response, dict):
        raise RuntimeError("Kafka Connect returned invalid connector configuration.")
    return response


def _wait_for_connector(
    rest_url: str,
    connector_name: str,
    timeout_seconds: int,
) -> None:
    endpoint = f"{rest_url}/connectors/{connector_name}/status"
    deadline = time.monotonic() + timeout_seconds
    last_state = "not reported"
    while time.monotonic() < deadline:
        try:
            response = _request_json(endpoint)
        except HTTPError as error:
            if error.code != 404:
                raise RuntimeError(
                    f"Could not read connector status (HTTP {error.code})."
                ) from error
        else:
            if not isinstance(response, dict):
                raise RuntimeError("Kafka Connect returned invalid connector status.")
            connector_state = response.get("connector", {}).get("state")
            task_states = [
                task.get("state") for task in response.get("tasks", [])
            ]
            last_state = f"connector={connector_state}, tasks={task_states}"
            if connector_state == "FAILED" or "FAILED" in task_states:
                raise RuntimeError(f"Debezium connector failed: {last_state}.")
            if connector_state == "RUNNING" and task_states and all(
                state == "RUNNING" for state in task_states
            ):
                return
        time.sleep(2)
    raise RuntimeError(
        f"Debezium connector did not become healthy in time ({last_state})."
    )


def register_connector() -> None:
    rest_url = _required("CONNECT_REST_URL").rstrip("/")
    timeout_seconds = int(_required("CONNECTOR_HEALTH_TIMEOUT_SECONDS"))
    if timeout_seconds <= 0:
        raise ValueError("CONNECTOR_HEALTH_TIMEOUT_SECONDS must be positive.")
    connector_name, _, config = _connector_config()
    _wait_for_connect(rest_url, timeout_seconds)
    existing = _existing_connector_config(rest_url, connector_name)
    if existing is not None:
        check_existing_config(existing, config)
        print(
            f"Connector {connector_name!r} already exists; "
            "its configuration and offsets were left unchanged."
        )
        _wait_for_connector(rest_url, connector_name, timeout_seconds)
        return

    _validate_connector_config(rest_url, connector_name, config)
    endpoint = f"{rest_url}/connectors"
    try:
        _request_json(
            endpoint,
            method="POST",
            payload={"name": connector_name, "config": config},
        )
    except HTTPError as error:
        if error.code == 409:
            existing = _existing_connector_config(rest_url, connector_name)
            if existing is None:
                raise RuntimeError("Concurrent connector creation could not be verified.") from None
            check_existing_config(existing, config)
            print(
                f"Connector {connector_name!r} was created concurrently; "
                "its configuration and offsets were left unchanged."
            )
            _wait_for_connector(rest_url, connector_name, timeout_seconds)
            return
        raise RuntimeError(
            f"Kafka Connect rejected connector configuration (HTTP {error.code})."
        ) from error
    _wait_for_connector(rest_url, connector_name, timeout_seconds)
    print(
        f"Connector {connector_name!r} is running. "
        "Its initial snapshot may still be in progress."
    )


def check_existing_config(existing: dict[str, object], expected: dict[str, str]) -> None:
    """Verify connector lineage without printing or changing stored configuration."""
    keys = (
        "connector.class", "database.hostname", "database.port", "database.names",
        "topic.prefix", "snapshot.mode", "schema.history.internal.kafka.topic",
        "key.converter.schemas.enable", "value.converter.schemas.enable",
    )
    changed = [key for key in keys if key in expected and existing.get(key) != expected[key]]
    if "table.include.list" in expected:
        actual_tables = set(str(existing.get("table.include.list", "")).replace(r"\.", ".").split(","))
        expected_tables = set(expected["table.include.list"].replace(r"\.", ".").split(","))
        if actual_tables != expected_tables:
            changed.append("table.include.list")
    if changed:
        raise RuntimeError("Existing CDC connector lineage differs for: " + ", ".join(changed))
    if (
        existing.get("snapshot.isolation.mode", "repeatable_read")
        != expected.get("snapshot.isolation.mode", "repeatable_read")
    ):
        print("Existing connector uses different snapshot isolation. Its settings are preserved.")
    if "database.password" in expected and existing.get("database.password") != expected["database.password"]:
        print("Existing connector credentials are unchanged. Schedule migration to the environment provider.")


if __name__ == "__main__":
    register_connector()
