#!/usr/bin/env python3
"""Validate project environment settings and local security secrets."""

from __future__ import annotations

import base64
import binascii
import re
import sys
from pathlib import Path

from dotenv import dotenv_values


# --------------------------------------------------
# Project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


# --------------------------------------------------
# Required environment variables
# --------------------------------------------------

REQUIRED_VARIABLES = [
    # Project
    "PROJECT_NAME",
    "TIMEZONE",

    # Source SQL Server
    "SOURCE_DB_HOST",
    "SOURCE_DB_PORT",
    "SOURCE_DB_NAME",
    "SOURCE_DB_SCHEMA",
    "SOURCE_DB_USER",
    "SOURCE_DB_PASSWORD",
    "SOURCE_DB_ODBC_DRIVER",
    "SOURCE_DB_ENCRYPT",
    "SOURCE_DB_TRUST_SERVER_CERTIFICATE",
    "SOURCE_DB_CONNECTION_TIMEOUT",
    "SOURCE_DB_SNAPSHOT_ISOLATION",

    # PostgreSQL administration
    "POSTGRES_BOOTSTRAP_DB",
    "POSTGRES_ADMIN_USER",
    "POSTGRES_ADMIN_PASSWORD",

    # PostgreSQL staging
    "STAGING_DB_NAME",
    "STAGING_DB_USER",
    "STAGING_DB_PASSWORD",

    # Airflow metadata database
    "AIRFLOW_DB_NAME",
    "AIRFLOW_DB_USER",
    "AIRFLOW_DB_PASSWORD",

    # ClickHouse administration
    "CLICKHOUSE_ADMIN_USER",
    "CLICKHOUSE_ADMIN_PASSWORD",
    "CLICKHOUSE_HOST",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_NATIVE_PORT",

    # ClickHouse warehouse
    "WAREHOUSE_DB_NAME",
    "WAREHOUSE_DB_USER",
    "WAREHOUSE_DB_PASSWORD",
    "WAREHOUSE_JDBC_DRIVER",
    "STAGING_JDBC_DRIVER",
    "STUDENT_ID_HMAC_KEY",

    # Grafana
    "GRAFANA_ADMIN_USER",
    "GRAFANA_ADMIN_PASSWORD",
    "GRAFANA_DB_USER",
    "GRAFANA_DB_PASSWORD",

    # Airflow security
    "AIRFLOW_JWT_SECRET",
    "AIRFLOW__CORE__FERNET_KEY",
    "AIRFLOW__API__SECRET_KEY",

    # Airflow administrator
    "AIRFLOW_ADMIN_USER",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_ADMIN_EMAIL",

    # Airflow network
    "POSTGRES_HOST",
    "POSTGRES_CONTAINER_PORT",
    "KAFKA_HOST",
    "KAFKA_INTERNAL_PORT",
    "KAFKA_CONTROLLER_PORT",
    "KAFKA_EXTERNAL_PORT",
    "KAFKA_EXTERNAL_ADVERTISED_HOST",
    "CONNECT_HOST",
    "CONNECT_INTERNAL_PORT",
    "CONNECT_METRICS_PORT",
    "CONNECTOR_HEALTH_TIMEOUT_SECONDS",
    "DEBEZIUM_CONNECTOR_NAME",
    "DEBEZIUM_TOPIC_PREFIX",
    "DEBEZIUM_SCHEMA_HISTORY_TOPIC",
    "DEBEZIUM_SNAPSHOT_MODE",
    "AIRFLOW_API_SERVER_URL",
    "STREAMING_APP_NAME",
    "STREAMING_QUERY_NAME",
    "STREAMING_CHECKPOINT_DIR",
    "STREAMING_MAX_OFFSETS_PER_TRIGGER",
    "STREAMING_KAFKA_GROUP_ID",
    "WAREHOUSE_WRITE_BATCH_SIZE",
    "KAFKA_TOPIC_RETENTION_MS",
    "KAFKA_NUM_PARTITIONS",
    "KAFKA_HEAP_OPTS",
    "CONNECT_HEAP_OPTS",
]


# --------------------------------------------------
# Validation configuration
# --------------------------------------------------

PLACEHOLDER_PREFIXES = (
    "CHANGE_ME",
    "GENERATE_",
)

BOOLEAN_VARIABLES = [
    "SOURCE_DB_ENCRYPT",
    "SOURCE_DB_TRUST_SERVER_CERTIFICATE",
]

PORT_VARIABLES = [
    "SOURCE_DB_PORT",
    "POSTGRES_CONTAINER_PORT",
    "CLICKHOUSE_HTTP_PORT",
    "CLICKHOUSE_NATIVE_PORT",
    "KAFKA_INTERNAL_PORT",
    "KAFKA_CONTROLLER_PORT",
    "KAFKA_EXTERNAL_PORT",
    "CONNECT_INTERNAL_PORT",
    "CONNECT_METRICS_PORT",
]

VALID_BOOLEAN_VALUES = {"yes", "no", "true", "false"}


# --------------------------------------------------
# Environment loading
# --------------------------------------------------

def read_environment_file() -> dict[str, str]:
    """Read environment variables from the repository .env file."""

    values = dotenv_values(ENV_FILE)

    return {
        key: value
        for key, value in values.items()
        if value is not None
    }


# --------------------------------------------------
# Required variable validation
# --------------------------------------------------

def validate_required_variables(
    values: dict[str, str],
) -> list[str]:
    """Check required variables and detect placeholder values."""

    errors: list[str] = []

    for variable in REQUIRED_VARIABLES:

        value = values.get(variable, "").strip()

        if not value:
            errors.append(
                f"Missing required variable: {variable}"
            )
            continue

        if value.startswith(PLACEHOLDER_PREFIXES):
            errors.append(
                f"Placeholder value detected: {variable}"
            )

    return errors


# --------------------------------------------------
# Boolean validation
# --------------------------------------------------

def validate_boolean_variables(
    values: dict[str, str],
) -> list[str]:
    """Validate environment variables expected to contain Boolean values."""

    errors: list[str] = []

    for variable in BOOLEAN_VARIABLES:

        value = values.get(variable, "").lower()

        if value and value not in VALID_BOOLEAN_VALUES:
            errors.append(
                f"Invalid Boolean value configured for {variable}."
            )

    return errors


# --------------------------------------------------
# Port validation
# --------------------------------------------------

def validate_ports(
    values: dict[str, str],
) -> list[str]:
    """Validate configured TCP ports."""

    errors: list[str] = []

    for variable in PORT_VARIABLES:

        value = values.get(variable, "")

        if not value:
            continue

        try:
            port = int(value)

            if not 1 <= port <= 65535:
                raise ValueError

        except ValueError:
            errors.append(
                f"Invalid TCP port configured for {variable}."
            )

    return errors


# --------------------------------------------------
# Connection timeout validation
# --------------------------------------------------

def validate_connection_timeout(
    values: dict[str, str],
) -> list[str]:
    """Validate SQL Server connection timeout."""

    errors: list[str] = []

    variable = "SOURCE_DB_CONNECTION_TIMEOUT"
    value = values.get(variable, "")

    if not value:
        return errors

    try:
        timeout = int(value)

        if timeout <= 0:
            raise ValueError

    except ValueError:
        errors.append(
            f"Invalid positive integer configured for {variable}."
        )

    return errors


def validate_snapshot_isolation(values: dict[str, str]) -> list[str]:
    """Require the isolation mode used for a consistent multi-table extract."""
    value = values.get("SOURCE_DB_SNAPSHOT_ISOLATION", "")
    if value != "SNAPSHOT":
        return ["SOURCE_DB_SNAPSHOT_ISOLATION must be SNAPSHOT."]
    return []


def validate_streaming_limits(values: dict[str, str]) -> list[str]:
    """Validate positive integer settings used to bound stream resources."""
    errors: list[str] = []
    for variable in (
        "CONNECTOR_HEALTH_TIMEOUT_SECONDS",
        "STREAMING_MAX_OFFSETS_PER_TRIGGER",
        "WAREHOUSE_WRITE_BATCH_SIZE",
        "KAFKA_TOPIC_RETENTION_MS",
        "KAFKA_NUM_PARTITIONS",
    ):
        try:
            if int(values.get(variable, "")) <= 0:
                raise ValueError
        except ValueError:
            errors.append(f"{variable} must be a positive integer.")
    if len(values.get("STUDENT_ID_HMAC_KEY", "")) < 32:
        errors.append("STUDENT_ID_HMAC_KEY must contain at least 32 characters.")
    if values.get("DEBEZIUM_SNAPSHOT_MODE") != "initial":
        errors.append("DEBEZIUM_SNAPSHOT_MODE must be initial.")
    return errors


# --------------------------------------------------
# Fernet key validation
# --------------------------------------------------

def validate_fernet_key(
    values: dict[str, str],
) -> list[str]:
    """Validate that the Airflow Fernet key decodes to 32 bytes."""

    errors: list[str] = []

    key = values.get("AIRFLOW__CORE__FERNET_KEY", "")

    if not key:
        return errors

    try:
        decoded = base64.b64decode(
            key,
            altchars=b"-_",
            validate=True,
        )

        if len(decoded) != 32:
            errors.append(
                "Invalid Airflow Fernet key length."
            )

    except (binascii.Error, ValueError):
        errors.append(
            "Invalid Airflow Fernet key format."
        )

    return errors


# --------------------------------------------------
# Airflow API secret validation
# --------------------------------------------------

def validate_api_secret(
    values: dict[str, str],
) -> list[str]:
    """Validate the Airflow API secret key format."""

    errors: list[str] = []

    secret = values.get("AIRFLOW__API__SECRET_KEY", "")

    if secret and not re.fullmatch(
        r"[0-9a-fA-F]{64}",
        secret,
    ):
        errors.append(
            "Invalid Airflow API secret key format."
        )

    return errors


# --------------------------------------------------
# Email validation
# --------------------------------------------------

def validate_email(
    values: dict[str, str],
) -> list[str]:
    """Perform basic email address format validation."""

    errors: list[str] = []

    email = values.get("AIRFLOW_ADMIN_EMAIL", "")

    if email and not re.fullmatch(
        r"[^@\s]+@[^@\s]+\.[^@\s]+",
        email,
    ):
        errors.append(
            "Invalid AIRFLOW_ADMIN_EMAIL format."
        )

    return errors


# --------------------------------------------------
# Main validation
# --------------------------------------------------

def validate_environment() -> list[str]:
    """Run all environment validation checks."""

    if not ENV_FILE.is_file():
        return [".env file does not exist."]

    values = read_environment_file()

    errors = [
        *validate_required_variables(values),
        *validate_boolean_variables(values),
        *validate_ports(values),
        *validate_connection_timeout(values),
        *validate_snapshot_isolation(values),
        *validate_streaming_limits(values),
        *validate_fernet_key(values),
        *validate_api_secret(values),
        *validate_email(values),
    ]

    return errors


# --------------------------------------------------
# Application entry point
# --------------------------------------------------

def main() -> None:
    """Run validation and return a non-zero status on failure."""

    try:
        errors = validate_environment()

    except OSError:
        print(
            "[ERROR] Unable to read the .env file.",
            file=sys.stderr,
        )
        sys.exit(1)

    if errors:

        print("[ERROR] Environment validation failed:")

        for error in errors:
            print(f"  - {error}")

        sys.exit(1)

    print("[INFO] Environment validation passed.")
    print("[INFO] Required variables are configured.")
    print("[INFO] Security settings are valid.")
    print("[INFO] Network configuration is valid.")


if __name__ == "__main__":
    main()