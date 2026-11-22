#!/usr/bin/env python3
"""Validate local settings without emitting their values."""

import argparse
import os
import re
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
CORE = """
POSTGRES_HOST POSTGRES_CONTAINER_PORT POSTGRES_BOOTSTRAP_DB POSTGRES_ADMIN_USER
POSTGRES_ADMIN_PASSWORD STAGING_DB_NAME STAGING_DB_USER STAGING_DB_PASSWORD
AIRFLOW_DB_NAME AIRFLOW_DB_USER AIRFLOW_DB_PASSWORD CLICKHOUSE_HOST CLICKHOUSE_HTTP_PORT
CLICKHOUSE_NATIVE_PORT CLICKHOUSE_ADMIN_USER CLICKHOUSE_ADMIN_PASSWORD WAREHOUSE_DB_NAME
WAREHOUSE_DB_USER WAREHOUSE_DB_PASSWORD GRAFANA_DB_USER GRAFANA_DB_PASSWORD
GRAFANA_ADMIN_USER GRAFANA_ADMIN_PASSWORD
""".split()
SOURCE = """
SOURCE_DB_HOST SOURCE_DB_PORT SOURCE_DB_NAME SOURCE_DB_SCHEMA
SOURCE_DB_ENCRYPT SOURCE_DB_TRUST_SERVER_CERTIFICATE
""".split()
BATCH = """
SOURCE_DB_USER SOURCE_DB_PASSWORD SOURCE_DB_ODBC_DRIVER SOURCE_DB_CONNECTION_TIMEOUT
SOURCE_DB_SNAPSHOT_ISOLATION STUDENT_ID_HMAC_KEY STAGING_JDBC_DRIVER WAREHOUSE_JDBC_DRIVER
AIRFLOW__CORE__FERNET_KEY AIRFLOW_JWT_SECRET AIRFLOW__API__SECRET_KEY AIRFLOW_ADMIN_USER
AIRFLOW_ADMIN_PASSWORD AIRFLOW_ADMIN_EMAIL AIRFLOW_API_SERVER_URL
""".split()
CDC_DEFAULTS = {
    "CDC_DB_NAME": "students_cdc",
    "CDC_DB_USER": "students_cdc_writer",
    "CDC_CHECKPOINT_DIR": "/opt/spark/checkpoints/students-cdc-warehouse",
    "STREAMING_GENERATION_ID": "students-cdc-v1",
    "STREAMING_TRIGGER_INTERVAL": "5 seconds",
    "STREAMING_BOOTSTRAP_TIMEOUT_SECONDS": "3600",
    "STREAMING_MAX_OFFSETS_PER_TRIGGER": "10000",
    "KAFKA_NUM_PARTITIONS": "1",
    "KAFKA_TOPIC_RETENTION_MS": "604800000",
    "CONNECTOR_HEALTH_TIMEOUT_SECONDS": "120",
    "DEBEZIUM_SNAPSHOT_MODE": "initial",
    "DEBEZIUM_SNAPSHOT_ISOLATION_MODE": "snapshot",
}
PLACEHOLDER = ("CHANGE_ME", "GENERATE_")


def validate_environment(values, mode):
    checks = CORE.copy()
    if mode in ("batch", "all"):
        checks += SOURCE + BATCH
    if mode in ("cdc", "all"):
        checks += SOURCE + ["CDC_DB_PASSWORD"]
        for field in ("USER", "PASSWORD"):
            override = f"SOURCE_CDC_{field}"
            checks.append(override if values.get(override) else f"SOURCE_DB_{field}")

    errors = []
    for name in sorted(set(checks)):
        value = values.get(name, "").strip()
        if not value or value.startswith(PLACEHOLDER):
            errors.append(f"Set a value for {name}.")

    for name, value in values.items():
        if name.endswith("_PORT") and value and (not value.isdecimal() or not 1 <= int(value) <= 65535):
            errors.append(f"{name} must be a valid TCP port.")
    for name in ("SOURCE_DB_ENCRYPT", "SOURCE_DB_TRUST_SERVER_CERTIFICATE"):
        if values.get(name) and values[name].lower() not in {"yes", "no", "true", "false"}:
            errors.append(f"{name} must be a boolean.")
    if mode in ("batch", "all") and values.get("SOURCE_DB_SNAPSHOT_ISOLATION") != "SNAPSHOT":
        errors.append("SOURCE_DB_SNAPSHOT_ISOLATION must be SNAPSHOT.")
    if mode in ("batch", "all"):
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}=", values.get("AIRFLOW__CORE__FERNET_KEY", "")):
            errors.append("AIRFLOW__CORE__FERNET_KEY must encode a 32-byte Fernet key.")
        for name in ("AIRFLOW_JWT_SECRET", "AIRFLOW__API__SECRET_KEY"):
            if len(values.get(name, "")) < 32:
                errors.append(f"{name} must contain at least 32 characters.")
    if mode in ("cdc", "all"):
        checkpoint = Path(values["CDC_CHECKPOINT_DIR"])
        root = Path("/opt/spark/checkpoints")
        if root not in checkpoint.parents or ".." in checkpoint.parts:
            errors.append("CDC_CHECKPOINT_DIR must be under /opt/spark/checkpoints.")
        if checkpoint == Path(values.get("STREAMING_CHECKPOINT_DIR", "/opt/spark/checkpoints/students-cdc")):
            errors.append("CDC_CHECKPOINT_DIR must be different from the legacy stream checkpoint.")
        if values["DEBEZIUM_SNAPSHOT_MODE"] != "initial":
            errors.append("DEBEZIUM_SNAPSHOT_MODE must be initial.")
        if values["DEBEZIUM_SNAPSHOT_ISOLATION_MODE"] not in {"snapshot", "repeatable_read"}:
            errors.append("Set DEBEZIUM_SNAPSHOT_ISOLATION_MODE to snapshot or repeatable_read.")
        if not values["STREAMING_GENERATION_ID"].strip():
            errors.append("Set a nonempty STREAMING_GENERATION_ID.")
        if not re.fullmatch(r"[1-9]\d* (?:milliseconds?|seconds?|minutes?)", values["STREAMING_TRIGGER_INTERVAL"]):
            errors.append("STREAMING_TRIGGER_INTERVAL must be a positive Spark processing interval.")

        for name in ("STREAMING_BOOTSTRAP_TIMEOUT_SECONDS", "STREAMING_MAX_OFFSETS_PER_TRIGGER",
                     "KAFKA_NUM_PARTITIONS", "KAFKA_TOPIC_RETENTION_MS", "CONNECTOR_HEALTH_TIMEOUT_SECONDS"):
            value = values[name]
            if not value.isdecimal() or int(value) < 1:
                errors.append(f"{name} must be a positive integer.")
    if values.get("SOURCE_DB_CONNECTION_TIMEOUT"):
        value = values["SOURCE_DB_CONNECTION_TIMEOUT"]
        if not value.isdecimal() or int(value) < 1:
            errors.append("SOURCE_DB_CONNECTION_TIMEOUT must be a positive integer.")
    for name in ("SPARK_BATCH_READ_PARTITIONS", "SPARK_BATCH_SHUFFLE_PARTITIONS",
                 "SPARK_BATCH_FETCH_SIZE", "SPARK_BATCH_WRITE_SIZE",
                 "SPARK_STREAM_SHUFFLE_PARTITIONS", "CLICKHOUSE_DASHBOARD_GROUP_BY_BYTES"):
        value = values.get(name)
        if value is not None and (not value.isdecimal() or int(value) < 1):
            errors.append(f"{name} must be a positive integer.")

    for group in (("WAREHOUSE_DB_NAME", "CDC_DB_NAME"),
                  ("WAREHOUSE_DB_USER", "CDC_DB_USER", "GRAFANA_DB_USER")):
        identifiers = [values.get(name, "") for name in group]
        if len(set(identifiers)) != len(identifiers) or any(
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) for name in identifiers
        ):
            errors.append(f"Use distinct ClickHouse identifiers for {', '.join(group)}.")
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("core", "batch", "cdc", "all"), default="all")
    args = parser.parse_args()
    env_file = ROOT / ".env"
    if not env_file.is_file():
        parser.error(".env is missing. Copy .env.example or run make init.")
    values = {**CDC_DEFAULTS, **{k: v for k, v in dotenv_values(env_file).items() if v is not None}}
    values.update(os.environ)
    errors = validate_environment(values, args.mode)
    if errors:
        for error in errors:
            print(error)
        return 1
    print(f"Environment validation passed ({args.mode}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
