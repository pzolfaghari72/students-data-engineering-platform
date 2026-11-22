#!/usr/bin/env python3
"""Generate missing local secrets and write them to the protected .env file."""

from __future__ import annotations

import base64
import os
import secrets
import tempfile
from collections.abc import Callable
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


# --------------------------------------------------
# Secret generators
# --------------------------------------------------

def generate_password() -> str:
    """Generate a cryptographically secure URL-safe password."""
    return secrets.token_urlsafe(32)


def generate_fernet_key() -> str:
    """Generate a Fernet-compatible key containing 32 random bytes."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def generate_api_secret() -> str:
    """Generate a 256-bit hexadecimal secret."""
    return secrets.token_hex(32)


# --------------------------------------------------
# Secret configuration
# --------------------------------------------------

SECRET_GENERATORS: dict[str, Callable[[], str]] = {
    # PostgreSQL
    "POSTGRES_ADMIN_PASSWORD": generate_password,
    "STAGING_DB_PASSWORD": generate_password,
    "AIRFLOW_DB_PASSWORD": generate_password,

    # ClickHouse
    "CLICKHOUSE_ADMIN_PASSWORD": generate_password,
    "WAREHOUSE_DB_PASSWORD": generate_password,
    "CDC_DB_PASSWORD": generate_password,
    "STUDENT_ID_HMAC_KEY": generate_api_secret,

    # Grafana
    "GRAFANA_ADMIN_PASSWORD": generate_password,
    "GRAFANA_DB_PASSWORD": generate_password,

    # Airflow
    "AIRFLOW_ADMIN_PASSWORD": generate_password,
    "AIRFLOW_JWT_SECRET": generate_api_secret,
    "AIRFLOW__CORE__FERNET_KEY": generate_fernet_key,
    "AIRFLOW__API__SECRET_KEY": generate_api_secret,
}


# --------------------------------------------------
# Validation helpers
# --------------------------------------------------

def is_placeholder(value: str) -> bool:
    """Check whether a value requires secret generation."""

    value = value.strip()

    return (
        not value
        or value.startswith(("CHANGE_ME", "GENERATE_"))
    )


# --------------------------------------------------
# Environment file update
# --------------------------------------------------

def update_environment_file() -> int:
    """Generate missing secrets and atomically update the .env file."""

    if not ENV_FILE.is_file():
        raise FileNotFoundError(
            ".env file does not exist."
        )

    lines = ENV_FILE.read_text(
        encoding="utf-8"
    ).splitlines()

    updated_keys: set[str] = set()
    updated_lines: list[str] = []
    generated_values: dict[str, str] = {}
    encountered_keys: set[str] = set()

    for line in lines:

        stripped = line.strip()

        if (
            not stripped
            or stripped.startswith("#")
            or "=" not in line
        ):
            updated_lines.append(line)
            continue

        key, value = line.split("=", maxsplit=1)
        key = key.strip()

        if key not in SECRET_GENERATORS:
            updated_lines.append(line)
            continue

        if key in encountered_keys:
            raise ValueError(
                f"Duplicate secret variable detected: {key}"
            )

        encountered_keys.add(key)

        if is_placeholder(value):

            if key not in generated_values:
                generated_values[key] = SECRET_GENERATORS[key]()

            updated_lines.append(
                f"{key}={generated_values[key]}"
            )

            updated_keys.add(key)

        else:
            updated_lines.append(line)

    # An existing installation can predate the dedicated CDC writer.
    if "CDC_DB_PASSWORD" not in encountered_keys:
        updated_lines.append(f"CDC_DB_PASSWORD={generate_password()}")
        updated_keys.add("CDC_DB_PASSWORD")

    if updated_keys:

        content = "\n".join(updated_lines) + "\n"

        fd, temporary_path = tempfile.mkstemp(
            prefix=".env.",
            dir=PROJECT_ROOT,
            text=True,
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as file:
                file.write(content)

            os.chmod(temporary_path, 0o600)

            os.replace(
                temporary_path,
                ENV_FILE,
            )

        finally:

            if os.path.exists(temporary_path):
                os.unlink(temporary_path)

    else:
        os.chmod(ENV_FILE, 0o600)

    return len(updated_keys)


# --------------------------------------------------
# Application entry point
# --------------------------------------------------

def main() -> None:
    """Generate missing secrets in the local environment file."""

    updated_count = update_environment_file()

    print(
        "[INFO] Secret generation completed. "
        f"{updated_count} value(s) generated."
    )


if __name__ == "__main__":
    main()