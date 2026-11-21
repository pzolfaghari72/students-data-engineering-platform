#!/usr/bin/env bash

# Stop the script if an error occurs, an undefined variable is used,
# or a command in a pipeline fails.
set -Eeuo pipefail


# --------------------------------------------------
# Project paths
# --------------------------------------------------

# Get the project root directory.
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Move to the project root directory.
cd "$PROJECT_ROOT"


# --------------------------------------------------
# Required project directories
# --------------------------------------------------

# Define the directory structure for the data engineering platform.
DIRECTORIES=(

    # Apache Airflow
    "airflow/dags"
    "airflow/logs"
    "airflow/plugins"

    # Debezium connector registration
    "debezium"

    # ClickHouse
    "clickhouse/config"

    # Grafana dashboards and provisioning
    "grafana/dashboards"
    "grafana/provisioning/dashboards"
    "grafana/provisioning/datasources"

    # Database documentation and SQL scripts
    "db/source_assessment"
    "db/staging_postgres"
    "db/warehouse_clickhouse"

    # Batch analytics application
    "spark"

    # Project tests
    "tests/unit"
)


# --------------------------------------------------
# Create project directories
# --------------------------------------------------

printf '[INFO] Preparing project directories...\n\n'

CREATED_COUNT=0
EXISTING_COUNT=0

for directory in "${DIRECTORIES[@]}"; do

    if [[ -d "$directory" ]]; then

        printf '[INFO] Already exists: %s\n' "$directory"
        ((EXISTING_COUNT += 1))

    else

        # Create the directory and any missing parent directories.
        mkdir -p "$directory"

        printf '[INFO] Created: %s\n' "$directory"
        ((CREATED_COUNT += 1))

    fi

done


# --------------------------------------------------
# Final summary
# --------------------------------------------------

printf '\n'
printf '[INFO] Project directory structure is ready.\n'
printf '[INFO] Created directories: %s\n' "$CREATED_COUNT"
printf '[INFO] Existing directories: %s\n' "$EXISTING_COUNT"
printf '[INFO] Total directories checked: %s\n' "${#DIRECTORIES[@]}"
