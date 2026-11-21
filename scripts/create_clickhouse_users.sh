#!/usr/bin/env bash

set -Eeuo pipefail

# --------------------------------------------------
# Environment validation
# --------------------------------------------------

: "${CLICKHOUSE_ADMIN_USER:?CLICKHOUSE_ADMIN_USER is required}"
: "${CLICKHOUSE_ADMIN_PASSWORD:?CLICKHOUSE_ADMIN_PASSWORD is required}"
: "${CLICKHOUSE_NATIVE_PORT:?CLICKHOUSE_NATIVE_PORT is required}"

: "${WAREHOUSE_DB_NAME:?WAREHOUSE_DB_NAME is required}"

: "${WAREHOUSE_DB_USER:?WAREHOUSE_DB_USER is required}"
: "${WAREHOUSE_DB_PASSWORD:?WAREHOUSE_DB_PASSWORD is required}"

: "${GRAFANA_DB_USER:?GRAFANA_DB_USER is required}"
: "${GRAFANA_DB_PASSWORD:?GRAFANA_DB_PASSWORD is required}"


# --------------------------------------------------
# SQL escaping
# --------------------------------------------------

escape_identifier() {
    local value="${1//\`/\`\`}"
    printf '`%s`' "$value"
}

escape_string() {
    local value="${1//\'/\'\'}"
    printf "'%s'" "$value"
}


# --------------------------------------------------
# SQL identifiers and literals
# --------------------------------------------------

database_identifier="$(escape_identifier "$WAREHOUSE_DB_NAME")"

warehouse_user_identifier="$(escape_identifier "$WAREHOUSE_DB_USER")"
warehouse_password_literal="$(escape_string "$WAREHOUSE_DB_PASSWORD")"

grafana_user_identifier="$(escape_identifier "$GRAFANA_DB_USER")"
grafana_password_literal="$(escape_string "$GRAFANA_DB_PASSWORD")"


# --------------------------------------------------
# ClickHouse initialization
# --------------------------------------------------

clickhouse-client \
    --port "$CLICKHOUSE_NATIVE_PORT" \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery \
    --query "

        -- Create warehouse database
        CREATE DATABASE IF NOT EXISTS ${database_identifier};

        -- Create ETL role
        CREATE ROLE IF NOT EXISTS warehouse_etl_role IN local_directory;

        -- Create Grafana read-only role
        CREATE ROLE IF NOT EXISTS warehouse_readonly_role IN local_directory;

        -- Create warehouse ETL user
        CREATE USER IF NOT EXISTS ${warehouse_user_identifier}
            IN local_directory
            IDENTIFIED WITH plaintext_password BY ${warehouse_password_literal};

        ALTER USER ${warehouse_user_identifier}
            IDENTIFIED WITH plaintext_password BY ${warehouse_password_literal};

        -- Create Grafana user
        CREATE USER IF NOT EXISTS ${grafana_user_identifier}
            IN local_directory
            IDENTIFIED WITH plaintext_password BY ${grafana_password_literal};

        ALTER USER ${grafana_user_identifier}
            IDENTIFIED WITH plaintext_password BY ${grafana_password_literal};

        -- Grant warehouse access to ETL role
        GRANT ALL ON ${database_identifier}.* TO warehouse_etl_role;

        -- Grant read-only access to Grafana role
        GRANT SELECT ON ${database_identifier}.* TO warehouse_readonly_role;

        -- Assign roles to users
        GRANT warehouse_etl_role TO ${warehouse_user_identifier};
        GRANT warehouse_readonly_role TO ${grafana_user_identifier};

        -- Set default roles
        SET DEFAULT ROLE warehouse_etl_role TO ${warehouse_user_identifier};
        SET DEFAULT ROLE warehouse_readonly_role TO ${grafana_user_identifier};

    "

printf '\n[INFO] ClickHouse warehouse initialization completed successfully.\n'
printf '[INFO] Warehouse ETL user: %s\n' "$WAREHOUSE_DB_USER"
printf '[INFO] Grafana read-only user: %s\n' "$GRAFANA_DB_USER"
printf '[INFO] Warehouse database: %s\n' "$WAREHOUSE_DB_NAME"