#!/usr/bin/env bash
set -Eeuo pipefail

: "${CLICKHOUSE_ADMIN_USER:?}" "${CLICKHOUSE_ADMIN_PASSWORD:?}" "${CLICKHOUSE_NATIVE_PORT:?}"
: "${WAREHOUSE_DB_NAME:?}" "${CDC_DB_NAME:?}"

for database in "$WAREHOUSE_DB_NAME" "$CDC_DB_NAME"; do
    [[ $database =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
        printf 'Unsupported ClickHouse database identifier.\n' >&2
        exit 1
    }
done
[[ $WAREHOUSE_DB_NAME != "$CDC_DB_NAME" ]] || exit 1

client() {
    clickhouse-client --port "$CLICKHOUSE_NATIVE_PORT" --user "$CLICKHOUSE_ADMIN_USER" \
        --password "$CLICKHOUSE_ADMIN_PASSWORD" --log_queries=0 --multiquery "$@"
}

# Keep the two publishers' DDL isolated even though they share one server.
client <<SQL
CREATE DATABASE IF NOT EXISTS \`$WAREHOUSE_DB_NAME\`;
CREATE DATABASE IF NOT EXISTS \`$CDC_DB_NAME\`;
SQL
client --database "$WAREHOUSE_DB_NAME" < /warehouse_ddl/warehouse_ddl.sql
client --database "$CDC_DB_NAME" < /warehouse_ddl/cdc_warehouse_ddl.sql
