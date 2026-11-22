#!/usr/bin/env bash
set -Eeuo pipefail

: "${WAREHOUSE_DB_NAME:?}" "${WAREHOUSE_DB_USER:?}" "${WAREHOUSE_DB_PASSWORD:?}"
: "${CDC_DB_NAME:?}" "${CDC_DB_USER:?}"
: "${GRAFANA_DB_USER:?}" "${GRAFANA_DB_PASSWORD:?}"
group_by_limit=${CLICKHOUSE_DASHBOARD_GROUP_BY_BYTES:-134217728}
[[ $group_by_limit =~ ^[1-9][0-9]*$ ]] || { printf 'Invalid dashboard group-by limit.\n' >&2; exit 1; }

for identifier in "$WAREHOUSE_DB_NAME" "$CDC_DB_NAME" "$WAREHOUSE_DB_USER" "$CDC_DB_USER" "$GRAFANA_DB_USER"; do
    [[ $identifier =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || {
        printf 'Unsupported ClickHouse database or user identifier.\n' >&2
        exit 1
    }
done
[[ $WAREHOUSE_DB_NAME != "$CDC_DB_NAME" ]] || exit 1
[[ $WAREHOUSE_DB_USER != "$CDC_DB_USER" && $WAREHOUSE_DB_USER != "$GRAFANA_DB_USER" &&
   $CDC_DB_USER != "$GRAFANA_DB_USER" ]] || exit 1

client() {
    clickhouse-client --port "${CLICKHOUSE_NATIVE_PORT:?}" --user "${CLICKHOUSE_ADMIN_USER:?}" \
        --password "${CLICKHOUSE_ADMIN_PASSWORD:?}" --log_queries=0 --multiquery "$@"
}

sql_literal() {
    local value=${1//\\/\\\\}
    printf "'%s'" "${value//\'/\\\'}"
}

# Validate the dedicated database before applying the incompatible CDC DDL.
client <<SQL
CREATE DATABASE IF NOT EXISTS \`$WAREHOUSE_DB_NAME\`;
CREATE DATABASE IF NOT EXISTS \`$CDC_DB_NAME\`;
SQL
client --database "$WAREHOUSE_DB_NAME" < /warehouse_ddl/warehouse_ddl.sql
client --database "$CDC_DB_NAME" < /warehouse_ddl/cdc_warehouse_ddl.sql

# SQL-authenticated users and administrator-owned serving views keep raw CDC
# images inaccessible to Grafana even on an upgraded warehouse.
{
    printf 'CREATE ROLE IF NOT EXISTS warehouse_etl_role IN local_directory;\n'
    printf 'CREATE ROLE IF NOT EXISTS warehouse_readonly_role IN local_directory;\n'
    printf 'CREATE ROLE IF NOT EXISTS cdc_writer_role IN local_directory;\n'
    printf 'ALTER ROLE cdc_writer_role SETTINGS max_bytes_before_external_group_by = %s, max_threads = 2;\n' "$group_by_limit"
    printf 'REVOKE ALL ON `%s`.* FROM warehouse_etl_role;\n' "$WAREHOUSE_DB_NAME"
    printf 'REVOKE SELECT ON `%s`.* FROM warehouse_readonly_role;\n' "$WAREHOUSE_DB_NAME"

    printf 'CREATE USER IF NOT EXISTS `%s` IN local_directory IDENTIFIED WITH sha256_password BY %s;\n' \
        "$WAREHOUSE_DB_USER" "$(sql_literal "$WAREHOUSE_DB_PASSWORD")"
    printf 'ALTER USER `%s` IDENTIFIED WITH sha256_password BY %s;\n' \
        "$WAREHOUSE_DB_USER" "$(sql_literal "$WAREHOUSE_DB_PASSWORD")"
    printf 'CREATE USER IF NOT EXISTS `%s` IN local_directory IDENTIFIED WITH sha256_password BY %s;\n' \
        "$GRAFANA_DB_USER" "$(sql_literal "$GRAFANA_DB_PASSWORD")"
    printf 'ALTER USER `%s` IDENTIFIED WITH sha256_password BY %s;\n' \
        "$GRAFANA_DB_USER" "$(sql_literal "$GRAFANA_DB_PASSWORD")"
    printf 'GRANT warehouse_etl_role TO `%s`; SET DEFAULT ROLE warehouse_etl_role TO `%s`;\n' \
        "$WAREHOUSE_DB_USER" "$WAREHOUSE_DB_USER"
    printf 'GRANT warehouse_readonly_role TO `%s`; SET DEFAULT ROLE warehouse_readonly_role TO `%s`;\n' \
        "$GRAFANA_DB_USER" "$GRAFANA_DB_USER"

    for table in warehouse_model_loads dim_{applicant,class_standing,course,date,degree,department,grade,student,term}_snapshots \
        fact_{application,course_offering,enrollment}_snapshots; do
        printf 'GRANT SELECT, INSERT ON `%s`.`%s` TO warehouse_etl_role;\n' "$WAREHOUSE_DB_NAME" "$table"
    done
    for view in batch_publication_status dim_{applicant,class_standing,course,date,degree,department,grade,student,term} \
        fact_{application,course_offering,enrollment}; do
        printf 'GRANT SELECT ON `%s`.`%s` TO warehouse_readonly_role;\n' "$WAREHOUSE_DB_NAME" "$view"
    done

    printf 'GRANT SELECT ON `%s`.cdc_business_kpis TO warehouse_readonly_role;\n' "$CDC_DB_NAME"
    printf 'GRANT SELECT ON `%s`.cdc_pipeline_health TO warehouse_readonly_role;\n' "$CDC_DB_NAME"
    printf 'ALTER ROLE warehouse_readonly_role SETTINGS max_bytes_before_external_group_by = %s, max_threads = 2;\n' "$group_by_limit"
    if [[ -n ${CDC_DB_PASSWORD:-} ]]; then
        printf 'CREATE USER IF NOT EXISTS `%s` IN local_directory IDENTIFIED WITH sha256_password BY %s;\n' \
            "$CDC_DB_USER" "$(sql_literal "$CDC_DB_PASSWORD")"
        printf 'ALTER USER `%s` IDENTIFIED WITH sha256_password BY %s;\n' \
            "$CDC_DB_USER" "$(sql_literal "$CDC_DB_PASSWORD")"
        printf 'GRANT SELECT, INSERT ON `%s`.* TO cdc_writer_role;\n' "$CDC_DB_NAME"
        printf 'GRANT cdc_writer_role TO `%s`; SET DEFAULT ROLE cdc_writer_role TO `%s`;\n' \
            "$CDC_DB_USER" "$CDC_DB_USER"
    fi
} | client
