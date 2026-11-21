BEGIN;

CREATE SCHEMA IF NOT EXISTS scd;

-- Generic typed-source-independent SCD2 store; source rows remain in JSONB
-- so SQL Server logical types do not need lossy conversions in the CDC path.
CREATE TABLE IF NOT EXISTS scd.entity_versions (
    version_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_schema VARCHAR(128) NOT NULL,
    source_table VARCHAR(128) NOT NULL,
    source_key JSONB NOT NULL,
    attributes JSONB NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    source_lsn TEXT,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT ck_entity_versions_validity
        CHECK (valid_to IS NULL OR valid_to >= valid_from),
    CONSTRAINT ck_entity_versions_current
        CHECK (
            (is_current AND valid_to IS NULL)
            OR (NOT is_current AND valid_to IS NOT NULL)
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_versions_current
    ON scd.entity_versions (source_schema, source_table, source_key)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS ix_entity_versions_lookup
    ON scd.entity_versions
       (source_schema, source_table, source_key, valid_from DESC);

CREATE INDEX IF NOT EXISTS ix_entity_versions_current_table
    ON scd.entity_versions (source_schema, source_table, source_key)
    WHERE is_current;

COMMIT;
