BEGIN;

CREATE SCHEMA IF NOT EXISTS history;

-- Preserve the earlier SQL Server CDC-shaped table for existing deployments.
-- New Debezium events use Kafka topic/partition/offset as their stable identity.
DO $$
BEGIN
    IF to_regclass('history.cdc_events') IS NOT NULL
       AND to_regclass('history.sqlserver_cdc_events_legacy') IS NULL
       AND EXISTS (
           SELECT 1
           FROM information_schema.columns
           WHERE table_schema = 'history'
             AND table_name = 'cdc_events'
             AND column_name = 'start_lsn'
       )
    THEN
        ALTER TABLE history.cdc_events
            RENAME TO sqlserver_cdc_events_legacy;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS history.cdc_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    source_schema VARCHAR(128) NOT NULL,
    source_table VARCHAR(128) NOT NULL,
    connector_name VARCHAR(128) NOT NULL,
    source_lsn TEXT,
    source_commit_lsn TEXT,
    source_change_lsn TEXT,
    source_event_serial_no TEXT,
    source_snapshot_state TEXT,
    operation CHAR(1) NOT NULL,
    source_key JSONB NOT NULL,
    before_image JSONB,
    after_image JSONB,
    source_timestamp TIMESTAMPTZ,

    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    batch_id BIGINT NOT NULL
        REFERENCES metadata.extraction_batches(batch_id),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT ck_cdc_events_operation
        CHECK (operation IN ('c', 'r', 'u', 'd', 't')),
    CONSTRAINT ck_cdc_events_kafka_position
        CHECK (kafka_partition >= 0 AND kafka_offset >= 0),
    CONSTRAINT uq_cdc_events_kafka_position
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

ALTER TABLE history.cdc_events
    ADD COLUMN IF NOT EXISTS source_commit_lsn TEXT,
    ADD COLUMN IF NOT EXISTS source_change_lsn TEXT,
    ADD COLUMN IF NOT EXISTS source_event_serial_no TEXT,
    ADD COLUMN IF NOT EXISTS source_snapshot_state TEXT;

CREATE INDEX IF NOT EXISTS ix_debezium_cdc_events_source_key
    ON history.cdc_events (source_schema, source_table, source_key);

CREATE INDEX IF NOT EXISTS ix_debezium_cdc_events_batch_id
    ON history.cdc_events (batch_id);

CREATE INDEX IF NOT EXISTS ix_debezium_cdc_events_captured_at
    ON history.cdc_events (captured_at DESC);

CREATE TABLE IF NOT EXISTS metadata.streaming_generations (
    query_name TEXT PRIMARY KEY,
    generation_id UUID NOT NULL UNIQUE,
    connector_name VARCHAR(128) NOT NULL,
    expected_tables JSONB NOT NULL,
    completed_snapshot_tables JSONB NOT NULL DEFAULT '[]'::jsonb,
    snapshot_end_offsets JSONB NOT NULL DEFAULT '{}'::jsonb,
    consumed_offsets JSONB NOT NULL DEFAULT '{}'::jsonb,
    snapshot_boundary_captured BOOLEAN NOT NULL DEFAULT FALSE,
    snapshot_events_complete BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(32) NOT NULL DEFAULT 'BOOTSTRAPPING',
    last_event_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_streaming_generation_status
        CHECK (status IN ('BOOTSTRAPPING', 'ACTIVE', 'FAILED'))
);

ALTER TABLE metadata.streaming_generations
    ADD COLUMN IF NOT EXISTS snapshot_end_offsets JSONB NOT NULL
        DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS consumed_offsets JSONB NOT NULL
        DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS snapshot_boundary_captured BOOLEAN NOT NULL
        DEFAULT FALSE;

COMMIT;
