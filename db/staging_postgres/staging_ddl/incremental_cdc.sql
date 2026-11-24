BEGIN;

CREATE SCHEMA IF NOT EXISTS incremental_cdc;

CREATE TABLE IF NOT EXISTS incremental_cdc.relay_batches (
    generation_id text NOT NULL,
    batch_id bigint NOT NULL CHECK (batch_id >= 0),
    source_checkpoint text NOT NULL,
    source_digest text NOT NULL,
    transport_records bigint NOT NULL CHECK (transport_records >= 0),
    relayed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (generation_id, batch_id)
);

CREATE TABLE IF NOT EXISTS incremental_cdc.history (
    generation_id text NOT NULL,
    first_batch bigint NOT NULL,
    source_table text NOT NULL,
    source_key text NOT NULL,
    source_version text NOT NULL,
    commit_lsn text NOT NULL,
    source_time_ms bigint NOT NULL,
    operation text NOT NULL CHECK (operation IN ('r', 'c', 'u', 'd')),
    image jsonb NOT NULL,
    payload_hash text NOT NULL,
    PRIMARY KEY (generation_id, source_table, source_key, source_version),
    FOREIGN KEY (generation_id, first_batch)
        REFERENCES incremental_cdc.relay_batches (generation_id, batch_id)
);

CREATE INDEX IF NOT EXISTS incremental_history_batch
    ON incremental_cdc.history (generation_id, first_batch, source_table, source_key, source_version);

CREATE TABLE IF NOT EXISTS incremental_cdc.runs (
    load_id text PRIMARY KEY,
    generation_id text NOT NULL,
    source_checkpoint text NOT NULL,
    after_batch bigint NOT NULL,
    through_batch bigint NOT NULL CHECK (through_batch >= after_batch),
    status text NOT NULL CHECK (status IN ('RUNNING', 'PUBLISHED', 'FAILED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    normalized_rows bigint,
    error_message text
);

COMMIT;
