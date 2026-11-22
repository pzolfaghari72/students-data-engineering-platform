-- ============================================================
-- STUDENTS DATA ENGINEERING PLATFORM
-- PostgreSQL Staging - Metadata Layer
-- ============================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS metadata;

-- ============================================================
-- 1. PIPELINE RUNS
-- Tracks each pipeline execution.
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.pipeline_runs
(
    run_id              BIGINT GENERATED ALWAYS AS IDENTITY,
    pipeline_name       VARCHAR(150) NOT NULL,
    run_type            VARCHAR(30) NOT NULL,
    status              VARCHAR(30) NOT NULL,

    started_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMPTZ,

    error_message       TEXT,
    execution_context   JSONB,

    CONSTRAINT pk_pipeline_runs
        PRIMARY KEY (run_id),

    CONSTRAINT ck_pipeline_runs_type
        CHECK (run_type IN ('INITIAL', 'INCREMENTAL', 'FULL')),

    CONSTRAINT ck_pipeline_runs_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),

    CONSTRAINT ck_pipeline_runs_completion
        CHECK (
            (status = 'RUNNING' AND completed_at IS NULL)
            OR
            (status <> 'RUNNING' AND completed_at IS NOT NULL)
        ),

    CONSTRAINT ck_pipeline_runs_time
        CHECK (
            completed_at IS NULL
            OR completed_at >= started_at
        )
);


-- ============================================================
-- 2. EXTRACTION BATCHES
-- Tracks extraction batches for individual source tables.
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.extraction_batches
(
    batch_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    run_id              BIGINT NOT NULL,

    source_schema       VARCHAR(128) NOT NULL,
    source_table        VARCHAR(128) NOT NULL,
    capture_instance    VARCHAR(128),

    extraction_type     VARCHAR(30) NOT NULL,
    status              VARCHAR(30) NOT NULL,

    from_lsn            BYTEA,
    to_lsn              BYTEA,

    started_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMPTZ,

    extracted_rows      BIGINT NOT NULL DEFAULT 0,
    inserted_rows       BIGINT NOT NULL DEFAULT 0,
    updated_rows        BIGINT NOT NULL DEFAULT 0,
    deleted_rows        BIGINT NOT NULL DEFAULT 0,

    error_message       TEXT,

    CONSTRAINT pk_extraction_batches
        PRIMARY KEY (batch_id),

    CONSTRAINT fk_extraction_batches_run
        FOREIGN KEY (run_id)
        REFERENCES metadata.pipeline_runs(run_id),

    CONSTRAINT ck_extraction_batches_type
        CHECK (extraction_type IN ('SNAPSHOT', 'CDC')),

    CONSTRAINT ck_extraction_batches_status
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),

    CONSTRAINT ck_extraction_batches_rows
        CHECK (
            extracted_rows >= 0
            AND inserted_rows >= 0
            AND updated_rows >= 0
            AND deleted_rows >= 0
        ),

    CONSTRAINT ck_extraction_batches_lsn_length
        CHECK (
            (from_lsn IS NULL OR OCTET_LENGTH(from_lsn) = 10)
            AND
            (to_lsn IS NULL OR OCTET_LENGTH(to_lsn) = 10)
        ),

    CONSTRAINT ck_extraction_batches_lsn_range
        CHECK (
            from_lsn IS NULL
            OR to_lsn IS NULL
            OR from_lsn <= to_lsn
        ),

    CONSTRAINT ck_extraction_batches_completion
        CHECK (
            (status = 'RUNNING' AND completed_at IS NULL)
            OR
            (status <> 'RUNNING' AND completed_at IS NOT NULL)
        ),

    CONSTRAINT ck_extraction_batches_time
        CHECK (
            completed_at IS NULL
            OR completed_at >= started_at
        ),

    CONSTRAINT ck_extraction_batches_capture
        CHECK (
            extraction_type <> 'CDC'
            OR capture_instance IS NOT NULL
        )
);


CREATE INDEX IF NOT EXISTS ix_extraction_batches_run_id
    ON metadata.extraction_batches (run_id);

CREATE INDEX IF NOT EXISTS ix_extraction_batches_source
    ON metadata.extraction_batches
       (source_schema, source_table, started_at DESC);


-- ============================================================
-- 3. TABLE LOAD STATUS
-- Stores the latest loading status of each source table.
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.table_load_status
(
    source_schema       VARCHAR(128) NOT NULL,
    source_table        VARCHAR(128) NOT NULL,

    last_batch_id       BIGINT,
    last_success_at     TIMESTAMPTZ,
    last_status         VARCHAR(30),

    total_rows_loaded   BIGINT NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_table_load_status
        PRIMARY KEY (source_schema, source_table),

    CONSTRAINT fk_table_load_status_batch
        FOREIGN KEY (last_batch_id)
        REFERENCES metadata.extraction_batches(batch_id),

    CONSTRAINT ck_table_load_status_rows
        CHECK (total_rows_loaded >= 0),

    CONSTRAINT ck_table_load_status_status
        CHECK (
            last_status IS NULL
            OR last_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')
        )
);


ALTER TABLE metadata.extraction_batches
    ADD COLUMN IF NOT EXISTS source_rows_expected BIGINT
    CHECK (source_rows_expected >= 0);

COMMIT;
