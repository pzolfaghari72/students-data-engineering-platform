SET allow_suspicious_low_cardinality_types = 1;

CREATE TABLE IF NOT EXISTS cdc_events (
    generation_id String,
    event_id String,
    source_schema LowCardinality(String),
    source_table LowCardinality(String),
    operation LowCardinality(String),
    source_key String,
    before_image String,
    after_image String,
    source_lsn String,
    source_commit_lsn String,
    source_change_lsn String,
    source_event_serial_no String,
    source_snapshot_state LowCardinality(String),
    source_version String,
    source_timestamp DateTime64(6, 'UTC'),
    kafka_topic String,
    kafka_partition Int32,
    kafka_offset Int64
)
ENGINE = MergeTree
ORDER BY (
    generation_id,
    source_schema,
    source_table,
    kafka_topic,
    kafka_partition,
    kafka_offset
);

CREATE TABLE IF NOT EXISTS warehouse_stream_generations (
    query_name String,
    generation_id String,
    status LowCardinality(String),
    published_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (query_name, generation_id, published_at);

CREATE OR REPLACE VIEW active_stream_generations AS
SELECT
    query_name,
    argMax(generation_id, published_at) AS generation_id
FROM warehouse_stream_generations
WHERE status = 'PUBLISHED'
GROUP BY query_name;

CREATE OR REPLACE VIEW cdc_current_state AS
WITH latest_by_key AS
(
    SELECT
        generation_id,
        source_schema,
        source_table,
        source_key,
        argMax(
            operation,
            tuple(source_version, kafka_partition, kafka_offset)
        ) AS latest_operation,
        argMax(
            after_image,
            tuple(source_version, kafka_partition, kafka_offset)
        ) AS row_image,
        argMax(
            source_timestamp,
            tuple(source_version, kafka_partition, kafka_offset)
        ) AS source_timestamp,
        argMax(
            source_lsn,
            tuple(source_version, kafka_partition, kafka_offset)
        ) AS source_lsn
    FROM cdc_events
    GROUP BY generation_id, source_schema, source_table, source_key
)
SELECT
    latest_by_key.generation_id,
    latest_by_key.source_schema,
    latest_by_key.source_table,
    latest_by_key.source_key,
    latest_by_key.row_image,
    latest_by_key.source_timestamp,
    latest_by_key.source_lsn
FROM latest_by_key
INNER JOIN active_stream_generations AS active
    ON active.generation_id = latest_by_key.generation_id
WHERE latest_by_key.latest_operation NOT IN ('d', 't');

CREATE OR REPLACE VIEW cdc_course_enrollments AS
SELECT
    source_key,
    row_image,
    source_timestamp,
    source_lsn
FROM cdc_current_state
WHERE source_table = 'CourseEnrollments';

CREATE OR REPLACE VIEW cdc_student_scd2 AS
WITH student_events AS
(
    SELECT
        events.generation_id,
        events.source_key,
        events.operation,
        if(events.operation = 'd', events.before_image, events.after_image)
            AS after_image,
        events.source_timestamp,
        events.source_version,
        events.kafka_partition,
        events.kafka_offset,
        row_number() OVER (
            PARTITION BY events.generation_id, events.kafka_topic,
                         events.kafka_partition, events.kafka_offset
            ORDER BY events.event_id
        ) AS event_copy
    FROM cdc_events AS events
    INNER JOIN active_stream_generations AS active
        ON active.generation_id = events.generation_id
    WHERE events.source_table = 'Students'
      AND events.operation IN ('c', 'r', 'u', 'd')
),
versions AS
(
    SELECT
        generation_id,
        source_key,
        operation,
        after_image,
        source_timestamp AS valid_from,
        nullIf(
            leadInFrame(
                source_timestamp,
                1,
                toDateTime64(0, 6, 'UTC')
            ) OVER (
                PARTITION BY generation_id, source_key
                ORDER BY source_version, kafka_partition, kafka_offset
                ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ),
            toDateTime64(0, 6, 'UTC')
        ) AS valid_to,
        row_number() OVER (
            PARTITION BY generation_id, source_key
            ORDER BY source_version DESC, kafka_partition DESC, kafka_offset DESC
        ) = 1 AS is_current,
        operation = 'd' AS is_deleted,
        source_version,
        kafka_partition,
        kafka_offset
    FROM student_events
    WHERE event_copy = 1
)
SELECT
    generation_id,
    source_key,
    after_image AS attributes,
    valid_from,
    valid_to,
    is_current,
    is_deleted,
    source_version,
    kafka_partition,
    kafka_offset
FROM versions;
