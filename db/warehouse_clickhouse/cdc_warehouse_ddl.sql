CREATE TABLE IF NOT EXISTS cdc_generations
(
    generation_id String,
    lineage String,
    bootstrap_end Map(String, UInt64),
    source_baseline String,
    created_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY (generation_id, created_at);

CREATE TABLE IF NOT EXISTS cdc_events
(
    generation_id String,
    batch_id UInt64,
    kafka_topic LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset UInt64,
    source_table LowCardinality(String),
    source_key String,
    operation LowCardinality(String),
    before_image String,
    after_image String,
    commit_lsn String,
    source_version String,
    snapshot_state LowCardinality(String),
    source_time_ms UInt64,
    record_hash FixedString(64)
)
ENGINE = MergeTree
ORDER BY (generation_id, batch_id, kafka_topic, kafka_partition, kafka_offset);

CREATE TABLE IF NOT EXISTS cdc_batch_commits
(
    generation_id String,
    batch_id UInt64,
    checkpoint_id String,
    record_digest FixedString(64),
    next_offsets Map(String, UInt64),
    broker_end_offsets Map(String, UInt64),
    records UInt64,
    live_source_time_ms UInt64 DEFAULT 0,
    verified_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY (generation_id, batch_id);

CREATE TABLE IF NOT EXISTS cdc_publications
(
    generation_id String,
    baseline_lsn String,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree ORDER BY (generation_id, published_at);

CREATE OR REPLACE VIEW cdc_active_generation AS
SELECT generation_id, baseline_lsn, published_at
FROM cdc_publications
ORDER BY published_at DESC, generation_id DESC
LIMIT 1;

CREATE OR REPLACE VIEW cdc_committed_records AS
SELECT *
FROM cdc_events
WHERE (generation_id, batch_id) IN (SELECT generation_id, batch_id FROM cdc_batch_commits);

-- Serve the active baseline and its committed live changes.
CREATE OR REPLACE VIEW cdc_serving_records AS
SELECT events.*
FROM cdc_committed_records AS events
INNER JOIN cdc_active_generation AS active ON events.generation_id = active.generation_id
WHERE (events.operation = 'r' AND events.commit_lsn = active.baseline_lsn)
   OR (events.operation IN ('c', 'u', 'd') AND events.commit_lsn >= active.baseline_lsn);

-- Replays at new Kafka offsets still represent the same source change.
CREATE OR REPLACE VIEW cdc_logical_events AS
SELECT
    generation_id,
    source_table,
    source_key,
    if(operation = 'r', commit_lsn, source_version) AS event_version,
    operation,
    argMin(tuple(before_image, after_image, source_time_ms),
           tuple(kafka_topic, kafka_partition, kafka_offset)) AS event
FROM cdc_serving_records
GROUP BY generation_id, source_table, source_key, event_version, operation;

CREATE OR REPLACE VIEW cdc_current_state AS
SELECT
    generation_id,
    source_table,
    source_key,
    latest.2 AS row_image,
    fromUnixTimestamp64Milli(toInt64(latest.3), 'UTC') AS source_timestamp
FROM
(
    SELECT generation_id, source_table, source_key,
           argMax(tuple(operation, after_image, source_time_ms),
                  tuple(source_version, kafka_partition, kafka_offset)) AS latest
    FROM cdc_serving_records
    GROUP BY generation_id, source_table, source_key
)
WHERE latest.1 != 'd';

CREATE OR REPLACE VIEW cdc_business_kpis SQL SECURITY DEFINER AS
SELECT active.generation_id AS generation_id, active.published_at AS baseline_published_at,
       totals.enrollments, totals.offerings, totals.applications, totals.students,
       totals.offered_seats, totals.graded_enrollments
FROM cdc_active_generation AS active
CROSS JOIN
(
    SELECT countIf(source_table = 'CourseEnrollments') AS enrollments,
           countIf(source_table = 'CourseOfferings') AS offerings,
           countIf(source_table = 'Applicants') AS applications,
           countIf(source_table = 'Students') AS students,
           sumIf(latest.2, source_table = 'CourseOfferings') AS offered_seats,
           countIf(source_table = 'CourseEnrollments' AND latest.3) AS graded_enrollments
    FROM
    (
        SELECT source_table, source_key,
               argMax(tuple(operation = 'd', JSONExtractUInt(after_image, 'Capacity'),
                            JSONExtractString(after_image, 'Grade') != ''),
                      tuple(source_version, kafka_partition, kafka_offset)) AS latest
        FROM cdc_serving_records
        WHERE source_table IN ('CourseEnrollments', 'CourseOfferings', 'Applicants', 'Students')
        GROUP BY source_table, source_key
    )
    WHERE NOT latest.1
) AS totals;

CREATE OR REPLACE VIEW cdc_pipeline_health SQL SECURITY DEFINER AS
WITH latest_generation AS
(
    SELECT generation_id, bootstrap_end, created_at
    FROM cdc_generations
    ORDER BY created_at DESC, generation_id DESC
    LIMIT 1
),
latest_batch AS
(
    SELECT generation_id,
           argMax(next_offsets, batch_id) AS offsets,
           argMax(broker_end_offsets, batch_id) AS broker_ends,
           max(verified_at) AS last_commit_at,
           argMaxIf(live_source_time_ms, batch_id, live_source_time_ms > 0) AS source_time_ms,
           maxIf(verified_at, live_source_time_ms > 0) AS live_verified_at
    FROM cdc_batch_commits
    WHERE generation_id IN (SELECT generation_id FROM latest_generation)
    GROUP BY generation_id
),
published AS
(
    SELECT generation_id, max(published_at) AS published_at
    FROM cdc_publications
    GROUP BY generation_id
)
SELECT
    generation.generation_id AS generation_id,
    nullIf((SELECT any(generation_id) FROM cdc_active_generation), '') AS active_generation,
    if(publication.published_at > toDateTime64(0, 3, 'UTC'), 'ACTIVE', 'BOOTSTRAPPING') AS status,
    nullIf(batch.last_commit_at, toDateTime64(0, 3, 'UTC')) AS last_verified_at,
    arraySum(mapValues(generation.bootstrap_end)) AS bootstrap_records,
    arraySum(arrayMap(part -> least(batch.offsets[part], generation.bootstrap_end[part]),
                      mapKeys(generation.bootstrap_end))) AS verified_bootstrap_records,
    if(isNull(last_verified_at), NULL, arraySum(arrayMap(
        part -> greatest(toInt64(batch.broker_ends[part]) - toInt64(batch.offsets[part]), 0),
        mapKeys(batch.broker_ends)))) AS offsets_behind_at_last_batch,
    if(batch.source_time_ms = 0, NULL,
       greatest(toUnixTimestamp64Milli(batch.live_verified_at) - toInt64(batch.source_time_ms), 0))
       AS last_live_processing_delay_ms
FROM latest_generation AS generation
LEFT JOIN latest_batch AS batch ON batch.generation_id = generation.generation_id
LEFT JOIN published AS publication ON publication.generation_id = generation.generation_id;
