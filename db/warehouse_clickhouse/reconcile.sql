SELECT
    batch.source_run_id,
    batch.snapshot_started_at AS batch_snapshot_started_at,
    batch.completed_at AS batch_published_at,
    cdc.generation_id AS cdc_generation,
    cdc.baseline_published_at AS cdc_baseline_published_at,
    health.last_verified_at AS cdc_last_verified_at,
    health.offsets_behind_at_last_batch,
    batch.fact_application_rows AS batch_applications,
    cdc.applications AS cdc_applications,
    toInt64(cdc.applications) - toInt64(batch.fact_application_rows) AS applications_delta,
    batch.fact_course_offering_rows AS batch_offerings,
    cdc.offerings AS cdc_offerings,
    toInt64(cdc.offerings) - toInt64(batch.fact_course_offering_rows) AS offerings_delta,
    batch.fact_enrollment_rows AS batch_enrollments,
    cdc.enrollments AS cdc_enrollments,
    toInt64(cdc.enrollments) - toInt64(batch.fact_enrollment_rows) AS enrollments_delta
FROM batch_publication_status AS batch
CROSS JOIN {cdc_db:Identifier}.cdc_business_kpis AS cdc
LEFT JOIN {cdc_db:Identifier}.cdc_pipeline_health AS health ON health.generation_id = cdc.generation_id;
