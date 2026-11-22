-- Execute only after applying both DDL files to empty ClickHouse local databases.
USE cdc_fixture;

INSERT INTO cdc_generations (generation_id,lineage,bootstrap_end,source_baseline)
VALUES ('fixture','synthetic',{'topic:0':6},'00000000:00000000:0002');
INSERT INTO cdc_events VALUES
('fixture',0,'topic',0,0,'Students','{"StudentId":1}','r','','{"StudentId":1}','00000000:00000000:0001','01:0','true',1,repeat('a',64)),
('fixture',0,'topic',0,1,'Students','{"StudentId":2}','r','','{"StudentId":2}','00000000:00000000:0002','02:0','last',2,repeat('b',64)),
('fixture',0,'topic',0,2,'CourseOfferings','{"CourseOfferingId":5}','c','','{"Capacity":11}','00000000:00000000:0003','03:1','false',3,repeat('c',64)),
('fixture',0,'topic',0,2,'CourseOfferings','{"CourseOfferingId":5}','c','','{"Capacity":11}','00000000:00000000:0003','03:1','false',3,repeat('c',64)),
('fixture',0,'topic',0,3,'CourseOfferings','{"CourseOfferingId":5}','u','{"Capacity":11}','{"Capacity":12}','00000000:00000000:0004','04:1','false',4,repeat('d',64)),
('fixture',0,'topic',0,4,'Students','{"StudentId":2}','d','{"StudentId":2}','','00000000:00000000:0005','05:1','false',5,repeat('e',64)),
('fixture',0,'topic',0,5,'Students','{"StudentId":2}','t','','','','00:1','',0,repeat('f',64));
SELECT throwIf((SELECT count() FROM cdc_committed_records) != 0, 'Unsealed CDC writes leaked');
SELECT throwIf((SELECT status FROM cdc_pipeline_health) != 'BOOTSTRAPPING', 'Bootstrap status missing');

INSERT INTO cdc_batch_commits (generation_id,batch_id,checkpoint_id,record_digest,next_offsets,broker_end_offsets,records,live_source_time_ms)
VALUES ('fixture',0,'test-query',repeat('a',64),{'topic:0':6},{'topic:0':7},6,5);
SELECT throwIf((SELECT count() FROM cdc_business_kpis) != 0, 'Unpublished CDC data leaked');
INSERT INTO cdc_publications (generation_id,baseline_lsn) VALUES ('fixture','00000000:00000000:0002');
SELECT throwIf((SELECT students + abs(toInt64(offered_seats)-12) FROM cdc_business_kpis) != 0, 'CDC delete, old baseline or latest update is wrong');
SELECT throwIf((SELECT offsets_behind_at_last_batch FROM cdc_pipeline_health) != 1, 'CDC lag is wrong');

INSERT INTO cdc_events VALUES
('fixture',1,'topic',0,6,'CourseOfferings','{"CourseOfferingId":5}','c','','{"Capacity":11}','00000000:00000000:0003','03:1','false',3,repeat('c',64)),
('fixture',1,'topic',0,7,'Students','{"StudentId":2}','c','','{"StudentId":2}','00000000:00000000:0006','06:1','false',6,repeat('d',64));
INSERT INTO cdc_batch_commits (generation_id,batch_id,checkpoint_id,record_digest,next_offsets,broker_end_offsets,records)
VALUES ('fixture',1,'test-query',repeat('b',64),{'topic:0':8},{'topic:0':8},2);
SELECT throwIf((SELECT count() FROM cdc_logical_events WHERE source_table='CourseOfferings') != 2, 'Logical replay not deduplicated');
SELECT throwIf((SELECT students + offered_seats FROM cdc_business_kpis) != 13, 'Latest CDC state regressed');

USE batch_fixture;
INSERT INTO dim_applicant_snapshots (snapshot_id,applicant_key,application_type,application_status)
VALUES ('staged','never-published','new','ok'),('current','published','new','ok'),('older','old-run','new','ok');
SELECT throwIf((SELECT count() FROM dim_applicant) != 0, 'Unpublished batch data leaked');
INSERT INTO warehouse_model_loads (snapshot_id,source_run_id,completed_at)
VALUES ('current',10,'2026-01-01 00:00:00'),('older',9,'2026-01-02 00:00:00');
SELECT throwIf((SELECT applicant_key FROM dim_applicant) != 'published', 'Backfill displaced the latest source run');
