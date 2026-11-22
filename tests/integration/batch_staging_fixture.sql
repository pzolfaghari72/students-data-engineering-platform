-- Run against a disposable database after applying the PostgreSQL staging DDL.
INSERT INTO metadata.pipeline_runs (pipeline_name,run_type,status,completed_at)
SELECT 'sqlserver_to_postgres_staging','FULL','SUCCESS',CURRENT_TIMESTAMP FROM generate_series(1,2);

INSERT INTO metadata.extraction_batches
    (run_id,source_schema,source_table,extraction_type,status,completed_at,
     extracted_rows,inserted_rows,source_rows_expected)
SELECT run_id,'dbo',source.table_name,'SNAPSHOT','SUCCESS',CURRENT_TIMESTAMP,
       case when run_id = 1 then 1 else 0 end,
       case when run_id = 1 then 1 else 0 end,
       case when run_id = 1 then 1 else 0 end
FROM metadata.pipeline_runs
CROSS JOIN unnest(ARRAY['Applicants','ClassStandings','CourseEnrollments','CourseOfferings','Courses',
    'DegreeLevels','DegreeRequirements','Degrees','DegreeTypes','Departments','Grades','Sessions',
    'StudentProfiles','Students','Terms']) AS source(table_name);

CREATE FUNCTION fixture_batch(table_name text) RETURNS bigint LANGUAGE SQL AS
$$ SELECT batch_id FROM metadata.extraction_batches WHERE run_id=1 AND source_table=table_name $$;
INSERT INTO raw.applicants VALUES
  (1,'Test','','Applicant','F','Test','Test','CA','00000','0','test@example.org',
   '2001-01-01','2024-01-01','2024FA','N',3.5,400,410,420,5,'A',20,fixture_batch('Applicants'),now());
INSERT INTO raw.class_standings VALUES ('FR','Freshman','B',0,fixture_batch('ClassStandings'),now());
INSERT INTO raw.course_enrollments VALUES (10,1,1,'A',fixture_batch('CourseEnrollments'),now());
INSERT INTO raw.course_offerings VALUES (1,'CS',101,'2024FA',1,12,fixture_batch('CourseOfferings'),now());
INSERT INTO raw.courses VALUES ('CS',101,'Test course','Test',3.0,12,fixture_batch('Courses'),now());
INSERT INTO raw.degree_levels VALUES ('B','Bachelor',fixture_batch('DegreeLevels'),now());
INSERT INTO raw.degree_requirements VALUES (1,1,'FR','FA','CS',101,fixture_batch('DegreeRequirements'),now());
INSERT INTO raw.degrees VALUES (1,'CS','BSC','Test science',fixture_batch('Degrees'),now());
INSERT INTO raw.degree_types VALUES ('BSC','Bachelor of Science','B',fixture_batch('DegreeTypes'),now());
INSERT INTO raw.departments VALUES ('CS','Computing',fixture_batch('Departments'),now());
INSERT INTO raw.grades VALUES ('A',4.0,true,fixture_batch('Grades'),now());
INSERT INTO raw.sessions VALUES ('FA','Fall',fixture_batch('Sessions'),now());
INSERT INTO raw.student_profiles VALUES (1,2,fixture_batch('StudentProfiles'),now());
INSERT INTO raw.students VALUES
  (1,'Test','','Student','M','Test','Test','CA','00000','0','test@example.org',
    '2001-01-01',1,'FR','A','2024FA',fixture_batch('Students'),now());
INSERT INTO raw.terms VALUES ('2024FA','FA',2024,fixture_batch('Terms'),now());
