/*
    Run this script only as an authorized SQL Server database administrator,
    through sqlcmd with SourceDatabase and SourceSchema set for the intended source.
    SQL Server Agent must be running for CDC capture jobs to advance.
*/

IF DB_NAME() <> N'$(SourceDatabase)'
    THROW 51000, 'Connect to the configured source database before enabling CDC.', 1;

DECLARE @source_schema SYSNAME = N'$(SourceSchema)';

IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE database_id = DB_ID()
      AND is_cdc_enabled = 1
)
    EXEC sys.sp_cdc_enable_db;

IF DATABASE_PRINCIPAL_ID(N'students_debezium_cdc_reader') IS NULL
    EXEC (N'CREATE ROLE [students_debezium_cdc_reader]');

DECLARE @tables TABLE (table_name SYSNAME NOT NULL);
INSERT INTO @tables (table_name)
VALUES
    (N'Applicants'),
    (N'ClassStandings'),
    (N'CourseEnrollments'),
    (N'CourseOfferings'),
    (N'Courses'),
    (N'DegreeLevels'),
    (N'DegreeRequirements'),
    (N'Degrees'),
    (N'DegreeTypes'),
    (N'Departments'),
    (N'Grades'),
    (N'Sessions'),
    (N'StudentProfiles'),
    (N'Students'),
    (N'Terms');

DECLARE @table_name SYSNAME;
DECLARE source_tables CURSOR LOCAL FAST_FORWARD FOR
    SELECT table_name FROM @tables;

OPEN source_tables;
FETCH NEXT FROM source_tables INTO @table_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID(QUOTENAME(@source_schema) + N'.' + QUOTENAME(@table_name), N'U') IS NULL
        THROW 51001, 'An assessed source table is missing from the configured schema.', 1;

    IF NOT EXISTS (
        SELECT 1
        FROM cdc.change_tables
        WHERE source_object_id = OBJECT_ID(QUOTENAME(@source_schema) + N'.' + QUOTENAME(@table_name))
    )
        EXEC sys.sp_cdc_enable_table
            @source_schema = @source_schema,
            @source_name = @table_name,
            @role_name = N'students_debezium_cdc_reader',
            @supports_net_changes = 0;

    FETCH NEXT FROM source_tables INTO @table_name;
END;

CLOSE source_tables;
DEALLOCATE source_tables;
