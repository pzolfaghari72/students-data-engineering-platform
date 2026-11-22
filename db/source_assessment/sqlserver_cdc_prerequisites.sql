/*
Run through sqlcmd in the configured source database with SourceSchema and
ConnectorLogin variables. The DBA session needs VIEW SERVER STATE and permission
to impersonate the existing connector login. This script is read-only.
*/
SET NOCOUNT ON;

DECLARE @source_schema SYSNAME = N'$(SourceSchema)';
DECLARE @connector_login SYSNAME = N'$(ConnectorLogin)';

SELECT name AS database_name, is_cdc_enabled, snapshot_isolation_state_desc
FROM sys.databases
WHERE database_id = DB_ID();

DECLARE @expected_tables TABLE (table_name SYSNAME NOT NULL);
INSERT INTO @expected_tables (table_name)
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


IF OBJECT_ID(N'cdc.change_tables') IS NULL
BEGIN
    PRINT N'CDC is disabled or the DBA session cannot inspect its catalog.';
    RETURN;
END;

SELECT servicename, status_desc, startup_type_desc
FROM sys.dm_server_services
WHERE servicename LIKE N'%SQL Server Agent%';

EXEC sys.sp_cdc_help_jobs;

DECLARE @capture TABLE (
    table_name SYSNAME,
    capture_instance SYSNAME,
    role_name SYSNAME,
    change_table SYSNAME
);
INSERT INTO @capture
SELECT expected.table_name, capture.capture_instance, capture.role_name,
       OBJECT_NAME(capture.object_id)
FROM @expected_tables AS expected
LEFT JOIN cdc.change_tables AS capture
    ON capture.source_object_id = OBJECT_ID(
        QUOTENAME(@source_schema) + N'.' + QUOTENAME(expected.table_name)
    );

SELECT table_name, capture_instance, role_name,
       sys.fn_cdc_get_min_lsn(capture_instance) AS retained_start_lsn
FROM @capture ORDER BY table_name, capture_instance;
SELECT sys.fn_cdc_get_max_lsn() AS captured_end_lsn;

IF SUSER_ID(@connector_login) IS NULL
    THROW 51000, 'ConnectorLogin must identify an existing SQL Server login.', 1;

EXECUTE AS LOGIN = @connector_login;
SELECT
    table_name,
    capture_instance,
    CASE WHEN role_name IS NULL THEN NULL ELSE IS_MEMBER(role_name) END AS capture_role_member,
    HAS_PERMS_BY_NAME(QUOTENAME(@source_schema) + N'.' + QUOTENAME(table_name), N'OBJECT', N'SELECT')
        AS source_select,
    HAS_PERMS_BY_NAME(N'cdc.' + QUOTENAME(change_table), N'OBJECT', N'SELECT') AS change_table_select
FROM @capture ORDER BY table_name, capture_instance;

SELECT HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',N'VIEW DATABASE STATE') AS can_view_database_state,
       HAS_PERMS_BY_NAME(DB_NAME(),N'DATABASE',N'VIEW DEFINITION') AS can_view_definition,
       HAS_PERMS_BY_NAME(NULL,N'SERVER',N'VIEW SERVER STATE') AS can_view_server_state;
REVERT;
