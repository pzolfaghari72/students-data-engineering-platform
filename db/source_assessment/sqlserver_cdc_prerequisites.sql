/*
    Read-only SQL Server CDC readiness checks for an authorized DBA.

    Run against the intended Students instance using an authorized DBA session.
    This script does not enable CDC,
    change permissions, alter source data, or change database options.
    Run as sysadmin or a principal allowed to impersonate the connector login.
    The SQL Server Agent DMV check requires VIEW SERVER STATE.
*/

USE [Students];
GO
SET NOCOUNT ON;

SELECT
    DB_NAME() AS database_name,
    d.is_cdc_enabled,
    d.snapshot_isolation_state_desc,
    d.is_read_committed_snapshot_on
FROM sys.databases AS d
WHERE d.database_id = DB_ID();

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

SELECT
    expected.table_name,
    capture.capture_instance,
    capture.role_name,
    capture.supports_net_changes
FROM @expected_tables AS expected
LEFT JOIN cdc.change_tables AS capture
    ON capture.source_object_id =
       OBJECT_ID(QUOTENAME(N'dbo') + N'.' + QUOTENAME(expected.table_name))
ORDER BY expected.table_name;

DECLARE @connector_login SYSNAME = N'<configured connector login>';

IF SUSER_ID(@connector_login) IS NULL
BEGIN
    PRINT N'Replace @connector_login with the existing Debezium SQL Server login.';
END
ELSE
BEGIN
    EXECUTE AS LOGIN = @connector_login;

    SELECT
        USER_NAME() AS effective_database_user,
        SUSER_SNAME() AS effective_login,
        IS_MEMBER(N'students_debezium_cdc_reader') AS is_cdc_reader_member,
        HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DATABASE STATE')
            AS has_view_database_state,
        HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION')
            AS has_view_definition,
        HAS_PERMS_BY_NAME(NULL, N'SERVER', N'VIEW SERVER STATE')
            AS has_view_server_state,
        HAS_PERMS_BY_NAME(N'dbo.Students', N'OBJECT', N'SELECT')
            AS can_select_source_table,
        HAS_PERMS_BY_NAME(N'cdc.dbo_Students_CT', N'OBJECT', N'SELECT')
            AS can_select_cdc_change_table;

    SELECT
        servicename,
        status_desc,
        startup_type_desc
    FROM sys.dm_server_services
    WHERE servicename LIKE N'%SQL Server Agent%';

    EXEC sys.sp_cdc_help_jobs;

    REVERT;
END;
