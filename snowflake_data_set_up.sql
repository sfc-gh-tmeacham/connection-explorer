-- =============================================================================
-- Data Lake Access Setup Script
-- 
-- Required Privileges:
--   - CREATE DATABASE ON ACCOUNT (or use existing database)
--   - CREATE WAREHOUSE ON ACCOUNT (or use existing warehouse)
--   - CREATE SCHEMA (on the target database)
--   - CREATE TABLE, CREATE STAGE, CREATE PROCEDURE (on the schema)
--   - CREATE TASK, EXECUTE TASK (on account or schema)
--   - IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE (for account_usage views)
--
-- Configuration:
--   Change the database, warehouse, and role below to customize deployment.
-- =============================================================================

SET DB_NAME = 'CONNECTION_EXPLORER_APP_DB';
SET WH_NAME = 'CONNECTION_EXPLORER_WH';
SET DEPLOY_ROLE = 'ACCOUNTADMIN';
SET APP_ROLE = 'SYSADMIN';

USE ROLE IDENTIFIER($DEPLOY_ROLE);

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_NAME)
  WAREHOUSE_SIZE = 'MEDIUM'
  GENERATION = '2'
  ENABLE_QUERY_ACCELERATION = TRUE
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for Snowflake Connection Explorer';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
USE DATABASE IDENTIFIER($DB_NAME);
CREATE SCHEMA IF NOT EXISTS APP;
USE SCHEMA APP;

-- Create stage for Streamlit app deployment
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the table to store 30-day access snapshot (transient - no time travel/fail-safe needed)
CREATE TRANSIENT TABLE IF NOT EXISTS data_lake_access_30d (
    organization_name VARCHAR,
    account_id VARCHAR,
    client VARCHAR,
    warehouse VARCHAR,
    database VARCHAR,
    schema_name VARCHAR,
    direction VARCHAR,
    access_count NUMBER
);

-- Add schema_name column if upgrading from an older version of the table
ALTER TABLE IF EXISTS data_lake_access_30d ADD COLUMN IF NOT EXISTS schema_name VARCHAR;

-- Create the client application classification lookup table
-- Seeded automatically by the Streamlit app from components/client_mappings.py.
-- The stored procedure joins against this table during refresh.
CREATE TABLE IF NOT EXISTS client_app_classification (
    priority       NUMBER,
    pattern        VARCHAR,
    source_field   VARCHAR,
    display_name   VARCHAR
);

-- Create the refresh stored procedure
-- Now uses the client_app_classification lookup table instead of a CASE/WHEN block.
CREATE OR ALTER PROCEDURE REFRESH_DATA_LAKE_ACCESS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    INSERT OVERWRITE INTO data_lake_access_30d (
        organization_name, account_id, client, warehouse, 
        database, schema_name, direction, access_count
    )
    WITH raw_sessions AS (
        SELECT
            CURRENT_ORGANIZATION_NAME() AS organization_name,
            CURRENT_ACCOUNT() AS account_id,
            s.client_application_id AS client_app_id,
            PARSE_JSON(s.client_environment):APPLICATION::VARCHAR AS application,
            q.warehouse_name AS warehouse,
            q.query_type,
            q.query_id,
            SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) AS database,
            SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) || '.' || SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 2) AS schema_name
        FROM snowflake.account_usage.sessions s
        INNER JOIN snowflake.account_usage.query_history q 
            ON q.session_id = s.session_id
        INNER JOIN snowflake.account_usage.access_history a 
            ON q.query_id = a.query_id,
        TABLE(FLATTEN(a.direct_objects_accessed)) t
        WHERE q.start_time > DATEADD(day, -30, CURRENT_DATE())
            AND q.query_type != 'CALL'
            AND s.client_application_id NOT LIKE 'SYSTEM%'
            AND SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) NOT LIKE 'USER$%'
    ),
    -- Match each session against the classification lookup table.
    -- A session can match on "application" or "client_app_id"; we keep only
    -- the highest-priority (lowest priority number) match per row.
    classified AS (
        SELECT
            rs.*,
            c.display_name,
            c.priority,
            ROW_NUMBER() OVER (
                PARTITION BY rs.query_id, rs.database, rs.schema_name
                ORDER BY c.priority ASC
            ) AS rn
        FROM raw_sessions rs
        LEFT JOIN client_app_classification c
            ON (c.source_field = 'client_app_id' AND rs.client_app_id ILIKE c.pattern)
            OR (c.source_field = 'application'   AND rs.application   ILIKE c.pattern)
    ),
    raw_access AS (
        SELECT
            organization_name,
            account_id,
            COALESCE(
                display_name,
                CASE
                    WHEN application IS NOT NULL THEN application
                    ELSE client_app_id || IFNULL(application, '')
                END
            ) AS client,
            warehouse,
            query_type,
            query_id,
            database,
            schema_name
        FROM classified
        WHERE rn = 1 OR display_name IS NULL
    )
    SELECT 
        organization_name,
        account_id, 
        client, 
        warehouse,
        database,
        schema_name,
        CASE
            WHEN query_type LIKE 'CREATE%' 
                 OR query_type LIKE 'ALTER%' 
                 OR query_type LIKE 'DROP%' 
                 OR query_type LIKE 'TRUNCATE%' 
                 OR query_type LIKE 'RENAME%' 
                 OR query_type LIKE 'UNDROP%'
                 OR query_type = 'COMMENT' 
            THEN 'DDL'
            WHEN query_type LIKE 'SHOW%' OR query_type LIKE 'DESCRIBE%' OR query_type IN ('DESC', 'LIST_FILES', 'EXPLAIN') THEN 'metadata'
            WHEN query_type IN ('SELECT', 'UNLOAD', 'GET_FILES') THEN 'read'
            WHEN query_type IN ('COPY_FILES', 'REMOVE_FILES') THEN 'write'
            ELSE 'write'
        END AS direction, 
        COUNT(DISTINCT query_id) AS access_count
    FROM raw_access
    GROUP BY ALL;
    
    RETURN 'Data lake access data refreshed successfully';
END;
$$;

-- Suspend the task if it already exists (required before CREATE OR ALTER)
ALTER TASK IF EXISTS DATA_LAKE_ACCESS_REFRESH_TASK SUSPEND;

-- Create or update the refresh task that calls the stored procedure
CREATE OR ALTER TASK DATA_LAKE_ACCESS_REFRESH_TASK
  WAREHOUSE = IDENTIFIER($WH_NAME)
  SCHEDULE = 'USING CRON 0 6 * * 0 America/Chicago'
  COMMENT = 'Refreshes data lake access data every Sunday at 6am CST'
AS
  CALL REFRESH_DATA_LAKE_ACCESS();

-- Resume the task so it runs on schedule
ALTER TASK DATA_LAKE_ACCESS_REFRESH_TASK RESUME;

-- Execute the procedure immediately to populate initial data
CALL REFRESH_DATA_LAKE_ACCESS();

-- Grant access to the app role
GRANT USAGE ON DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($APP_ROLE);
GRANT CREATE SCHEMA ON DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($APP_ROLE);
GRANT USAGE ON SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT CREATE TABLE ON SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT CREATE STAGE ON SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT SELECT ON ALL TABLES IN SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT INSERT ON ALL TABLES IN SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT UPDATE ON ALL TABLES IN SCHEMA APP TO ROLE IDENTIFIER($APP_ROLE);
GRANT USAGE ON WAREHOUSE IDENTIFIER($WH_NAME) TO ROLE IDENTIFIER($APP_ROLE);
-- NOTE: The GRANT USAGE ON STREAMLIT is handled by the deploy script
-- after `snow streamlit deploy` creates the app.
