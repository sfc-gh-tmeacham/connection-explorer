-- =============================================================================
-- Data Lake Access Setup Script
-- 
-- Required Privileges:
--   - CREATE DATABASE ON ACCOUNT (or use existing database)
--   - CREATE WAREHOUSE ON ACCOUNT (or use existing warehouse)
--   - CREATE COMPUTE POOL ON ACCOUNT (for Streamlit on Container)
--   - CREATE SCHEMA (on the target database)
--   - CREATE TABLE, CREATE STAGE, CREATE PROCEDURE (on the schema)
--   - CREATE TASK, EXECUTE TASK (on account or schema)
--   - IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE (for account_usage views)
--
-- Configuration:
--   Change the database, schema, warehouse, compute pool, and role below to customize deployment.
-- =============================================================================

SET DB_NAME = 'CONNECTION_EXPLORER_APP_DB';
SET SCHEMA_NAME = 'APP';
SET WH_NAME = 'CONNECTION_EXPLORER_WH';
SET COMPUTE_POOL_NAME = 'STREAMLIT_COMPUTE_POOL';  -- Compute pool for Streamlit on Container
SET EAI_NAME = 'PYPI_ACCESS_INTEGRATION';           -- External access integration for PyPI packages
SET DEPLOY_ROLE = 'ACCOUNTADMIN';
SET APP_OWNER_ROLE = 'SYSADMIN';  -- Role that owns the Streamlit app
SET FQ_SCHEMA = $DB_NAME || '.' || $SCHEMA_NAME;    -- Fully-qualified schema for GRANT statements

USE ROLE IDENTIFIER($DEPLOY_ROLE);

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($WH_NAME)
  WAREHOUSE_SIZE = 'MEDIUM'
  GENERATION = '2'
  ENABLE_QUERY_ACCELERATION = TRUE
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for Snowflake Connection Explorer';

-- Create compute pool for Streamlit on Container deployment
CREATE COMPUTE POOL IF NOT EXISTS IDENTIFIER($COMPUTE_POOL_NAME)
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS
  AUTO_SUSPEND_SECS = 300
  AUTO_RESUME = TRUE
  COMMENT = 'Compute pool for Snowflake Connection Explorer Streamlit app';

-- Create external access integration for PyPI package installation (container runtime)
-- Uses the Snowflake-managed network rule — no custom network rule required.
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS IDENTIFIER($EAI_NAME)
  ALLOWED_NETWORK_RULES = (snowflake.external_access.pypi_rule)
  ENABLED = TRUE
  COMMENT = 'Allows Streamlit container runtime to install packages from PyPI';

-- Create database and schema
CREATE DATABASE IF NOT EXISTS IDENTIFIER($DB_NAME);
USE DATABASE IDENTIFIER($DB_NAME);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_NAME);
USE SCHEMA IDENTIFIER($SCHEMA_NAME);

-- Create stage for Streamlit app deployment
CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the table to store 30-day access snapshot (transient - no time travel/fail-safe needed)
CREATE TRANSIENT TABLE IF NOT EXISTS CONNECTION_ACCESS_30D (
    organization_name VARCHAR,
    account_id VARCHAR,
    client VARCHAR,
    warehouse VARCHAR,
    database VARCHAR,
    schema_name VARCHAR,
    direction VARCHAR,
    access_count NUMBER
)
COMMENT = '30-day access snapshot aggregated by REFRESH_CONNECTION_ACCESS()';

-- Add schema_name column if upgrading from an older version of the table
ALTER TABLE IF EXISTS CONNECTION_ACCESS_30D ADD COLUMN IF NOT EXISTS schema_name VARCHAR;

-- Create the client application classification lookup table
-- Seeded automatically by the Streamlit app from components/client_mappings.py.
-- The stored procedure joins against this table during refresh.
CREATE TABLE IF NOT EXISTS CLIENT_APP_CLASSIFICATION (
    priority       NUMBER,
    pattern        VARCHAR,
    source_field   VARCHAR,
    display_name   VARCHAR
)
COMMENT = 'Client application pattern-matching rules for display name resolution';

-- Create the refresh stored procedure
-- Now uses the CLIENT_APP_CLASSIFICATION lookup table instead of a CASE/WHEN block.
-- Logging requires an event table configured at the account level:
--   CREATE EVENT TABLE IF NOT EXISTS <db>.<schema>.<table>;
--   ALTER ACCOUNT SET EVENT_TABLE = '<db>.<schema>.<table>';
CREATE OR ALTER PROCEDURE REFRESH_CONNECTION_ACCESS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    row_count INTEGER;
    result_msg VARCHAR;
BEGIN
    SYSTEM$LOG_INFO('REFRESH_CONNECTION_ACCESS: Starting data refresh');

    /*
     * Pipeline overview
     * =================
     * Joins sessions → query_history → access_history to build a 30-day
     * snapshot of which clients accessed which databases/schemas, via
     * which warehouses, and whether the access was read or write.
     *
     * CTE chain:
     *   raw_sessions     – Flattens direct_objects_accessed, extracts
     *                       database and qualified schema name from each
     *                       objectName.  Filters out noise:
     *                         • CALL statements (app function invocations)
     *                         • Session/transaction commands (BEGIN, SET, USE…)
     *                         • SYSTEM% client apps (internal Snowflake processes)
     *                         • USER$% temp databases (query result caches)
     *                         • APP!FUNC names (app-qualified function calls like
     *                           BENCH_V2!SPCS_GET_LOGS — these use '!' not '.'
     *                           and would produce bogus DB/schema nodes)
     *                       schema_name is NULL when objectName has no dots
     *                       (bare database-level access like SHOW/DESCRIBE).
     *
     *   matched_by_*     – Two-pass classification against the
     *                       CLIENT_APP_CLASSIFICATION lookup table.  First
     *                       matches on client_app_id patterns, then on
     *                       application (from CLIENT_ENVIRONMENT JSON).
     *                       Split into two LEFT JOINs to avoid an OR-join
     *                       that would force a cartesian product.
     *
     *   classified       – Picks the best classification match per
     *                       (query_id, database, schema_name) by lowest
     *                       priority value.  ROW_NUMBER deduplicates when
     *                       multiple patterns match.
     *
     *   raw_access       – Resolves the final client display name:
     *                       classification match → application field →
     *                       raw client_application_id as last resort.
     *
     *   Final SELECT     – Categorises query_type into direction
     *                       (read/write/DDL/metadata) and aggregates
     *                       distinct query counts per route.
     */

    INSERT OVERWRITE INTO CONNECTION_ACCESS_30D (
        organization_name, account_id, client, warehouse, 
        database, schema_name, direction, access_count
    )
    WITH raw_sessions AS (
        SELECT
            CURRENT_ORGANIZATION_NAME() AS organization_name,
            CURRENT_ACCOUNT_NAME() AS account_id,
            s.client_application_id AS client_app_id,
            PARSE_JSON(s.client_environment):APPLICATION::VARCHAR AS application,
            q.warehouse_name AS warehouse,
            q.query_type,
            q.query_id,
            SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) AS database,
            CASE WHEN SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 2) != ''
                 THEN SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) || '.' || SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 2)
                 ELSE NULL
            END AS schema_name
        FROM snowflake.account_usage.sessions s
        INNER JOIN snowflake.account_usage.query_history q 
            ON q.session_id = s.session_id
        INNER JOIN snowflake.account_usage.access_history a 
            ON q.query_id = a.query_id,
        TABLE(FLATTEN(a.direct_objects_accessed)) t
        WHERE q.start_time > DATEADD(day, -30, CURRENT_DATE())
            AND q.query_type != 'CALL'
            AND q.query_type NOT IN ('BEGIN_TRANSACTION', 'COMMIT', 'ROLLBACK', 'SET', 'UNSET', 'USE')
            AND s.client_application_id NOT LIKE 'SYSTEM%'
            AND SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) NOT LIKE 'USER$%'
            AND t.VALUE:objectName::VARCHAR NOT LIKE '%!%'
    ),
    -- Split-join approach for better performance with large query volumes.
    -- Step 1: Match on client_app_id patterns only
    matched_by_app_id AS (
        SELECT
            rs.*,
            c.display_name AS dn_app_id,
            c.priority AS p_app_id
        FROM raw_sessions rs
        LEFT JOIN CLIENT_APP_CLASSIFICATION c
            ON c.source_field = 'client_app_id' 
            AND rs.client_app_id ILIKE c.pattern
    ),
    -- Step 2: Match on application patterns only
    matched_by_application AS (
        SELECT
            m.*,
            c.display_name AS dn_app,
            c.priority AS p_app
        FROM matched_by_app_id m
        LEFT JOIN CLIENT_APP_CLASSIFICATION c
            ON c.source_field = 'application' 
            AND m.application ILIKE c.pattern
    ),
    -- Step 3: Pick the best match (lowest priority value = highest rank)
    classified AS (
        SELECT
            organization_name, account_id, client_app_id, application,
            warehouse, query_type, query_id, database, schema_name,
            CASE 
                WHEN COALESCE(p_app_id, 999) <= COALESCE(p_app, 999) THEN dn_app_id 
                ELSE dn_app 
            END AS display_name,
            LEAST(COALESCE(p_app_id, 999), COALESCE(p_app, 999)) AS priority,
            ROW_NUMBER() OVER (
                PARTITION BY query_id, database, schema_name
                ORDER BY LEAST(COALESCE(p_app_id, 999), COALESCE(p_app, 999)) ASC
            ) AS rn
        FROM matched_by_application
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
                 OR query_type IN ('COMMENT', 'GRANT', 'REVOKE', 'RESTORE')
            THEN 'DDL'
            WHEN query_type LIKE 'SHOW%' OR query_type LIKE 'DESCRIBE%' OR query_type IN ('DESC', 'LIST_FILES', 'EXPLAIN') THEN 'metadata'
            WHEN query_type IN ('SELECT', 'UNLOAD', 'GET_FILES') THEN 'read'
            WHEN query_type IN ('COPY_FILES', 'REMOVE_FILES') THEN 'write'
            ELSE 'write'
        END AS direction, 
        COUNT(DISTINCT query_id) AS access_count
    FROM raw_access
    GROUP BY ALL;

    -- Log the row count after the refresh
    SELECT COUNT(*) INTO :row_count FROM CONNECTION_ACCESS_30D;
    result_msg := 'Connection access data refreshed successfully. Rows: ' || :row_count;
    SYSTEM$LOG_INFO('REFRESH_CONNECTION_ACCESS: ' || :result_msg);
    RETURN result_msg;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG_ERROR('REFRESH_CONNECTION_ACCESS: Failed with SQLCODE=' || :SQLCODE || ' — ' || :SQLERRM);
        RAISE;
END;
$$;

-- Enable INFO-level logging so SYSTEM$LOG_INFO messages are captured
ALTER PROCEDURE REFRESH_CONNECTION_ACCESS() SET LOG_LEVEL = INFO;

-- Suspend the task if it already exists (required before CREATE OR ALTER)
ALTER TASK IF EXISTS DATA_ACCESS_REFRESH_TASK SUSPEND;

-- Create or update the serverless refresh task
-- Serverless tasks omit the WAREHOUSE clause; Snowflake manages compute
-- automatically.  The role that owns the task needs EXECUTE MANAGED TASK
-- ON ACCOUNT (granted below).
--
-- Why EXECUTE IMMEDIATE + $$ delimiters?
-- ───────────────────────────────────────
-- CREATE TASK ... AS expects a single SQL statement.  We need two
-- statements (CALL the procedure, then SET_RETURN_VALUE), so we wrap
-- them in an anonymous Snowscript block.  The $$ dollar-quoting prevents
-- the inner semicolons from being parsed as statement terminators by
-- the outer CREATE TASK.
--
-- Why not call SYSTEM$SET_RETURN_VALUE inside the stored procedure?
-- ─────────────────────────────────────────────────────────────────
-- SYSTEM$SET_RETURN_VALUE is a side-effect function that can ONLY run
-- in the task execution context.  Calling it from inside a stored
-- procedure (even with CALL syntax) fails with:
--   "Query called from a stored procedure contains a function with
--    side effects [SYSTEM$SET_RETURN_VALUE]."
-- The workaround is to capture the procedure's RETURN value at the
-- task level via CALL ... INTO, then pass it to SET_RETURN_VALUE.
-- This populates the RETURN_VALUE column in TASK_HISTORY() output.
CREATE OR ALTER TASK DATA_ACCESS_REFRESH_TASK
  SCHEDULE = 'USING CRON 0 6 * * 0 America/Chicago'
  COMMENT = 'Refreshes connection access data every Sunday at 6am CST'
AS
  EXECUTE IMMEDIATE
  $$
  DECLARE
    result VARCHAR;  -- holds the procedure's return message (e.g. "... Rows: 59")
  BEGIN
    -- Run the refresh; captures the RETURN string into :result
    CALL REFRESH_CONNECTION_ACCESS() INTO :result;
    -- Surface the result in TASK_HISTORY().RETURN_VALUE
    CALL SYSTEM$SET_RETURN_VALUE(:result);
  END;
  $$;

-- Resume the task so it runs on schedule
ALTER TASK DATA_ACCESS_REFRESH_TASK RESUME;

-- Execute the task immediately to populate initial data
EXECUTE TASK DATA_ACCESS_REFRESH_TASK;

-- Grant access to the app owner role
GRANT USAGE ON DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT CREATE SCHEMA ON DATABASE IDENTIFIER($DB_NAME) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT USAGE ON SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT CREATE TABLE ON SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT CREATE STAGE ON SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT INSERT ON ALL TABLES IN SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT UPDATE ON ALL TABLES IN SCHEMA IDENTIFIER($FQ_SCHEMA) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT USAGE ON WAREHOUSE IDENTIFIER($WH_NAME) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT USAGE ON COMPUTE POOL IDENTIFIER($COMPUTE_POOL_NAME) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT USAGE ON INTEGRATION IDENTIFIER($EAI_NAME) TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT USAGE ON PROCEDURE REFRESH_CONNECTION_ACCESS() TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT OPERATE ON TASK DATA_ACCESS_REFRESH_TASK TO ROLE IDENTIFIER($APP_OWNER_ROLE);
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE IDENTIFIER($APP_OWNER_ROLE);
-- NOTE: The GRANT USAGE ON STREAMLIT is handled by the deploy script
-- after `snow streamlit deploy` creates the app.
