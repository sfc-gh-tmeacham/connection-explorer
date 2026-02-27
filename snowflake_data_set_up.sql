-- =============================================================================
-- Data Lake Access Setup Script
-- 
-- Required Privileges:
--   - CREATE DATABASE ON ACCOUNT (or use existing database)
--   - CREATE SCHEMA (on the target database)
--   - CREATE TABLE, CREATE STAGE, CREATE PROCEDURE (on the schema)
--   - CREATE TASK, EXECUTE TASK (on account or schema)
--   - IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE (for account_usage views)
-- =============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_DATA_LAKE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS;

-- Create stage for Streamlit app deployment
CREATE STAGE IF NOT EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Create the table to store 30-day access snapshot
CREATE TABLE IF NOT EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d (
    organization_name VARCHAR,
    account_id VARCHAR,
    client VARCHAR,
    warehouse VARCHAR,
    database VARCHAR,
    direction VARCHAR,
    access_count NUMBER
);

-- Create the client application classification lookup table
-- This replaces the 70+ line CASE/WHEN block with a maintainable table.
-- Priority determines match order (lower = higher priority).
-- source_field: "application" matches the parsed APPLICATION field,
--               "client_app_id" matches the raw CLIENT_APPLICATION_ID.
CREATE TABLE IF NOT EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.client_app_classification (
    priority       NUMBER,
    pattern        VARCHAR,
    source_field   VARCHAR,
    display_name   VARCHAR
);

-- Seed classification mappings (idempotent via MERGE)
MERGE INTO SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.client_app_classification AS tgt
USING (
    SELECT column1 AS priority, column2 AS pattern, column3 AS source_field, column4 AS display_name
    FROM VALUES
        (0, '%snowpark%', 'client_app_id', 'Snowpark'),
        (1, '%SNOWPARK%', 'application', 'Snowpark'),
        (2, '%deployments%', 'application', 'Kafka'),
        (3, '%cosmos%', 'application', 'COSMOS'),
        (4, '%rappid%', 'application', 'RAPPID'),
        (5, '%dtlk%', 'application', 'DTLK'),
        (6, '%nice%', 'application', 'NICE'),
        (7, '%nexis%', 'application', 'NEXIS'),
        (8, '%MASHUP%', 'application', 'Power BI'),
        (9, '%POWERBI%', 'application', 'Power BI'),
        (10, '%microsoftonprem%', 'application', 'Power BI'),
        (11, '%adbc%go%', 'application', 'ADBC-Go'),
        (12, '%DTS%', 'application', 'SSIS'),
        (13, '%DTEXEC%', 'application', 'SSIS'),
        (14, '%datastage%', 'application', 'IBM DataStage'),
        (15, '%REPORTSERVER%', 'application', 'SSRS/PBIRS'),
        (16, '%MSRS%', 'application', 'SSRS/PBIRS'),
        (17, '%REPORTINGSERVICE%', 'application', 'SSRS/PBIRS'),
        (18, '%REPORTBUILDER%', 'application', 'SSRS/PBIRS'),
        (19, '%VISUALSTUDIO%', 'application', 'SSRS/PBIRS'),
        (20, '%SQLSe%', 'application', 'SQL Server'),
        (21, '%GRAFANA%', 'application', 'Grafana'),
        (22, '%CIRRUS%', 'application', 'Cirrus CI'),
        (23, '%TOAD%', 'application', 'Toad'),
        (24, '%BOOTSTRAP%', 'application', 'Tomcat'),
        (25, '%QLIKREPL%', 'application', 'Qlik Replicate'),
        (26, '%rstudio%', 'application', 'RStudio'),
        (27, '%MicroStrat%', 'application', 'MicroStrategy'),
        (28, '%TABLEAU%', 'application', 'Tableau'),
        (29, '%HYPERION%', 'application', 'Hyperion'),
        (30, '%softoffice%', 'application', 'Microsoft Office'),
        (31, '%msacces%', 'application', 'Microsoft Access'),
        (32, '%DATABRICKS%', 'application', 'Databricks/Spark'),
        (33, '%dbatch%', 'application', 'Databricks/Spark'),
        (34, '%SPARK%', 'application', 'Databricks/Spark'),
        (35, '%ALTERYX%', 'application', 'Alteryx'),
        (36, '%INFA_DI%', 'application', 'Informatica Cloud'),
        (37, '%CDATA%', 'application', 'CData'),
        (38, '%fivetran%', 'application', 'Fivetran'),
        (39, '%tibco%', 'application', 'Tibco Spotfire'),
        (40, '%palantir%', 'application', 'Palantir'),
        (41, '%PERL%', 'application', 'Perl'),
        (42, '%iis%', 'application', 'Microsoft IIS'),
        (43, '%inets%', 'application', 'Microsoft IIS'),
        (44, '%w3wp%', 'application', 'Microsoft IIS'),
        (45, '%BUSINESSOBJECTS%', 'application', 'Business Objects'),
        (46, '%bobj%', 'application', 'Business Objects'),
        (47, '%DOMO%', 'application', 'Domo'),
        (48, '%astronomer%', 'application', 'Astronomer'),
        (49, '%DATAFACTORY%', 'application', 'Azure Data Factory'),
        (50, '%INTEGRATIONRUNTIME%', 'application', 'Azure Data Factory'),
        (51, '%EXCEL%', 'application', 'Excel'),
        (52, '%SNOWFLAKE%', 'application', 'Snowflake Web'),
        (53, '%JARVIS%', 'application', 'Jarvis'),
        (54, '%WEBJOBS%', 'application', 'Azure App Service/WebJobs'),
        (55, '%JENKINS%', 'application', 'Jenkins'),
        (56, '%KAFKA%', 'application', 'Kafka'),
        (57, '%airflow%', 'application', 'Airflow'),
        (58, '%starburst%', 'application', 'Starburst'),
        (59, '%prest%', 'application', 'Presto'),
        (60, '%boomi%', 'application', 'Boomi'),
        (61, '%SAS%', 'application', 'SAS'),
        (62, '%arcgis%', 'application', 'ArcGIS'),
        (63, '%dbeave%', 'application', 'DBeaver'),
        (64, '%vscode%', 'application', 'VSCode'),
        (65, '%teradata%', 'application', 'Teradata'),
        (66, '%powershell%', 'application', 'PowerShell'),
        (67, '%uipath%', 'application', 'UiPath'),
        (68, '%fads%', 'application', 'Fads'),
        (69, '%snowcli%', 'application', 'SnowCLI'),
        (70, '%intellij%', 'application', 'IntelliJ'),
        (71, '%sigma%', 'application', 'Sigma'),
        (72, '%talend%', 'application', 'Talend'),
        (73, '%thoughspot%', 'application', 'ThoughtSpot'),
        (74, '%install4j%', 'application', 'install4j'),
        (75, '%cognos%', 'application', 'Cognos'),
        (76, '%nimbus%', 'application', 'Nimbus'),
        (77, '%surefire%', 'application', 'Apache Maven Surefire'),
        (78, '%dataiku%', 'application', 'Dataiku'),
        (79, '%laserfiche%', 'application', 'Laserfiche'),
        (80, '%coalesce%', 'application', 'Coalesce'),
        (81, '%wherescape%', 'application', 'WhereScape'),
        (82, '%salesforce%', 'application', 'Salesforce'),
        (83, '%diffcheck%', 'application', 'Diffchecker'),
        (84, '%flyspeed%', 'application', 'FlySpeed SQL'),
        (85, '%AdvancedQuery%', 'application', 'AdvancedQueryTool'),
        (86, '%python%', 'application', 'Python'),
        (87, '%SNOWFLAKE%', 'client_app_id', 'Snowflake Web'),
        (88, '%JDBC%', 'client_app_id', 'JDBC'),
        (89, '%javascript%', 'client_app_id', 'Javascript')
) AS src
ON tgt.pattern = src.pattern AND tgt.source_field = src.source_field
WHEN MATCHED THEN UPDATE SET
    tgt.priority = src.priority,
    tgt.display_name = src.display_name
WHEN NOT MATCHED THEN INSERT (priority, pattern, source_field, display_name)
    VALUES (src.priority, src.pattern, src.source_field, src.display_name);

-- Create the refresh stored procedure
-- Now uses the client_app_classification lookup table instead of a CASE/WHEN block.
CREATE OR REPLACE PROCEDURE SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.REFRESH_DATA_LAKE_ACCESS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    TRUNCATE TABLE SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d;
    
    INSERT INTO SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d (
        organization_name, account_id, client, warehouse, 
        database, direction, access_count
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
            SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) AS database
        FROM snowflake.account_usage.sessions s
        INNER JOIN snowflake.account_usage.query_history q 
            ON q.session_id = s.session_id
        INNER JOIN snowflake.account_usage.access_history a 
            ON q.query_id = a.query_id,
        TABLE(FLATTEN(a.direct_objects_accessed)) t
        WHERE q.start_time > DATEADD(day, -30, CURRENT_DATE())
            AND q.query_type != 'CALL'
            AND s.client_application_id NOT LIKE 'SYSTEM%'
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
                PARTITION BY rs.query_id, rs.database
                ORDER BY c.priority ASC
            ) AS rn
        FROM raw_sessions rs
        LEFT JOIN SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.client_app_classification c
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
            database
        FROM classified
        WHERE rn = 1 OR display_name IS NULL
    )
    SELECT 
        organization_name,
        account_id, 
        client, 
        warehouse,
        database, 
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
            ELSE 'write'
        END AS direction, 
        COUNT(DISTINCT query_id) AS access_count
    FROM raw_access
    GROUP BY ALL;
    
    RETURN 'Data lake access data refreshed successfully';
END;
$$;

-- Create the refresh task that calls the stored procedure
-- NOTE: Update WAREHOUSE to your preferred warehouse before running
CREATE OR REPLACE TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 6 * * 0 America/Chicago'
  COMMENT = 'Refreshes data lake access data every Sunday at 6am CST'
AS
  CALL SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.REFRESH_DATA_LAKE_ACCESS();

-- Resume the task so it runs on schedule
ALTER TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK RESUME;

-- Execute the procedure immediately to populate initial data
CALL SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.REFRESH_DATA_LAKE_ACCESS();
