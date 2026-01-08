-- =============================================================================
-- Data Lake Access Setup Script
-- Requires ACCOUNTADMIN role to run
-- =============================================================================

USE ROLE ACCOUNTADMIN;

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

-- Create the refresh task
-- NOTE: Update WAREHOUSE to your preferred warehouse before running
CREATE OR REPLACE TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 6 * * 0 America/Chicago'
  COMMENT = 'Refreshes data lake access data every Sunday at 6am CST'
AS
BEGIN
    TRUNCATE TABLE SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d;
    
    INSERT INTO SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d (
        organization_name, account_id, client, warehouse, 
        database, direction, access_count
    )
    WITH raw_access AS (
        SELECT
            CURRENT_ORGANIZATION_NAME() as organization_name,
            q.query_text,
            CURRENT_ACCOUNT() as account_id,
            s.client_application_id as CLIENT_APP_ID,
            PARSE_JSON(s.client_environment):APPLICATION::VARCHAR AS application,
            case when CLIENT_APP_ID ilike '%snowpark%' then 'Snowpark'
            when application ilike '%SNOWPARK%' then 'Snowpark'
            when application ilike '%deployments%' then 'Kafka'
            when application ilike '%cosmos%' then 'COSMOS'
            when application ilike '%rappid%' then 'RAPPID'
            when application ilike '%dtlk%' then 'DTLK'
            when application ilike '%nice%' then 'NICE'
            when application ilike '%nexis%' then 'NEXIS'
            when application ilike '%MASHUP%' then 'Power BI' 
            when application ilike '%POWERBI%' then 'Power BI' 
            when application ilike '%microsoftonprem%' then 'Power BI'
            when application ilike '%adbc%go%' then 'ADBC-Go'
            when application ilike '%DTS%' then 'SSIS'
            when application ilike '%DTEXEC%' then 'SSIS'
            when application ilike '%datastage%' then 'IBM DataStage'
            when application ilike '%REPORTSERVER%' then 'SSRS/PBIRS'
            when application ilike '%MSRS%' then 'SSRS/PBIRS'
            when application ilike '%REPORTINGSERVICE%' then 'SSRS/PBIRS'
            when application ilike '%REPORTBUILDER%' then 'SSRS/PBIRS'
            when application ilike '%VISUALSTUDIO%' then 'SSRS/PBIRS'
            when application ilike '%SQLSe%' then 'SQL Server'
            when application ilike '%GRAFANA%' then 'Grafana'
            when application ilike '%CIRRUS%' then 'Cirrus CI'
            when application ilike '%TOAD%' then 'Toad'
            when application ilike '%BOOTSTRAP%' then 'Tomcat'
            when application ilike '%QLIKREPL%' then 'Qlik Replicate'
            when application ilike '%rstudio%' then 'RStudio'
            when application ilike '%MicroStrat%' then 'MicroStrategy'
            when application ilike '%TABLEAU%' then 'Tableau'
            when application ilike '%HYPERION%' then 'Hyperion'
            when application ilike '%softoffice%' then 'Microsoft Office'
            when application ilike '%msacces%' then 'Microsoft Access'
            when application ilike '%DATABRICKS%' then 'Databricks/Spark'
            when application ilike '%dbatch%' then 'Databricks/Spark'
            when application ilike '%SPARK%' then 'Databricks/Spark'
            when application ilike '%ALTERYX%' then 'Alteryx'
            when application ilike '%INFA_DI%' then 'Informatica Cloud'
            when application ilike '%CDATA%' then 'CData'
            when application ilike '%fivetran%' then 'Fivetran'
            when application ilike '%tibco%' then 'Tibco Spotfire'
            when application ilike '%palantir%' then 'Palantir'
            when application ilike '%PERL%' then 'Perl'
            when application ilike '%iis%' then 'Microsoft IIS'
            when application ilike '%inets%' then 'Microsoft IIS'
            when application ilike '%w3wp%' then 'Microsoft IIS'
            when application ilike '%BUSINESSOBJECTS%' then 'Business Objects'
            when application ilike '%bobj%' then 'Business Objects'
            when application ilike '%DOMO%' then 'Domo'
            when application ilike '%astronomer%' then 'Astronomer'
            when application ilike '%DATAFACTORY%' then 'Azure Data Factory'
            when application ilike '%INTEGRATIONRUNTIME%' then 'Azure Data Factory'
            when application ilike '%EXCEL%' then 'Excel'
            when application ilike '%SNOWFLAKE%' then 'Snowflake Web'
            when application ilike '%JARVIS%' then 'Jarvis'
            when application ilike '%WEBJOBS%' then 'Azure App Service/WebJobs'
            when application ilike '%JENKINS%' then 'Jenkins'
            when application ilike '%KAFKA%' then 'Kafka'
            when application ilike '%airflow%' then 'Airflow'
            when application ilike '%starburst%' then 'Starburst'
            when application ilike '%prest%' then 'Presto'
            when application ilike '%boomi%' then 'Boomi'
            when application ilike '%SAS%' then 'SAS'
            when application ilike '%arcgis%' then 'ArcGIS'
            when application ilike '%dbeave%' then 'DBeaver'
            when application ilike '%vscode%' then 'VSCode'
            when application ilike '%teradata%' then 'Teradata'
            when application ilike '%powershell%' then 'PowerShell'
            when application ilike '%uipath%' then 'UiPath'
            when application ilike '%fads%' then 'Fads'
            when application ilike '%snowcli%' then 'SnowCLI'
            when application ilike '%intellij%' then 'IntelliJ'
            when application ilike '%sigma%' then 'Sigma'
            when application ilike '%talend%' then 'Talend'
            when application ilike '%thoughspot%' then 'ThoughSpot'
            when application ilike '%install4j%' then 'install4j'
            when application ilike '%cognos%' then 'Cognos'
            when application ilike '%nimbus%' then 'Nimbus'
            when application ilike '%surefire%' then 'Apache Maven Surefire'
            when application ilike '%dataiku%' then 'Dataiku'
            when application ilike '%laserfiche%' then 'Laserfiche'
            when application ilike '%coalesce%' then 'Coalesce'
            when application ilike '%wherescape%' then 'WhereScape'
            when application ilike '%salesforce%' then 'Salesforce'
            when application ilike '%diffcheck%' then 'Diffchecker'
            when application ilike '%flyspeed%' then 'FlySpeed SQL'
            when application ilike '%AdvancedQuery%' then 'AdvancedQueryTool' 
            when application IS NULL then case when upper(CLIENT_APP_ID) like '%SNOWFLAKE%' then 'Snowflake Web' 
            when upper(CLIENT_APP_ID) like '%JDBC%' then 'JDBC' || ifnull(application,'')
            when CLIENT_APP_ID ilike '%javascript%' then 'Javascript' || ifnull(application,'')
            else CLIENT_APP_ID || ifnull(application,'') end
            when CLIENT_APP_ID like '%JDBC%' then 'JDBC:' || application
            when application ilike '%python%' then 'Python'  
            else application end as CLIENT,
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
    )
    SELECT 
        organization_name,
        account_id, 
        client, 
        warehouse,
        database, 
        CASE
            WHEN r.query_type LIKE 'CREATE%' 
                 OR r.query_type LIKE 'ALTER%' 
                 OR r.query_type LIKE 'DROP%' 
                 OR r.query_type LIKE 'TRUNCATE%' 
                 OR r.query_type LIKE 'RENAME%' 
                 OR r.query_type LIKE 'UNDROP%'
                 OR r.query_type = 'COMMENT' 
            THEN 'DDL'
            WHEN r.query_type LIKE 'SHOW%' OR r.query_type LIKE 'DESCRIBE%' OR r.query_type IN ('DESC', 'LIST_FILES', 'EXPLAIN') THEN 'metadata'
            WHEN r.query_type IN ('SELECT', 'UNLOAD', 'GET_FILES') THEN 'read'
            ELSE 'write'
        END AS direction, 
        count(distinct query_id) as access_count
    FROM raw_access r
    GROUP BY ALL;
END;

-- Resume the task so it runs on schedule
ALTER TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK RESUME;

-- Execute the task immediately to populate initial data
EXECUTE TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK;
