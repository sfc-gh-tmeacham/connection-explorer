@echo off
REM =============================================================================
REM Snowflake Connection Explorer - Deployment Script (Windows)
REM 
REM Prerequisites:
REM   1. Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli
REM   2. Connection configured: snow connection add
REM   3. Role with required privileges (see snowflake_data_set_up.sql header)
REM
REM Usage:
REM   deploy.bat [connection_name]
REM
REM Example:
REM   deploy.bat my_snowflake_connection
REM =============================================================================

setlocal EnableDelayedExpansion

set CONNECTION=%1

if "%CONNECTION%"=="" set CONNECTION=default

echo ==============================================
echo   Snowflake Connection Explorer - Deployment
echo ==============================================
echo.
echo Connection: %CONNECTION%
echo.

REM Step 1: Run the setup SQL to create database, schema, table, and task
REM The SQL script creates its own warehouse (CONNECTION_EXPLORER_WH)
echo [1/3] Setting up database, schema, warehouse, and refresh task...
snow sql --connection %CONNECTION% --filename "%~dp0snowflake_data_set_up.sql" --warehouse CONNECTION_EXPLORER_WH
if errorlevel 1 (
    echo ERROR: Failed to run setup SQL. Please check your connection and permissions.
    exit /b 1
)

echo.
echo [2/3] Waiting for initial data load (this may take a few minutes)...
echo       The task is running to populate the 30-day access snapshot.
echo.

REM Step 2: Deploy the Streamlit app
echo [3/3] Deploying Streamlit app to Snowflake...
snow streamlit deploy --connection %CONNECTION% --database CONNECTION_EXPLORER_APP_DB --schema APP --replace
if errorlevel 1 (
    echo ERROR: Failed to deploy Streamlit app. Please check your connection and permissions.
    exit /b 1
)

REM Grant the app role access to the Streamlit app (must run after deploy creates it)
snow sql --connection %CONNECTION% --warehouse CONNECTION_EXPLORER_WH --query "GRANT USAGE ON STREAMLIT CONNECTION_EXPLORER_APP_DB.APP.SNOWFLAKE_CONNECTION_EXPLORER TO ROLE SYSADMIN"

echo.
echo ==============================================
echo   Deployment Complete!
echo ==============================================
echo.
echo Your Snowflake Connection Explorer is now available at:
echo   https://app.snowflake.com -^> Streamlit -^> SNOWFLAKE_CONNECTION_EXPLORER
echo.
echo Note: The refresh task runs every Sunday at 6am CST.
echo       Initial data may take a few minutes to populate.
echo.

endlocal
