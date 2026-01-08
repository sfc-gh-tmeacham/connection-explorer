@echo off
REM =============================================================================
REM Data Lake Explorer - Deployment Script (Windows)
REM 
REM Prerequisites:
REM   1. Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli
REM   2. Connection configured: snow connection add
REM   3. ACCOUNTADMIN role access
REM
REM Usage:
REM   deploy.bat [connection_name] [warehouse_name]
REM
REM Example:
REM   deploy.bat my_snowflake_connection COMPUTE_WH
REM =============================================================================

setlocal EnableDelayedExpansion

set CONNECTION=%1
set WAREHOUSE=%2

if "%CONNECTION%"=="" set CONNECTION=default
if "%WAREHOUSE%"=="" set WAREHOUSE=COMPUTE_WH

echo ==============================================
echo   Data Lake Explorer - Deployment
echo ==============================================
echo.
echo Connection: %CONNECTION%
echo Warehouse:  %WAREHOUSE%
echo.

REM Step 1: Run the setup SQL to create database, schema, table, and task
echo [1/3] Setting up database, schema, and refresh task...
snow sql --connection %CONNECTION% --filename snowflake_data_set_up.sql --role ACCOUNTADMIN --warehouse %WAREHOUSE%
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
snow streamlit deploy --connection %CONNECTION% --database SNOWFLAKE_DATA_LAKE --schema DATA_LAKE_ACCESS --replace
if errorlevel 1 (
    echo ERROR: Failed to deploy Streamlit app. Please check your connection and permissions.
    exit /b 1
)

echo.
echo ==============================================
echo   Deployment Complete!
echo ==============================================
echo.
echo Your Data Lake Explorer is now available at:
echo   https://app.snowflake.com -^> Streamlit -^> DATA_LAKE_EXPLORER
echo.
echo Note: The refresh task runs every Sunday at 6am CST.
echo       Initial data may take a few minutes to populate.
echo.

endlocal

