@echo off
REM =============================================================================
REM Snowflake Connection Explorer - Uninstall Script (Windows)
REM 
REM This script removes all objects created by the deployment:
REM   - Streamlit app
REM   - Task
REM   - Stored procedure
REM   - Table
REM   - Stage
REM   - Schema
REM   - Database (optional)
REM
REM Prerequisites:
REM   1. Snowflake CLI (snow) installed
REM   2. Connection configured with role that owns the deployed objects
REM
REM Usage:
REM   uninstall.bat <connection_name> [--keep-database]
REM
REM Example:
REM   uninstall.bat my_snowflake_connection
REM   uninstall.bat my_snowflake_connection --keep-database
REM =============================================================================

setlocal EnableDelayedExpansion

set CONNECTION=
set KEEP_DATABASE=false

REM Parse arguments
:parse_args
if "%~1"=="" goto done_parsing
if "%~1"=="--keep-database" (
    set KEEP_DATABASE=true
    shift
    goto parse_args
)
if "%CONNECTION%"=="" (
    set CONNECTION=%~1
)
shift
goto parse_args

:done_parsing

if "%CONNECTION%"=="" set CONNECTION=default

echo ==============================================
echo   Snowflake Connection Explorer - Uninstall
echo ==============================================
echo.
echo Connection:    %CONNECTION%
echo Keep Database: %KEEP_DATABASE%
echo.
echo This will remove:
echo   - Streamlit app: SNOWFLAKE_CONNECTION_EXPLORER
echo   - Task: DATA_ACCESS_REFRESH_TASK
echo   - Procedure: REFRESH_DATA_LAKE_ACCESS
echo   - Table: data_lake_access_30d
echo   - Stage: STREAMLIT_STAGE
echo   - Schema: APP
if "%KEEP_DATABASE%"=="false" echo   - Database: CONNECTION_EXPLORER_APP_DB
echo.

set /p CONFIRM="Are you sure you want to continue? (y/N) "
if /i not "%CONFIRM%"=="y" (
    echo Uninstall cancelled.
    exit /b 0
)

echo.
echo [1/4] Dropping Streamlit app...
snow sql --connection %CONNECTION% -q "DROP STREAMLIT IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.SNOWFLAKE_CONNECTION_EXPLORER;"
if errorlevel 1 echo Warning: Could not drop Streamlit app

echo.
echo [2/4] Suspending and dropping task...
snow sql --connection %CONNECTION% -q "ALTER TASK IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.DATA_ACCESS_REFRESH_TASK SUSPEND;"
snow sql --connection %CONNECTION% -q "DROP TASK IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.DATA_ACCESS_REFRESH_TASK;"
if errorlevel 1 echo Warning: Could not drop task

echo.
echo [3/4] Dropping procedure, table, and stage...
snow sql --connection %CONNECTION% -q "DROP PROCEDURE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.REFRESH_DATA_LAKE_ACCESS();"
snow sql --connection %CONNECTION% -q "DROP TABLE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.data_lake_access_30d;"
snow sql --connection %CONNECTION% -q "DROP STAGE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.STREAMLIT_STAGE;"

echo.
if "%KEEP_DATABASE%"=="true" (
    echo [4/4] Dropping schema ^(keeping database^)...
    snow sql --connection %CONNECTION% -q "DROP SCHEMA IF EXISTS CONNECTION_EXPLORER_APP_DB.APP;"
) else (
    echo [4/4] Dropping database...
    snow sql --connection %CONNECTION% -q "DROP DATABASE IF EXISTS CONNECTION_EXPLORER_APP_DB;"
)

echo.
echo ==============================================
echo   Uninstall Complete!
echo ==============================================
echo.
echo All Snowflake Connection Explorer objects have been removed.
echo.

endl