@echo off
REM =============================================================================
REM Snowflake Connection Explorer - Uninstall Script (Windows)
REM 
REM This script removes objects created by the deployment.
REM
REM By default it drops the app-level objects (Streamlit app, task, procedure,
REM tables, stage) but keeps the database and schema intact.
REM
REM Use flags to escalate:
REM   --drop-schema     Also drop the schema after removing objects
REM   --drop-database   Drop the entire database (implies --drop-schema)
REM
REM Prerequisites:
REM   1. Snowflake CLI (snow) installed
REM   2. Connection configured with role that owns the deployed objects
REM
REM Usage:
REM   uninstall.bat <connection_name> [--drop-schema] [--drop-database]
REM
REM Examples:
REM   uninstall.bat my_snowflake_connection                  # Objects only
REM   uninstall.bat my_snowflake_connection --drop-schema    # Objects + schema
REM   uninstall.bat my_snowflake_connection --drop-database  # Everything
REM =============================================================================

setlocal EnableDelayedExpansion

set CONNECTION=
set DROP_SCHEMA=false
set DROP_DATABASE=false

REM Parse arguments
:parse_args
if "%~1"=="" goto done_parsing
if "%~1"=="--drop-schema" (
    set DROP_SCHEMA=true
    shift
    goto parse_args
)
if "%~1"=="--drop-database" (
    set DROP_DATABASE=true
    set DROP_SCHEMA=true
    shift
    goto parse_args
)
if "%~1:~0,2%"=="--" (
    echo Unknown option: %~1
    echo Valid options: --drop-schema, --drop-database
    exit /b 1
)
if "%CONNECTION%"=="" (
    set CONNECTION=%~1
)
shift
goto parse_args

:done_parsing

if "%CONNECTION%"=="" set CONNECTION=default

REM Load configuration
set "CONF_FILE=%~dp0deploy.conf"
if not exist "%CONF_FILE%" (
    echo ERROR: deploy.conf not found in %~dp0
    echo.
    echo   To fix this, copy the example config and edit it for your environment:
    echo.
    echo     copy "%~dp0deploy.conf.example" "%~dp0deploy.conf"
    echo.
    echo   IMPORTANT: Values in deploy.conf must match the SET variables in
    echo   snowflake_data_set_up.sql. If you change one, update the other.
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%CONF_FILE%") do (
    REM Skip comment lines
    set "LINE=%%A"
    if not "!LINE:~0,1!"=="#" set "%%A=%%B"
)

echo ==============================================
echo   Snowflake Connection Explorer - Uninstall
echo ==============================================
echo.
echo   Connection : %CONNECTION%
echo   Database   : %DB_NAME%
echo   Schema     : %SCHEMA_NAME%
echo.
echo The following actions will be performed:
echo.
echo   DROP  Streamlit app   %DB_NAME%.%SCHEMA_NAME%.%APP_NAME%
echo   DROP  Task            %DB_NAME%.%SCHEMA_NAME%.DATA_ACCESS_REFRESH_TASK
echo   DROP  Procedure       %DB_NAME%.%SCHEMA_NAME%.REFRESH_CONNECTION_ACCESS^(^)
echo   DROP  Table           %DB_NAME%.%SCHEMA_NAME%.CONNECTION_ACCESS_30D
echo   DROP  Table           %DB_NAME%.%SCHEMA_NAME%.CLIENT_APP_CLASSIFICATION
echo   DROP  Stage           %DB_NAME%.%SCHEMA_NAME%.STREAMLIT_STAGE
if "%DROP_SCHEMA%"=="true" (
    echo   DROP  Schema          %DB_NAME%.%SCHEMA_NAME%
) else (
    echo   KEEP  Schema          %DB_NAME%.%SCHEMA_NAME%
)
if "%DROP_DATABASE%"=="true" (
    echo   DROP  Database        %DB_NAME%
) else (
    echo   KEEP  Database        %DB_NAME%
)
echo.

set /p CONFIRM="Continue with uninstall? (y/N) "
if /i not "%CONFIRM%"=="y" (
    echo Uninstall cancelled.
    exit /b 0
)

echo.
echo [1/3] Dropping Streamlit app...
snow sql --connection %CONNECTION% -q "DROP STREAMLIT IF EXISTS %DB_NAME%.%SCHEMA_NAME%.%APP_NAME%;"
if errorlevel 1 echo Warning: Could not drop Streamlit app

echo.
echo [2/3] Suspending and dropping task...
snow sql --connection %CONNECTION% -q "ALTER TASK IF EXISTS %DB_NAME%.%SCHEMA_NAME%.DATA_ACCESS_REFRESH_TASK SUSPEND;"
snow sql --connection %CONNECTION% -q "DROP TASK IF EXISTS %DB_NAME%.%SCHEMA_NAME%.DATA_ACCESS_REFRESH_TASK;"
if errorlevel 1 echo Warning: Could not drop task

echo.
echo [3/3] Dropping procedure, tables, and stage...
snow sql --connection %CONNECTION% -q "DROP PROCEDURE IF EXISTS %DB_NAME%.%SCHEMA_NAME%.REFRESH_CONNECTION_ACCESS();"
snow sql --connection %CONNECTION% -q "DROP TABLE IF EXISTS %DB_NAME%.%SCHEMA_NAME%.CONNECTION_ACCESS_30D;"
snow sql --connection %CONNECTION% -q "DROP TABLE IF EXISTS %DB_NAME%.%SCHEMA_NAME%.CLIENT_APP_CLASSIFICATION;"
snow sql --connection %CONNECTION% -q "DROP STAGE IF EXISTS %DB_NAME%.%SCHEMA_NAME%.STREAMLIT_STAGE;"

if "%DROP_SCHEMA%"=="true" (
    echo.
    echo Dropping schema %DB_NAME%.%SCHEMA_NAME%...
    snow sql --connection %CONNECTION% -q "DROP SCHEMA IF EXISTS %DB_NAME%.%SCHEMA_NAME%;"
)

if "%DROP_DATABASE%"=="true" (
    echo.
    echo Dropping database %DB_NAME%...
    snow sql --connection %CONNECTION% -q "DROP DATABASE IF EXISTS %DB_NAME%;"
)

echo.
echo ==============================================
echo   Uninstall Complete!
echo ==============================================
echo.
echo Removed objects from %DB_NAME%.%SCHEMA_NAME%.
if "%DROP_SCHEMA%"=="true" echo Schema %SCHEMA_NAME% was dropped.
if "%DROP_DATABASE%"=="true" echo Database %DB_NAME% was dropped.
echo.

endlocal
