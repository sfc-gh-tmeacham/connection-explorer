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
    set "line=%%A"
    if not "!line:~0,1!"=="#" (
        set "%%A=%%B"
    )
)

echo ==============================================
echo   Snowflake Connection Explorer - Deployment
echo ==============================================
echo.
echo Connection: %CONNECTION%
echo Database:   %DB_NAME%
echo Schema:     %SCHEMA_NAME%
echo Warehouse:  %WH_NAME%
echo.

REM Step 1: Run the setup SQL to create database, schema, table, and task
echo [1/3] Setting up database, schema, warehouse, and refresh task...
snow sql --connection %CONNECTION% --filename "%~dp0snowflake_data_set_up.sql" --warehouse %WH_NAME%
if errorlevel 1 (
    echo ERROR: Failed to run setup SQL. Please check your connection and permissions.
    exit /b 1
)

echo.

REM Step 2: Deploy the Streamlit app
echo [2/3] Deploying Streamlit app to Snowflake...
snow streamlit deploy --connection %CONNECTION% --database %DB_NAME% --schema %SCHEMA_NAME% --replace
if errorlevel 1 (
    echo ERROR: Failed to deploy Streamlit app. Please check your connection and permissions.
    exit /b 1
)

REM Step 3: Grant the app role access to the Streamlit app (must run after deploy creates it)
echo.
echo [3/3] Granting %APP_OWNER_ROLE% access to the Streamlit app...
snow sql --connection %CONNECTION% --warehouse %WH_NAME% --query "GRANT USAGE ON STREAMLIT %DB_NAME%.%SCHEMA_NAME%.%APP_NAME% TO ROLE %APP_OWNER_ROLE%"

echo.
echo ==============================================
echo   Deployment Complete!
echo ==============================================
echo.
echo Your Snowflake Connection Explorer is now available at:
echo   https://app.snowflake.com -^> Streamlit -^> %APP_NAME%
echo.
echo Note: The refresh task runs every Sunday at 6am CST.
echo       Initial data may take a few minutes to populate.
echo.

endlocal
