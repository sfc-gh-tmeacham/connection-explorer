#!/bin/bash
# =============================================================================
# Snowflake Connection Explorer - Uninstall Script
# 
# This script removes all objects created by the deployment:
#   - Streamlit app
#   - Task
#   - Stored procedure
#   - Table
#   - Stage
#   - Schema
#   - Database (optional)
#
# Prerequisites:
#   1. Snowflake CLI (snow) installed
#   2. Connection configured with role that owns the deployed objects
#
# Usage:
#   ./uninstall.sh <connection_name> [--keep-database]
#
# Options:
#   --keep-database    Keep the CONNECTION_EXPLORER_APP_DB database (only remove schema)
#
# Example:
#   ./uninstall.sh my_snowflake_connection
#   ./uninstall.sh my_snowflake_connection --keep-database
# =============================================================================

set -e  # Exit on error

CONNECTION=""
KEEP_DATABASE=false

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --keep-database)
            KEEP_DATABASE=true
            ;;
        --*)
            echo "Unknown option: $arg"
            exit 1
            ;;
        *)
            if [ -z "$CONNECTION" ]; then
                CONNECTION="$arg"
            fi
            ;;
    esac
done

if [ -z "$CONNECTION" ]; then
    CONNECTION="default"
fi

echo "=============================================="
echo "  Snowflake Connection Explorer - Uninstall"
echo "=============================================="
echo ""
echo "Connection:    $CONNECTION"
echo "Keep Database: $KEEP_DATABASE"
echo ""
echo "This will remove:"
echo "  - Streamlit app: SNOWFLAKE_CONNECTION_EXPLORER"
echo "  - Task: DATA_ACCESS_REFRESH_TASK"
echo "  - Procedure: REFRESH_CONNECTION_ACCESS"
echo "  - Table: CONNECTION_ACCESS_30D"
echo "  - Stage: STREAMLIT_STAGE"
echo "  - Schema: APP"
if [ "$KEEP_DATABASE" = false ]; then
    echo "  - Database: CONNECTION_EXPLORER_APP_DB"
fi
echo ""
read -p "Are you sure you want to continue? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstall cancelled."
    exit 0
fi

echo ""
echo "[1/4] Dropping Streamlit app..."
snow sql --connection "$CONNECTION" -q \
    "DROP STREAMLIT IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.SNOWFLAKE_CONNECTION_EXPLORER;"

echo ""
echo "[2/4] Suspending and dropping task..."
snow sql --connection "$CONNECTION" -q \
    "ALTER TASK IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.DATA_ACCESS_REFRESH_TASK SUSPEND;"
snow sql --connection "$CONNECTION" -q \
    "DROP TASK IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.DATA_ACCESS_REFRESH_TASK;"

echo ""
echo "[3/4] Dropping procedure, table, and stage..."
snow sql --connection "$CONNECTION" -q \
    "DROP PROCEDURE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.REFRESH_CONNECTION_ACCESS();"
snow sql --connection "$CONNECTION" -q \
    "DROP TABLE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.CONNECTION_ACCESS_30D;"
snow sql --connection "$CONNECTION" -q \
    "DROP STAGE IF EXISTS CONNECTION_EXPLORER_APP_DB.APP.STREAMLIT_STAGE;"

echo ""
if [ "$KEEP_DATABASE" = true ]; then
    echo "[4/4] Dropping schema (keeping database)..."
    snow sql --connection "$CONNECTION" -q \
        "DROP SCHEMA IF EXISTS CONNECTION_EXPLORER_APP_DB.APP;"
else
    echo "[4/4] Dropping database..."
    snow sql --connection "$CONNECTION" -q \
        "DROP DATABASE IF EXISTS CONNECTION_EXPLORER_APP_DB;"
fi

echo ""
echo "=============================================="
echo "  Uninstall Complete!"
echo "=============================================="
echo ""
echo "All Snowflake Connection Explorer objects have been removed."
echo ""
