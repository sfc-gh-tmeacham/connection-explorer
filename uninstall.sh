#!/bin/bash
# =============================================================================
# Snowflake Data Lake Explorer - Uninstall Script
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
#   2. Connection configured with ACCOUNTADMIN role access
#
# Usage:
#   ./uninstall.sh [connection_name] [--keep-database]
#
# Options:
#   --keep-database    Keep the SNOWFLAKE_DATA_LAKE database (only remove schema)
#
# Example:
#   ./uninstall.sh my_snowflake_connection
#   ./uninstall.sh my_snowflake_connection --keep-database
# =============================================================================

set -e  # Exit on error

CONNECTION=${1:-"default"}
KEEP_DATABASE=false

# Check for --keep-database flag
for arg in "$@"; do
    if [ "$arg" == "--keep-database" ]; then
        KEEP_DATABASE=true
    fi
done

echo "=============================================="
echo "  Snowflake Data Lake Explorer - Uninstall"
echo "=============================================="
echo ""
echo "Connection: $CONNECTION"
echo "Keep Database: $KEEP_DATABASE"
echo ""
echo "This will remove:"
echo "  - Streamlit app: SNOWFLAKE_DATA_LAKE_EXPLORER"
echo "  - Task: DATA_LAKE_ACCESS_REFRESH_TASK"
echo "  - Procedure: REFRESH_DATA_LAKE_ACCESS"
echo "  - Table: data_lake_access_30d"
echo "  - Stage: STREAMLIT_STAGE"
echo "  - Schema: DATA_LAKE_ACCESS"
if [ "$KEEP_DATABASE" = false ]; then
    echo "  - Database: SNOWFLAKE_DATA_LAKE"
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
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "DROP STREAMLIT IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.SNOWFLAKE_DATA_LAKE_EXPLORER;"

echo ""
echo "[2/4] Suspending and dropping task..."
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "ALTER TASK IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK SUSPEND;"
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "DROP TASK IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK;"

echo ""
echo "[3/4] Dropping procedure, table, and stage..."
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "DROP PROCEDURE IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.REFRESH_DATA_LAKE_ACCESS();"
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "DROP TABLE IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d;"
snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
    "DROP STAGE IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.STREAMLIT_STAGE;"

echo ""
if [ "$KEEP_DATABASE" = true ]; then
    echo "[4/4] Dropping schema (keeping database)..."
    snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
        "DROP SCHEMA IF EXISTS SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS;"
else
    echo "[4/4] Dropping database..."
    snow sql --connection "$CONNECTION" --role ACCOUNTADMIN -q \
        "DROP DATABASE IF EXISTS SNOWFLAKE_DATA_LAKE;"
fi

echo ""
echo "=============================================="
echo "  Uninstall Complete!"
echo "=============================================="
echo ""
echo "All Snowflake Data Lake Explorer objects have been removed."
echo ""

