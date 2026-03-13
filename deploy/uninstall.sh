#!/bin/bash
# =============================================================================
# Snowflake Connection Explorer - Uninstall Script
# 
# This script removes objects created by the deployment.
#
# By default it drops the app-level objects (Streamlit app, task, procedure,
# tables, stage) but keeps the database and schema intact.
#
# Use flags to escalate:
#   --drop-schema     Also drop the schema after removing objects
#   --drop-database   Drop the entire database (implies --drop-schema)
#
# Prerequisites:
#   1. Snowflake CLI (snow) installed
#   2. Connection configured with role that owns the deployed objects
#
# Usage:
#   ./uninstall.sh <connection_name> [--drop-schema] [--drop-database]
#
# Examples:
#   ./uninstall.sh my_snowflake_connection                  # Objects only
#   ./uninstall.sh my_snowflake_connection --drop-schema    # Objects + schema
#   ./uninstall.sh my_snowflake_connection --drop-database  # Everything
# =============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTION=""
DROP_SCHEMA=false
DROP_DATABASE=false

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --drop-schema)
            DROP_SCHEMA=true
            ;;
        --drop-database)
            DROP_DATABASE=true
            DROP_SCHEMA=true  # implied
            ;;
        --*)
            echo "Unknown option: $arg"
            echo "Valid options: --drop-schema, --drop-database"
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

# Load configuration
if [ ! -f "$SCRIPT_DIR/deploy.conf" ]; then
    echo "ERROR: deploy.conf not found in $SCRIPT_DIR"
    echo ""
    echo "  To fix this, copy the example config and edit it for your environment:"
    echo ""
    echo "    cp $SCRIPT_DIR/deploy.conf.example $SCRIPT_DIR/deploy.conf"
    echo ""
    echo "  IMPORTANT: Values in deploy.conf must match the SET variables in"
    echo "  snowflake_data_set_up.sql. If you change one, update the other."
    exit 1
fi
# shellcheck source=deploy.conf.example
source "$SCRIPT_DIR/deploy.conf"

echo "=============================================="
echo "  Snowflake Connection Explorer - Uninstall"
echo "=============================================="
echo ""
echo "  Connection : $CONNECTION"
echo "  Database   : $DB_NAME"
echo "  Schema     : $SCHEMA_NAME"
echo ""
echo "The following actions will be performed:"
echo ""
echo "  DROP  Streamlit app   ${DB_NAME}.${SCHEMA_NAME}.${APP_NAME}"
echo "  DROP  Task            ${DB_NAME}.${SCHEMA_NAME}.DATA_ACCESS_REFRESH_TASK"
echo "  DROP  Procedure       ${DB_NAME}.${SCHEMA_NAME}.REFRESH_CONNECTION_ACCESS()"
echo "  DROP  Table           ${DB_NAME}.${SCHEMA_NAME}.CONNECTION_ACCESS_30D"
echo "  DROP  Table           ${DB_NAME}.${SCHEMA_NAME}.CLIENT_APP_CLASSIFICATION"
echo "  DROP  Stage           ${DB_NAME}.${SCHEMA_NAME}.STREAMLIT_STAGE"
if [ "$DROP_SCHEMA" = true ]; then
    echo "  DROP  Schema          ${DB_NAME}.${SCHEMA_NAME}"
else
    echo "  KEEP  Schema          ${DB_NAME}.${SCHEMA_NAME}"
fi
if [ "$DROP_DATABASE" = true ]; then
    echo "  DROP  Database        ${DB_NAME}"
else
    echo "  KEEP  Database        ${DB_NAME}"
fi
echo ""

read -p "Continue with uninstall? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Uninstall cancelled."
    exit 0
fi

echo ""
echo "[1/3] Dropping Streamlit app..."
snow sql --connection "$CONNECTION" -q \
    "DROP STREAMLIT IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.${APP_NAME};"

echo ""
echo "[2/3] Suspending and dropping task..."
snow sql --connection "$CONNECTION" -q \
    "ALTER TASK IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.DATA_ACCESS_REFRESH_TASK SUSPEND;"
snow sql --connection "$CONNECTION" -q \
    "DROP TASK IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.DATA_ACCESS_REFRESH_TASK;"

echo ""
echo "[3/3] Dropping procedure, tables, and stage..."
snow sql --connection "$CONNECTION" -q \
    "DROP PROCEDURE IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.REFRESH_CONNECTION_ACCESS();"
snow sql --connection "$CONNECTION" -q \
    "DROP TABLE IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.CONNECTION_ACCESS_30D;"
snow sql --connection "$CONNECTION" -q \
    "DROP TABLE IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.CLIENT_APP_CLASSIFICATION;"
snow sql --connection "$CONNECTION" -q \
    "DROP STAGE IF EXISTS ${DB_NAME}.${SCHEMA_NAME}.STREAMLIT_STAGE;"

if [ "$DROP_SCHEMA" = true ]; then
    echo ""
    echo "Dropping schema ${DB_NAME}.${SCHEMA_NAME}..."
    snow sql --connection "$CONNECTION" -q \
        "DROP SCHEMA IF EXISTS ${DB_NAME}.${SCHEMA_NAME};"
fi

if [ "$DROP_DATABASE" = true ]; then
    echo ""
    echo "Dropping database ${DB_NAME}..."
    snow sql --connection "$CONNECTION" -q \
        "DROP DATABASE IF EXISTS ${DB_NAME};"
fi

echo ""
echo "=============================================="
echo "  Uninstall Complete!"
echo "=============================================="
echo ""
echo "Removed objects from ${DB_NAME}.${SCHEMA_NAME}."
if [ "$DROP_SCHEMA" = true ]; then
    echo "Schema ${SCHEMA_NAME} was dropped."
fi
if [ "$DROP_DATABASE" = true ]; then
    echo "Database ${DB_NAME} was dropped."
fi
echo ""
