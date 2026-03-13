#!/bin/bash
# =============================================================================
# Snowflake Connection Explorer - Deployment Script
# 
# Prerequisites:
#   1. Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli
#   2. Connection configured: snow connection add
#   3. Role with required privileges (see snowflake_data_set_up.sql header)
#
# Usage:
#   ./deploy.sh [connection_name]
#
# Example:
#   ./deploy.sh my_snowflake_connection
# =============================================================================

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONNECTION=${1:-"default"}

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
echo "  Snowflake Connection Explorer - Deployment"
echo "=============================================="
echo ""
echo "Connection: $CONNECTION"
echo "Database:   $DB_NAME"
echo "Schema:     $SCHEMA_NAME"
echo "Warehouse:  $WH_NAME"
echo ""

# Step 1: Run the setup SQL to create database, schema, table, and task
# The SQL script creates its own warehouse
echo "[1/3] Setting up database, schema, warehouse, and refresh task..."
snow sql \
    --connection "$CONNECTION" \
    --filename "$SCRIPT_DIR/snowflake_data_set_up.sql" \
    --warehouse "$WH_NAME"

echo ""

# Step 2: Deploy the Streamlit app
echo "[2/3] Deploying Streamlit app to Snowflake..."
snow streamlit deploy \
    --connection "$CONNECTION" \
    --database "$DB_NAME" \
    --schema "$SCHEMA_NAME" \
    --replace

# Grant the app role access to the Streamlit app (must run after deploy creates it)
echo ""
echo "[3/3] Granting $APP_OWNER_ROLE access to the Streamlit app..."
snow sql \
    --connection "$CONNECTION" \
    --warehouse "$WH_NAME" \
    --query "GRANT USAGE ON STREAMLIT ${DB_NAME}.${SCHEMA_NAME}.${APP_NAME} TO ROLE ${APP_OWNER_ROLE}"

echo ""
echo "=============================================="
echo "  Deployment Complete!"
echo "=============================================="
echo ""
echo "Your Snowflake Connection Explorer is now available at:"
echo "  https://app.snowflake.com → Streamlit → $APP_NAME"
echo ""
echo "Note: The refresh task runs every Sunday at 6am CST."
echo "      Initial data may take a few minutes to populate."
echo ""

