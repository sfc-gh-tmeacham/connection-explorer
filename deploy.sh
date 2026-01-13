#!/bin/bash
# =============================================================================
# Data Lake Explorer - Deployment Script
# 
# Prerequisites:
#   1. Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli
#   2. Connection configured: snow connection add
#   3. Role with required privileges (see snowflake_data_set_up.sql header)
#
# Usage:
#   ./deploy.sh <connection_name> [warehouse_name]
#
# Example:
#   ./deploy.sh my_snowflake_connection COMPUTE_WH
# =============================================================================

set -e  # Exit on error

CONNECTION=${1:-"default"}
WAREHOUSE=${2:-"COMPUTE_WH"}

echo "=============================================="
echo "  Data Lake Explorer - Deployment"
echo "=============================================="
echo ""
echo "Connection: $CONNECTION"
echo "Warehouse:  $WAREHOUSE"
echo ""

# Step 1: Run the setup SQL to create database, schema, table, and task
echo "[1/3] Setting up database, schema, and refresh task..."
snow sql \
    --connection "$CONNECTION" \
    --filename snowflake_data_set_up.sql \
    --warehouse "$WAREHOUSE"

echo ""
echo "[2/3] Waiting for initial data load (this may take a few minutes)..."
echo "      The task is running to populate the 30-day access snapshot."
echo ""

# Step 2: Deploy the Streamlit app
echo "[3/3] Deploying Streamlit app to Snowflake..."
snow streamlit deploy \
    --connection "$CONNECTION" \
    --database SNOWFLAKE_DATA_LAKE \
    --schema DATA_LAKE_ACCESS \
    --replace

echo ""
echo "=============================================="
echo "  Deployment Complete!"
echo "=============================================="
echo ""
echo "Your Data Lake Explorer is now available at:"
echo "  https://app.snowflake.com → Streamlit → DATA_LAKE_EXPLORER"
echo ""
echo "Note: The refresh task runs every Sunday at 6am CST."
echo "      Initial data may take a few minutes to populate."
echo ""

