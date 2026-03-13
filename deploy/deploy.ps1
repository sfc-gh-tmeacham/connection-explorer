# =============================================================================
# Snowflake Connection Explorer - Deployment Script (PowerShell)
#
# Prerequisites:
#   1. Snowflake CLI (snow) installed: https://docs.snowflake.com/en/developer-guide/snowflake-cli
#   2. Connection configured: snow connection add
#   3. Role with required privileges (see snowflake_data_set_up.sql header)
#      - ACCOUNTADMIN (or a role with CREATE DATABASE, CREATE WAREHOUSE,
#        CREATE COMPUTE POOL, EXECUTE MANAGED TASK ON ACCOUNT)
#      - IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE (for account_usage views)
#
# Usage:
#   .\deploy.ps1 [-Connection <name>]
#
# Examples:
#   .\deploy.ps1                              # Uses "default" connection
#   .\deploy.ps1 -Connection my_sf_conn       # Uses named connection
# =============================================================================

param(
    [string]$Connection = "default"
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
function Write-Step {
    param([string]$Step, [string]$Message)
    Write-Host "[$Step] $Message" -ForegroundColor Cyan
}

function Write-Success {
    param([string]$Message)
    Write-Host "  OK  $Message" -ForegroundColor Green
}

function Write-Detail {
    param([string]$Message)
    Write-Host "      $Message" -ForegroundColor DarkGray
}

function Write-Err {
    param([string]$Message)
    Write-Host "  ERROR  $Message" -ForegroundColor Red
}

function Write-Remediation {
    param([string[]]$Steps)
    Write-Host ""
    Write-Host "  Remediation steps:" -ForegroundColor Yellow
    $i = 1
    foreach ($s in $Steps) {
        Write-Host "    $i. $s" -ForegroundColor Yellow
        $i++
    }
    Write-Host ""
}

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
$confPath = Join-Path $PSScriptRoot "deploy.conf"

if (-not (Test-Path $confPath)) {
    Write-Err "deploy.conf not found in $PSScriptRoot"
    Write-Host ""
    Write-Host "  To fix this, copy the example config and edit it for your environment:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "    Copy-Item `"$(Join-Path $PSScriptRoot 'deploy.conf.example')`" `"$confPath`"" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  IMPORTANT: Values in deploy.conf must match the SET variables in" -ForegroundColor Yellow
    Write-Host "  snowflake_data_set_up.sql. If you change one, update the other." -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

# Parse key=value pairs (skip comments and blank lines)
Get-Content $confPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith('#')) {
        $parts = $line -split '=', 2
        if ($parts.Count -eq 2) {
            Set-Variable -Name $parts[0].Trim() -Value $parts[1].Trim() -Scope Script
        }
    }
}

# Derived fully-qualified names
$FQ_SCHEMA = "${DB_NAME}.${SCHEMA_NAME}"
$FQ_APP = "${DB_NAME}.${SCHEMA_NAME}.${APP_NAME}"

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "=============================================="
Write-Host "  Snowflake Connection Explorer - Deployment"
Write-Host "=============================================="
Write-Host ""
Write-Host "  Connection : $Connection"
Write-Host "  Config     : $confPath"
Write-Host "  Database   : $DB_NAME"
Write-Host "  Schema     : $SCHEMA_NAME"
Write-Host "  Warehouse  : $WH_NAME"
Write-Host "  App Name   : $APP_NAME"
Write-Host "  Timestamp  : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host ""

# -- Check Snowflake CLI is installed
Write-Step "0/3" "Running pre-flight checks..."

$snowCmd = Get-Command snow -ErrorAction SilentlyContinue
if (-not $snowCmd) {
    Write-Err "Snowflake CLI (snow) is not installed or not on PATH."
    Write-Remediation @(
        "Install the Snowflake CLI: https://docs.snowflake.com/en/developer-guide/snowflake-cli/installation",
        "Verify installation with: snow --version",
        "Ensure the install directory is in your PATH environment variable."
    )
    exit 1
}
Write-Success "Snowflake CLI found: $($snowCmd.Source)"

# -- Check the SQL setup file exists
$sqlFile = Join-Path $PSScriptRoot "snowflake_data_set_up.sql"
if (-not (Test-Path $sqlFile)) {
    Write-Err "Setup SQL file not found at: $sqlFile"
    Write-Remediation @(
        "Ensure snowflake_data_set_up.sql exists in the same directory as this script.",
        "If you moved deploy.ps1, copy the SQL file alongside it."
    )
    exit 1
}
Write-Success "Setup SQL file found: $sqlFile"

# -- Validate the connection by running a lightweight query
Write-Detail "Validating connection '$Connection'..."
try {
    $connTest = snow sql --connection $Connection --query "SELECT CURRENT_ROLE() AS ROLE, CURRENT_ACCOUNT() AS ACCOUNT" 2>&1
    if ($LASTEXITCODE -ne 0) { throw "snow sql returned exit code $LASTEXITCODE" }
    Write-Success "Connection '$Connection' is valid."
    Write-Detail ($connTest | Out-String).Trim()
} catch {
    Write-Err "Cannot connect to Snowflake using connection '$Connection'."
    Write-Detail "Error: $_"
    Write-Remediation @(
        "List available connections: snow connection list",
        "Add a new connection:      snow connection add",
        "Test connectivity:         snow sql --connection $Connection --query ""SELECT 1""",
        "If using SSO, ensure your browser can open for authentication.",
        "Check ~/.snowflake/connections.toml for typos in the connection config."
    )
    exit 1
}
Write-Host ""

# ---------------------------------------------------------------------------
# Step 1: Run setup SQL
# ---------------------------------------------------------------------------
Write-Step "1/3" "Setting up database, schema, warehouse, compute pool, and refresh task..."
Write-Detail "Executing: $sqlFile"
Write-Detail "This creates $DB_NAME, $WH_NAME,"
Write-Detail "$COMPUTE_POOL_NAME, tables, stored procedure, and scheduled task."
Write-Host ""

try {
    snow sql --connection $Connection --filename $sqlFile --warehouse $WH_NAME 2>&1 | ForEach-Object {
        Write-Detail $_
    }
    if ($LASTEXITCODE -ne 0) { throw "snow sql returned exit code $LASTEXITCODE" }
    Write-Success "Database setup completed."
} catch {
    Write-Err "Failed to execute setup SQL."
    Write-Detail "Error: $_"

    $errMsg = $_.ToString()
    $remediations = @()

    if ($errMsg -match "(?i)insufficient privileges|access denied|not authorized") {
        $remediations += "Ensure you are using a role with ACCOUNTADMIN privileges (or equivalent)."
        $remediations += "Required privileges: CREATE DATABASE, CREATE WAREHOUSE, CREATE COMPUTE POOL,"
        $remediations += "  EXECUTE MANAGED TASK ON ACCOUNT, IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE."
        $remediations += "Check your active role: snow sql --connection $Connection --query ""SELECT CURRENT_ROLE()"""
    }
    elseif ($errMsg -match "(?i)object.*already exists") {
        $remediations += "Some objects may already exist from a previous deployment."
        $remediations += "The script uses IF NOT EXISTS / CREATE OR ALTER, so this is usually safe."
        $remediations += "If an object was manually altered, try dropping and redeploying."
    }
    elseif ($errMsg -match "(?i)warehouse.*does not exist|invalid warehouse") {
        $remediations += "The script creates warehouse $WH_NAME — it may have been dropped."
        $remediations += "Manually create it: CREATE WAREHOUSE $WH_NAME WAREHOUSE_SIZE='MEDIUM';"
        $remediations += "Or run the first part of snowflake_data_set_up.sql manually to create the warehouse."
    }
    elseif ($errMsg -match "(?i)network|timeout|connection refused|socket") {
        $remediations += "Check your network connection and any VPN/proxy configuration."
        $remediations += "Verify the Snowflake account URL is reachable from this machine."
        $remediations += "If behind a corporate firewall, ensure *.snowflakecomputing.com is allowed."
    }
    else {
        $remediations += "Review the full error output above for details."
        $remediations += "Run the SQL file manually to isolate the failing statement:"
        $remediations += "  snow sql --connection $Connection --filename $sqlFile --warehouse $WH_NAME"
        $remediations += "Check the Snowflake query history for more details on the failure."
    }

    Write-Remediation $remediations
    exit 1
}
Write-Host ""

# ---------------------------------------------------------------------------
# Step 2: Deploy Streamlit app
# ---------------------------------------------------------------------------
Write-Step "2/3" "Deploying Streamlit app to Snowflake..."
Write-Detail "Target: $FQ_SCHEMA"
Write-Host ""

try {
    snow streamlit deploy --connection $Connection --database $DB_NAME --schema $SCHEMA_NAME --replace 2>&1 | ForEach-Object {
        Write-Detail $_
    }
    if ($LASTEXITCODE -ne 0) { throw "snow streamlit deploy returned exit code $LASTEXITCODE" }
    Write-Success "Streamlit app deployed."
} catch {
    Write-Err "Failed to deploy Streamlit app."
    Write-Detail "Error: $_"

    $errMsg = $_.ToString()
    $remediations = @()

    if ($errMsg -match "(?i)snowflake\.yml|snowflake\.local\.yml|project definition|not found") {
        $remediations += "Ensure snowflake.yml (or snowflake.local.yml) exists in the project root."
        $remediations += "The file should define the Streamlit app under the 'streamlit' section."
        $remediations += "See: https://docs.snowflake.com/en/developer-guide/snowflake-cli/streamlit-apps/overview"
    }
    elseif ($errMsg -match "(?i)stage.*not found|stage.*does not exist") {
        $remediations += "The internal stage STREAMLIT_STAGE may not exist yet."
        $remediations += "Ensure step 1 (setup SQL) completed successfully — it creates the stage."
        $remediations += "Manually create it: CREATE STAGE IF NOT EXISTS ${FQ_SCHEMA}.STREAMLIT_STAGE DIRECTORY=(ENABLE=TRUE);"
    }
    elseif ($errMsg -match "(?i)insufficient privileges|access denied|not authorized") {
        $remediations += "Ensure your role can create Streamlit apps in $FQ_SCHEMA."
        $remediations += "Required: CREATE STREAMLIT privilege on the schema."
        $remediations += "The setup SQL grants these to $APP_OWNER_ROLE — verify the grants completed."
    }
    elseif ($errMsg -match "(?i)already exists") {
        $remediations += "The --replace flag should handle this, but the app may be owned by a different role."
        $remediations += "Drop the existing app manually: DROP STREAMLIT $FQ_APP;"
        $remediations += "Then re-run this deployment script."
    }
    else {
        $remediations += "Review the full error output above for details."
        $remediations += "Try deploying manually: snow streamlit deploy --connection $Connection --database $DB_NAME --schema $SCHEMA_NAME --replace"
        $remediations += "Check that your snowflake.yml is valid YAML and defines the Streamlit app correctly."
    }

    Write-Remediation $remediations
    exit 1
}
Write-Host ""

# ---------------------------------------------------------------------------
# Step 3: Grant Streamlit access
# ---------------------------------------------------------------------------
Write-Step "3/3" "Granting $APP_OWNER_ROLE access to the Streamlit app..."

try {
    snow sql --connection $Connection --warehouse $WH_NAME --query "GRANT USAGE ON STREAMLIT $FQ_APP TO ROLE $APP_OWNER_ROLE" 2>&1 | ForEach-Object {
        Write-Detail $_
    }
    if ($LASTEXITCODE -ne 0) { throw "GRANT USAGE returned exit code $LASTEXITCODE" }
    Write-Success "$APP_OWNER_ROLE granted access to the Streamlit app."
} catch {
    # Non-fatal — the app is deployed, just the grant failed
    Write-Host "  WARN  Could not grant $APP_OWNER_ROLE access to the Streamlit app." -ForegroundColor DarkYellow
    Write-Detail "Error: $_"
    Write-Remediation @(
        "This is non-fatal — the app is deployed but $APP_OWNER_ROLE may not be able to see it.",
        "Run the grant manually:",
        "  snow sql --connection $Connection --query ""GRANT USAGE ON STREAMLIT $FQ_APP TO ROLE $APP_OWNER_ROLE""",
        "If the Streamlit object name differs, check: SHOW STREAMLITS IN SCHEMA $FQ_SCHEMA;"
    )
}

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "=============================================="
Write-Host "  Deployment Complete!" -ForegroundColor Green
Write-Host "=============================================="
Write-Host ""
Write-Host "  Your Snowflake Connection Explorer is now available at:"
Write-Host "    https://app.snowflake.com -> Streamlit -> $APP_NAME"
Write-Host ""
Write-Host "  Scheduled refresh: Every Sunday at 6:00 AM CST"
Write-Host "  Manual refresh:    CALL ${FQ_SCHEMA}.REFRESH_CONNECTION_ACCESS();"
Write-Host ""
Write-Host "  Troubleshooting:"
Write-Host "    - View task history:  SELECT * FROM TABLE(${DB_NAME}.INFORMATION_SCHEMA.TASK_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 10;"
Write-Host "    - Check row count:    SELECT COUNT(*) FROM ${FQ_SCHEMA}.CONNECTION_ACCESS_30D;"
Write-Host "    - Verify app exists:  SHOW STREAMLITS IN SCHEMA $FQ_SCHEMA;"
Write-Host ""
