# Snowflake Connection Explorer

[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.54-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**Full visibility into your Snowflake data access** — A Streamlit application that visualizes database, schema, warehouse, and client application access patterns using interactive network graphs powered by Snowflake Horizon Catalog.

![Snowflake](static/snowflake-bug-logo.png)

## Repository Policy (Public / Read-Only)

This repository is published **as-is** for reference and reuse.

- **Support**: No support is provided.
- **Issues / Discussions**: Not accepted.
- **Pull requests**: Not accepted (please fork if you'd like to modify).

## Features

### Network Graph Page
- **Interactive vis.js Network**: Visualizes connections between clients, warehouses, databases, and schemas with physics-based layout
- **4-Level Hierarchy**: CLIENT → WAREHOUSE → DATABASE → SCHEMA flow visualization
- **SVG Client Icons**: Automatically displays branded icons for 60+ recognized client applications (Tableau, Power BI, Databricks, dbt, etc.)
- **Click-to-Filter**: Click any node to add it to the sidebar filters (additive); click empty canvas to clear all filters
- **Cluster Databases**: Group database nodes into a single cluster to simplify the view
- **Hide Node Types**: Toggle visibility for warehouses, clients, databases, or schemas — at least 2 types must remain visible
- **Full Screen Mode**: Expand the network graph to fill the browser window
- **Save PNG**: Export a high-resolution (4096px+) PNG of the network graph with readable labels

### Charts Page
- **Stacked Bar Charts**: Top databases, schemas, warehouses, and clients by access count, split by read/write direction
- **4-Column Sankey Diagrams**: Flow visualization of Client → Warehouse → Database → Schema access for reads and writes
- **Heatmaps**: Database × Client, Schema × Client, and Warehouse × Client heat grids
- **Treemap**: Hierarchical view of access volume by database, schema, warehouse, and client

### Data Page
- **Interactive Table**: View raw access data with sortable and filterable columns
- **Group By**: Aggregate data by any combination of Client, Warehouse, Database, Schema, or Direction
- **Access Totals**: Row counts and total access count summaries

### Classifications Page
- **Client Application Editor**: View and edit the `client_app_classification` table that maps raw client strings to friendly display names
- **Inline Editing**: Update classification entries directly in the app with changes written back to Snowflake

### General
- **Multi-Page Navigation**: Separate pages for Network Graph, Charts, Data, and Classifications with top-of-page navigation
- **Real-time Data**: Pulls from Snowflake's `account_usage` views via Horizon Catalog
- **Smart Client Detection**: Automatically identifies 60+ application types
- **Flexible Filtering**: Filter by database, schema, warehouse, client, organization, direction, and access count
- **Theme Support**: Adapts to Streamlit's light and dark themes
- **Sample Data Mode**: Works locally without Snowflake connection for demos

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or conda
- Snowflake account (optional — sample data available for demos)

## Installation

### Option 1: Using uv (Recommended)

```bash
# Clone the repository
git clone https://github.com/sfc-gh-mfulkerson/data-lake-explorer.git
cd data-lake-explorer

# Install dependencies and run
uv run streamlit run streamlit_app.py
```

`uv` will automatically create a virtual environment and install all dependencies from `pyproject.toml`.

### Option 2: Using conda

```bash
# Clone the repository
git clone https://github.com/sfc-gh-mfulkerson/data-lake-explorer.git
cd data-lake-explorer

# Create conda environment
conda env create -f environment.yml
conda activate streamlit-connection-explorer
```

## Running Locally

```bash
# With uv
uv run streamlit run streamlit_app.py

# With conda (after activating the environment)
streamlit run streamlit_app.py
```

The app will open in your browser at `http://localhost:8501`

**Note**: Without a Snowflake connection, the app will display sample data for demonstration purposes.

## How the App Connects to Snowflake

The app uses a three-tier connection strategy that adapts to wherever it's running:

| Environment | Connection Method | Configuration |
|---|---|---|
| **Streamlit in Snowflake** | `get_active_session()` — Snowflake auto-injects a pre-authenticated Snowpark session into the container runtime | None required; inherits role and warehouse from the Streamlit app object |
| **Local with credentials** | `st.connection("snowflake")` — Streamlit's built-in Snowflake connector reads from `.streamlit/secrets.toml` | Requires `.streamlit/secrets.toml` with account, user, and authentication details |
| **Local without credentials** | `session = None` — falls back to synthetic sample data | No configuration needed; the app runs in demo mode |

The logic lives in `streamlit_app.py` and executes on startup:

```python
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    try:
        conn = st.connection("snowflake")
        session = conn.session()
    except Exception:
        session = None
```

**Key points:**
- When deployed to Snowflake, there are **no credentials to manage** — the platform handles authentication transparently via `get_active_session()`
- The app never uses raw `snowflake.connector` or `Session.builder` — all queries go through the Snowpark session
- All data access uses `session.sql(...).to_pandas()` to execute SQL and return DataFrames

## Deploying to Snowflake (Streamlit in Snowflake)

### Prerequisites
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli) (`snow`) installed
- Snowflake account with a role that has the [required privileges](#snowflake-permissions)
- A warehouse for running queries

### Quick Deploy (Recommended)

The easiest way to deploy is using the automated deployment scripts:

**Mac/Linux:**
```bash
# Configure your Snowflake connection (one-time setup)
snow connection add

# Deploy everything
./deploy/deploy.sh <connection_name> [warehouse_name]

# Example:
./deploy/deploy.sh my_snowflake_connection COMPUTE_WH
```

**Windows:**
```cmd
REM Configure your Snowflake connection (one-time setup)
snow connection add

REM Deploy everything
deploy\deploy.bat <connection_name> [warehouse_name]

REM Example:
deploy\deploy.bat my_snowflake_connection COMPUTE_WH
```

The deployment script will:
1. Create the `CONNECTION_EXPLORER_APP_DB.APP` database and schema
2. Create the transient data table and refresh stored procedure
3. Execute the procedure to load initial 30-day access data
4. Deploy the Streamlit app

### Manual Deployment

If you prefer step-by-step deployment:

#### Step 1: Set up the Database and Data Pipeline

Run the setup script to create the database, schema, table, and scheduled refresh task:

```bash
snow sql --connection <connection_name> \
    --filename deploy/snowflake_data_set_up.sql \
    --warehouse <warehouse_name>
```

Or run directly in Snowflake:
```sql
-- Open deploy/snowflake_data_set_up.sql in Snowsight and execute
```

**What `snowflake_data_set_up.sql` creates:**
- Database: `CONNECTION_EXPLORER_APP_DB`
- Schema: `APP`
- Table: `connection_access_30d` (transient table with 30-day access snapshot)
- Stage: `STREAMLIT_STAGE` (for app deployment)
- Procedure: `REFRESH_CONNECTION_ACCESS()` (uses INSERT OVERWRITE)
- Task: `DATA_ACCESS_REFRESH_TASK` (runs weekly on Sundays at 6am CST)

**Data collected:**
- Organization name and account name
- Client application, warehouse, database, and fully-qualified schema name
- Access direction:
  - **read**: SELECT, UNLOAD, GET_FILES
  - **write**: INSERT, UPDATE, DELETE, MERGE, COPY, PUT_FILES, COPY_FILES, REMOVE_FILES
  - **DDL**: CREATE, ALTER, DROP, TRUNCATE, RENAME, UNDROP, COMMENT, GRANT, REVOKE, RESTORE
  - **metadata**: SHOW, DESCRIBE, LIST_FILES, EXPLAIN
- Access count per combination
- Excludes: personal databases (`USER$%`), system clients, session/transaction commands

**Performance optimizations:**
- Uses split-join pattern for client classification to handle high query volumes efficiently
- Transient table with INSERT OVERWRITE for atomic refreshes without time travel overhead

#### Step 2: Deploy the Streamlit App

```bash
snow streamlit deploy \
    --connection <connection_name> \
    --database CONNECTION_EXPLORER_APP_DB \
    --schema APP \
    --replace
```

### Customizing the Warehouse

By default, the refresh task uses `CONNECTION_EXPLORER_WH`. To change this, edit `deploy/snowflake_data_set_up.sql` before deployment:

```sql
SET WH_NAME = 'YOUR_WAREHOUSE_NAME';  -- Change this
```

## Uninstalling

To remove all Connection Explorer objects from your account:

**Mac/Linux:**
```bash
./deploy/uninstall.sh <connection_name>

# To keep the database but remove everything else:
./deploy/uninstall.sh <connection_name> --keep-database
```

**Windows:**
```cmd
deploy\uninstall.bat <connection_name>

REM To keep the database but remove everything else:
deploy\uninstall.bat <connection_name> --keep-database
```

This will remove:
- Streamlit app
- Scheduled refresh task
- Stored procedure
- Data table
- Stage
- Schema (and optionally the database)

## Project Structure

```
data-lake-explorer/
├── streamlit_app.py              # Main app entry point and multi-page router
├── views/                        # Streamlit view modules (not in pages/ to avoid auto-discovery)
│   ├── network.py                # Network graph page
│   ├── charts.py                 # Charts page (bars, Sankey, heatmaps, treemap)
│   ├── data.py                   # Data table page with group-by
│   └── classifications.py        # Client classification editor page
├── components/                   # Shared components and logic
│   ├── network.py                # vis.js network rendering (inline HTML/JS)
│   ├── charts.py                 # Plotly chart builders
│   ├── data.py                   # Data loading and filtering
│   ├── assets.py                 # SVG icon loading and encoding
│   ├── client_mappings.py        # Client application detection rules
│   ├── theme.py                  # Snowflake brand colors and theme helpers
│   └── setup.py                  # Snowflake connection setup
├── static/                       # Static assets
│   ├── client-icons/             # 60+ branded SVG client icons
│   ├── snowflake-database.svg    # Database node icon (rounded square)
│   ├── snowflake-warehouse.svg   # Warehouse node icon (diamond)
│   ├── snowflake-schema.svg      # Schema node icon (hexagon)
│   └── snowflake-bug-logo.*      # Snowflake logo
├── deploy/                       # Deployment and infrastructure scripts
│   ├── snowflake_data_set_up.sql # Database/schema/task setup script
│   ├── deploy.sh / deploy.bat   # Deployment scripts
│   └── uninstall.sh / uninstall.bat  # Uninstall scripts
├── snowflake.yml                 # Snowflake CLI deployment config
├── pyproject.toml                # Python project config and dependencies
├── docs/                         # Developer documentation and lessons learned
└── README.md
```

## Configuration

### Snowflake Infrastructure

The deployment script (`deploy/snowflake_data_set_up.sql`) creates the following resources. All names are configurable via variables at the top of the script.

#### Resources Created by Deployment

| Resource | Name | Type | Purpose |
|----------|------|------|---------|
| **Database** | `CONNECTION_EXPLORER_APP_DB` | Standard | Houses all app objects |
| **Schema** | `APP` | Standard | Contains tables, procedures, tasks, and stage |
| **Warehouse** | `CONNECTION_EXPLORER_WH` | Medium, Gen 2 | Runs queries for the Streamlit app and refresh procedure. Auto-suspends after 60s. Query acceleration enabled. |
| **Compute Pool** | `STREAMLIT_COMPUTE_POOL` | CPU_X64_XS (1 node) | Runs the Streamlit app container when deployed to Snowflake. Auto-suspends after 300s. |
| **Stage** | `STREAMLIT_STAGE` | Internal, directory-enabled | Stores the deployed Streamlit app files |
| **Table** | `connection_access_30d` | Transient | 30-day access snapshot (no time travel/fail-safe overhead) |
| **Table** | `client_app_classification` | Standard | Client application pattern-matching rules (304 rows, seeded by the app) |
| **Procedure** | `REFRESH_CONNECTION_ACCESS()` | SQL | Aggregates data from `account_usage` views into the access table |
| **Task** | `DATA_ACCESS_REFRESH_TASK` | Serverless | Runs `REFRESH_CONNECTION_ACCESS()` weekly (Sundays 6am CST) |
| **Streamlit App** | `SNOWFLAKE_CONNECTION_EXPLORER` | Streamlit | The deployed application (created by `snow streamlit deploy`) |

#### External Dependencies

These resources must exist **before** deployment:

| Resource | Purpose | Notes |
|----------|---------|-------|
| `SNOWFLAKE` database | Source data (`account_usage` views) | Shared database present in every Snowflake account. Requires `IMPORTED PRIVILEGES` grant. |
| `PYPI_ACCESS_INTEGRATION` | External access integration | Required for Streamlit in Snowflake to install Python packages from PyPI. Must be created by an account admin if it doesn't already exist. |

#### Customizing Resource Names

Edit the variables at the top of `deploy/snowflake_data_set_up.sql`:

```sql
SET DB_NAME = 'CONNECTION_EXPLORER_APP_DB';    -- Database name
SET WH_NAME = 'CONNECTION_EXPLORER_WH';        -- Warehouse name
SET COMPUTE_POOL_NAME = 'STREAMLIT_COMPUTE_POOL'; -- Compute pool name
SET DEPLOY_ROLE = 'ACCOUNTADMIN';              -- Role that runs the setup
SET APP_OWNER_ROLE = 'SYSADMIN';               -- Role that owns the app at runtime
```

### Roles and Privileges

The deployment uses a two-role model:

| Role | Default | Purpose |
|------|---------|---------|
| **Deploy Role** | `ACCOUNTADMIN` | Runs the setup script — creates all resources, grants privileges, and configures the task. Only needed during deployment. |
| **App Owner Role** | `SYSADMIN` | Owns and runs the Streamlit app day-to-day. Receives scoped grants from the deploy role. |

#### Deploy Role Privileges

The deploy role needs:

| Privilege | Purpose |
|-----------|---------|
| `CREATE DATABASE ON ACCOUNT` | Create `CONNECTION_EXPLORER_APP_DB` |
| `CREATE WAREHOUSE ON ACCOUNT` | Create `CONNECTION_EXPLORER_WH` |
| `CREATE COMPUTE POOL ON ACCOUNT` | Create `STREAMLIT_COMPUTE_POOL` |
| `CREATE SCHEMA` on the database | Create the `APP` schema |
| `CREATE TABLE`, `CREATE STAGE`, `CREATE PROCEDURE` on the schema | Create app objects |
| `CREATE TASK`, `EXECUTE TASK` on account or schema | Create and run the refresh task |
| `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` | Access `account_usage` views |

#### App Owner Role Grants

The setup script grants the app owner role exactly what it needs to operate:

| Grant | Scope |
|-------|-------|
| `USAGE` | Database, schema, warehouse, compute pool |
| `SELECT`, `INSERT`, `UPDATE` | All tables in `APP` schema |
| `CREATE TABLE`, `CREATE STAGE` | `APP` schema |
| `USAGE` | `PYPI_ACCESS_INTEGRATION` |
| `USAGE` | `REFRESH_CONNECTION_ACCESS()` procedure |
| `OPERATE` | `DATA_ACCESS_REFRESH_TASK` |
| `EXECUTE MANAGED TASK ON ACCOUNT` | Required for serverless tasks |
| `USAGE` | Streamlit app (granted by deploy script after `snow streamlit deploy`) |

> **Note:** Access to `account_usage` views requires `IMPORTED PRIVILEGES` on the shared `SNOWFLAKE` database. This is typically granted by an account administrator:
> ```sql
> GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
> ```

#### Data Sources

The refresh procedure queries these `snowflake.account_usage` views:
- `snowflake.account_usage.sessions`
- `snowflake.account_usage.query_history`
- `snowflake.account_usage.access_history`

Results are pre-aggregated into `CONNECTION_EXPLORER_APP_DB.APP.connection_access_30d` for fast dashboard performance.

### Client Application Mappings

The app automatically recognizes 60+ client applications including:
- BI Tools: Tableau, Power BI, MicroStrategy, Domo
- ETL/ELT: Databricks, Airflow, Azure Data Factory, Alteryx
- Development: Python, JDBC, DBeaver, VSCode
- And many more...

## License

See [LICENSE](LICENSE) file.

## Built With

- [Streamlit](https://streamlit.io/) - The web framework
- [vis.js](https://visjs.org/) - Interactive network graph visualization
- [Plotly](https://plotly.com/python/) - Charts (bar, Sankey, heatmap, treemap)
- [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index) - Snowflake connectivity
- [Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) - AI-powered development assistant
