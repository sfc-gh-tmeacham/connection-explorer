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
- **Heatmaps**: Client × Database, Database × Schema, and Client × Warehouse heat grids
- **Treemap**: Hierarchical view of access volume by database, schema, warehouse, and client

### Data Page
- **Interactive Table**: View raw access data with sortable and filterable columns
- **Group By**: Aggregate data by any combination of Client, Warehouse, Database, Schema, or Direction
- **Access Totals**: Row counts and total access count summaries

### General
- **Multi-Page Navigation**: Separate pages for Network Graph, Charts, and Data with top-of-page navigation
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
conda activate streamlit-data-lake-explorer
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
./deploy.sh <connection_name> [warehouse_name]

# Example:
./deploy.sh my_snowflake_connection COMPUTE_WH
```

**Windows:**
```cmd
REM Configure your Snowflake connection (one-time setup)
snow connection add

REM Deploy everything
deploy.bat <connection_name> [warehouse_name]

REM Example:
deploy.bat my_snowflake_connection COMPUTE_WH
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
    --filename snowflake_data_set_up.sql \
    --warehouse <warehouse_name>
```

Or run directly in Snowflake:
```sql
-- Open snowflake_data_set_up.sql in Snowsight and execute
```

**What `snowflake_data_set_up.sql` creates:**
- Database: `CONNECTION_EXPLORER_APP_DB`
- Schema: `APP`
- Table: `data_lake_access_30d` (transient table with 30-day access snapshot)
- Stage: `STREAMLIT_STAGE` (for app deployment)
- Procedure: `REFRESH_DATA_LAKE_ACCESS()` (uses INSERT OVERWRITE)
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

By default, the refresh task uses `CONNECTION_EXPLORER_WH`. To change this, edit `snowflake_data_set_up.sql` before deployment:

```sql
SET WH_NAME = 'YOUR_WAREHOUSE_NAME';  -- Change this
```

## Uninstalling

To remove all Connection Explorer objects from your account:

**Mac/Linux:**
```bash
./uninstall.sh <connection_name>

# To keep the database but remove everything else:
./uninstall.sh <connection_name> --keep-database
```

**Windows:**
```cmd
uninstall.bat <connection_name>

REM To keep the database but remove everything else:
uninstall.bat <connection_name> --keep-database
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
├── pages/                        # Streamlit pages
│   ├── network.py                # Network graph page
│   ├── charts.py                 # Charts page (bars, Sankey, heatmaps, treemap)
│   └── data.py                   # Data table page with group-by
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
├── snowflake_data_set_up.sql     # Database/schema/task setup script
├── snowflake.yml                 # Snowflake CLI deployment config
├── pyproject.toml                # Python project config and dependencies
├── deploy.sh / deploy.bat        # Deployment scripts
├── uninstall.sh / uninstall.bat  # Uninstall scripts
├── docs/                         # Developer documentation and lessons learned
└── README.md
```

## Configuration

### Snowflake Permissions

The deploying role requires the following privileges:

| Privilege | Purpose |
|-----------|---------|
| `CREATE DATABASE ON ACCOUNT` | Create the `CONNECTION_EXPLORER_APP_DB` database |
| `CREATE SCHEMA` | Create the `APP` schema |
| `CREATE TABLE`, `CREATE STAGE`, `CREATE PROCEDURE` | Create objects in the schema |
| `CREATE TASK`, `EXECUTE TASK` | Create and run the scheduled refresh task |
| `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE` | Access to `account_usage` views |

The app queries data from `snowflake.account_usage` views:
- `snowflake.account_usage.sessions`
- `snowflake.account_usage.query_history`
- `snowflake.account_usage.access_history`

> **Note:** Access to `account_usage` views requires `IMPORTED PRIVILEGES` on the shared `SNOWFLAKE` database. This is typically granted by an account administrator:
> ```sql
> GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
> ```

The refresh procedure pre-aggregates this data into `CONNECTION_EXPLORER_APP_DB.APP.data_lake_access_30d` for fast dashboard performance.

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
