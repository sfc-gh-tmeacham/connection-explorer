# Data Lake Explorer

**Full visibility into your Snowflake data access** — A Streamlit application that visualizes database and warehouse access patterns using interactive network graphs powered by Snowflake Horizon Catalog.

![Snowflake](static/snowflake-bug-logo.png)

## Repository Policy (Public / Read-Only)

This repository is published **as-is** for reference and reuse.

- **Support**: No support is provided.
- **Issues / Discussions**: Not accepted.
- **Pull requests**: Not accepted (please fork if you'd like to modify).

## Features

### Network Graph (Home Page)
- **Interactive vis.js Network**: Visualizes connections between databases, warehouses, and client applications with physics-based layout
- **SVG Client Icons**: Automatically displays branded icons for 60+ recognized client applications (Tableau, Power BI, Databricks, dbt, etc.)
- **Node Isolation**: Click any node to isolate it and see only its direct connections
- **Cluster Databases**: Group database nodes into a single cluster to simplify the view
- **Hide Warehouses**: Toggle warehouse nodes on/off (hidden by default) for a cleaner Client → Database view
- **Full Screen Mode**: Expand the network graph to fill the browser window
- **Save PNG**: Export a high-resolution (4096px+) PNG of the network graph with readable labels

### Charts Page
- **Stacked Bar Charts**: Top databases, warehouses, and clients by access count, split by read/write direction
- **3-Column Sankey Diagrams**: Flow visualization of Client → Warehouse → Database access for reads and writes
- **Client × Warehouse Heatmap**: Heat grid showing which clients use which warehouses
- **Client × Database Heatmap**: Heat grid showing which clients access which databases
- **Treemap**: Hierarchical view of access volume by database, warehouse, and client

### General
- **Multi-Page Navigation**: Separate pages for the network graph and charts, with top-of-page navigation
- **Real-time Data**: Pulls from Snowflake's `account_usage` views via Horizon Catalog
- **Smart Client Detection**: Automatically identifies 60+ application types
- **Flexible Filtering**: Filter by database, warehouse, client, organization, direction, and access count
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
1. Create the `SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS` database and schema
2. Create the data table and refresh task
3. Execute the task to load initial 30-day access data
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
- Database: `SNOWFLAKE_DATA_LAKE`
- Schema: `DATA_LAKE_ACCESS`
- Table: `data_lake_access_30d` (30-day access snapshot)
- Stage: `STREAMLIT_STAGE` (for app deployment)
- Task: `DATA_LAKE_ACCESS_REFRESH_TASK` (runs weekly on Sundays at 6am CST)

#### Step 2: Deploy the Streamlit App

```bash
snow streamlit deploy \
    --connection <connection_name> \
    --database SNOWFLAKE_DATA_LAKE \
    --schema DATA_LAKE_ACCESS \
    --replace
```

### Customizing the Warehouse

By default, the refresh task uses `COMPUTE_WH`. To change this, edit `snowflake_data_set_up.sql` before deployment:

```sql
CREATE OR REPLACE TASK SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.DATA_LAKE_ACCESS_REFRESH_TASK
  WAREHOUSE = YOUR_WAREHOUSE_NAME  -- Change this
  ...
```

## Uninstalling

To remove all Data Lake Explorer objects from your account:

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
│   └── charts.py                 # Charts page (bars, Sankey, heatmaps, treemap)
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
│   ├── snowflake-database.svg    # Database node icon
│   ├── snowflake-warehouse.svg   # Warehouse node icon
│   └── snowflake-bug-logo.*      # Snowflake logo
├── snowflake_data_set_up.sql     # Database/schema/task setup script
├── snowflake.yml                 # Snowflake CLI deployment config
├── pyproject.toml                # Python project config and dependencies
├── deploy.sh / deploy.bat        # Deployment scripts
├── uninstall.sh / uninstall.bat  # Uninstall scripts
└── README.md
```

## Configuration

### Snowflake Permissions

The deploying role requires the following privileges:

| Privilege | Purpose |
|-----------|---------|
| `CREATE DATABASE ON ACCOUNT` | Create the `SNOWFLAKE_DATA_LAKE` database |
| `CREATE SCHEMA` | Create the `DATA_LAKE_ACCESS` schema |
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

The refresh task pre-aggregates this data into `SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d` for fast dashboard performance.

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

