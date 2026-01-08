# Data Lake Explorer

**Full visibility into your Snowflake data access** — A Streamlit application that visualizes database and warehouse access patterns using interactive network graphs powered by Snowflake Horizon Catalog.

![Snowflake](static/snowflake-bug-logo.png)

## Features

- **Interactive Network Visualization**: See how databases, warehouses, and client applications connect
- **Real-time Data**: Pulls from Snowflake's `account_usage` views via Horizon Catalog
- **Smart Client Detection**: Automatically identifies 50+ application types (Tableau, Power BI, Databricks, etc.)
- **Flexible Filtering**: Filter by database, warehouse, client, organization, and access direction
- **Theme Support**: Adapts to Streamlit's light and dark themes
- **Sample Data Mode**: Works locally without Snowflake connection for demos

## Screenshots

The app displays:
- **Network Graph**: Interactive visualization showing data flow between databases and warehouses
- **Bar Charts**: Access counts by client, database, and warehouse

## Prerequisites

- Python 3.11+
- Snowflake account (optional - sample data available for demos)

## Installation

### Option 1: Using pip

```bash
# Clone the repository
git clone https://github.com/sfc-gh-mfulkerson/data-lake-explorer.git
cd data-lake-explorer

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

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
streamlit run streamlit_app.py
```

The app will open in your browser at `http://localhost:8501`

**Note**: Without a Snowflake connection, the app will display sample data for demonstration purposes.

## Deploying to Snowflake (Streamlit in Snowflake)

### Prerequisites
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli) (`snow`) installed
- Snowflake account with **ACCOUNTADMIN** role access
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
    --role ACCOUNTADMIN \
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

## Project Structure

```
data-lake-explorer/
├── streamlit_app.py          # Main Streamlit application
├── snowflake_data_set_up.sql # Database/schema/task setup script
├── deploy.sh                 # Deployment script (Mac/Linux)
├── deploy.bat                # Deployment script (Windows)
├── snowflake.yml             # Snowflake CLI deployment config
├── static/                   # Static assets (images)
│   ├── snowflake-bug-logo.png
│   ├── snowflake-database.png
│   ├── snowflake-warehouse.png
│   └── ...
├── requirements.txt          # pip dependencies
├── environment.yml           # conda environment
└── README.md
```

## Configuration

### Snowflake Permissions

**Setup requires ACCOUNTADMIN** to create the database, schema, and task.

The app queries data from `snowflake.account_usage` views:
- `snowflake.account_usage.sessions`
- `snowflake.account_usage.query_history`
- `snowflake.account_usage.access_history`

The refresh task pre-aggregates this data into `SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d` for fast dashboard performance.

### Client Application Mappings

The app automatically recognizes 50+ client applications including:
- BI Tools: Tableau, Power BI, MicroStrategy, Domo
- ETL/ELT: Databricks, Airflow, Azure Data Factory, Alteryx
- Development: Python, JDBC, DBeaver, VSCode
- And many more...

## License

See [LICENSE](LICENSE) file.

## Built With

- [Streamlit](https://streamlit.io/) - The web framework
- [PyVis](https://pyvis.readthedocs.io/) - Network visualization
- [Altair](https://altair-viz.github.io/) - Declarative charts
- [Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index) - Snowflake connectivity

