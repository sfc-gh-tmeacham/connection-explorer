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
conda activate streamlit-medtronic
```

## Running Locally

```bash
streamlit run streamlit_app.py
```

The app will open in your browser at `http://localhost:8501`

**Note**: Without a Snowflake connection, the app will display sample data for demonstration purposes.

## Deploying to Snowflake (Streamlit in Snowflake)

### Prerequisites
- Snowflake CLI (`snow`) installed
- Snowflake account with appropriate permissions

### Deployment Steps

1. **Update `snowflake.yml`** with your database, schema, and stage:

```yaml
definition_version: 1
streamlit:
  name: DATA_LAKE_EXPLORER
  stage: YOUR_DB.YOUR_SCHEMA.YOUR_STAGE
  main_file: streamlit_app.py
  query_warehouse: YOUR_WAREHOUSE
  python: 3.11
  packages:
    - streamlit
    - pandas
    - altair==5.5.0
    - pyvis==0.3.2
    - networkx==3.4.2
    - pillow==12.0.0
artifacts:
  - streamlit_app.py
  - static/**
```

2. **Deploy using Snowflake CLI**:

```bash
snow streamlit deploy
```

## Project Structure

```
data-lake-explorer/
├── streamlit_app.py      # Main Streamlit application
├── static/               # Static assets (images)
│   ├── snowflake-bug-logo.png
│   ├── snowflake-database.png
│   ├── snowflake-warehouse.png
│   └── ...
├── requirements.txt      # pip dependencies
├── environment.yml       # conda environment
├── snowflake.yml         # Snowflake deployment config
└── README.md
```

## Configuration

### Snowflake Permissions

The app queries the following views (requires `SNOWFLAKE` database access):
- `snowflake.account_usage.sessions`
- `snowflake.account_usage.query_history`
- `snowflake.account_usage.access_history`

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

