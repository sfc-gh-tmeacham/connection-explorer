"""Data loading, processing, and filtering."""

from typing import Sequence

import pandas as pd
import streamlit as st

from components.setup import ensure_tables_exist


@st.cache_data(show_spinner=False, ttl=3600)
def get_current_account(session) -> str:
    """Get current Snowflake account name (cached for 1 hour)."""
    if session is not None:
        try:
            return session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
        except Exception:
            pass
    return "SAMPLE_ACCOUNT"


def sample_dataframe(session) -> pd.DataFrame:
    """Generate sample data for demo/local development.

    Models a realistic medallion architecture with multiple domain databases
    per layer, purpose-built warehouses, and realistic tool connections.

    Bronze (raw):  RAW_EVENTS_DB, RAW_ERP_DB, RAW_CRM_DB, RAW_CLICKSTREAM_DB
    Silver (clean): CLEANED_EVENTS_DB, CLEANED_ERP_DB, CLEANED_CRM_DB,
                    CLEANED_CLICKSTREAM_DB, INTEGRATED_DB
    Gold (marts):  FINANCE_MART_DB, MARKETING_MART_DB, PRODUCT_MART_DB,
                   EXECUTIVE_MART_DB
    Other:         ML_FEATURES_DB, GOVERNANCE_DB, REVERSE_ETL_DB, SANDBOX_DB
    """
    current_account = get_current_account(session)
    org = "SAMPLE_ORG"

    # Each row: (DATABASE, WAREHOUSE, CLIENT, DIRECTION, ACCESS_COUNT)
    rows = [
        # ═══════════════════════════════════════════════════════════════════
        # BRONZE LAYER — raw ingestion from source systems
        # ═══════════════════════════════════════════════════════════════════

        # ── RAW_EVENTS_DB: application event streams ─────────────────────
        ("RAW_EVENTS_DB", "INGEST_STREAMING_WH", "Kafka", "write", 18200),
        ("RAW_EVENTS_DB", "INGEST_STREAMING_WH", "Snowpark", "write", 6400),
        ("RAW_EVENTS_DB", "INGEST_STREAMING_WH", "Airflow", "write", 4100),

        # ── RAW_ERP_DB: SAP / finance source data ────────────────────────
        ("RAW_ERP_DB", "INGEST_BATCH_WH", "Fivetran", "write", 14600),
        ("RAW_ERP_DB", "INGEST_BATCH_WH", "Informatica Cloud", "write", 9200),
        ("RAW_ERP_DB", "INGEST_BATCH_WH", "Airflow", "write", 3800),

        # ── RAW_CRM_DB: Salesforce / HubSpot extracts ───────────────────
        ("RAW_CRM_DB", "INGEST_BATCH_WH", "Fivetran", "write", 11800),
        ("RAW_CRM_DB", "INGEST_BATCH_WH", "Airbyte", "write", 7600),
        ("RAW_CRM_DB", "INGEST_BATCH_WH", "MuleSoft", "write", 3200),

        # ── RAW_CLICKSTREAM_DB: web/mobile analytics ─────────────────────
        ("RAW_CLICKSTREAM_DB", "INGEST_STREAMING_WH", "Kafka", "write", 22400),
        ("RAW_CLICKSTREAM_DB", "INGEST_STREAMING_WH", "Airbyte", "write", 5800),
        ("RAW_CLICKSTREAM_DB", "INGEST_BATCH_WH", "Matillion", "write", 2900),

        # ═══════════════════════════════════════════════════════════════════
        # SILVER LAYER — cleansed, conformed, deduplicated
        # ═══════════════════════════════════════════════════════════════════

        # ── CLEANED_EVENTS_DB ────────────────────────────────────────────
        ("CLEANED_EVENTS_DB", "TRANSFORM_CORE_WH", "dbt", "write", 11400),
        ("CLEANED_EVENTS_DB", "TRANSFORM_CORE_WH", "Snowpark", "write", 5600),
        ("RAW_EVENTS_DB", "TRANSFORM_CORE_WH", "dbt", "read", 11200),
        ("RAW_EVENTS_DB", "TRANSFORM_CORE_WH", "Snowpark", "read", 5400),

        # ── CLEANED_ERP_DB ───────────────────────────────────────────────
        ("CLEANED_ERP_DB", "TRANSFORM_CORE_WH", "dbt", "write", 9800),
        ("CLEANED_ERP_DB", "TRANSFORM_CORE_WH", "Coalesce", "write", 4200),
        ("CLEANED_ERP_DB", "TRANSFORM_CORE_WH", "Airflow", "write", 3100),
        ("RAW_ERP_DB", "TRANSFORM_CORE_WH", "dbt", "read", 9600),
        ("RAW_ERP_DB", "TRANSFORM_CORE_WH", "Coalesce", "read", 4000),
        ("RAW_ERP_DB", "TRANSFORM_CORE_WH", "Airflow", "read", 2900),

        # ── CLEANED_CRM_DB ───────────────────────────────────────────────
        ("CLEANED_CRM_DB", "TRANSFORM_CORE_WH", "dbt", "write", 8600),
        ("CLEANED_CRM_DB", "TRANSFORM_CORE_WH", "Matillion", "write", 3400),
        ("RAW_CRM_DB", "TRANSFORM_CORE_WH", "dbt", "read", 8400),
        ("RAW_CRM_DB", "TRANSFORM_CORE_WH", "Matillion", "read", 3200),

        # ── CLEANED_CLICKSTREAM_DB ───────────────────────────────────────
        ("CLEANED_CLICKSTREAM_DB", "TRANSFORM_CORE_WH", "dbt", "write", 13200),
        ("CLEANED_CLICKSTREAM_DB", "TRANSFORM_CORE_WH", "Snowpark", "write", 6100),
        ("RAW_CLICKSTREAM_DB", "TRANSFORM_CORE_WH", "dbt", "read", 12800),
        ("RAW_CLICKSTREAM_DB", "TRANSFORM_CORE_WH", "Snowpark", "read", 5900),

        # ── INTEGRATED_DB: cross-domain entity resolution ────────────────
        ("INTEGRATED_DB", "TRANSFORM_HEAVY_WH", "dbt", "write", 7800),
        ("INTEGRATED_DB", "TRANSFORM_HEAVY_WH", "Snowpark", "write", 4600),
        ("INTEGRATED_DB", "TRANSFORM_HEAVY_WH", "Coalesce", "write", 2800),
        ("CLEANED_EVENTS_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 7200),
        ("CLEANED_CRM_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 6800),
        ("CLEANED_ERP_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 5400),
        ("CLEANED_CLICKSTREAM_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 6100),
        ("CLEANED_CRM_DB", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 3800),
        ("CLEANED_ERP_DB", "TRANSFORM_HEAVY_WH", "Coalesce", "read", 2600),

        # ═══════════════════════════════════════════════════════════════════
        # GOLD LAYER — business-ready dimensional marts
        # ═══════════════════════════════════════════════════════════════════

        # ── FINANCE_MART_DB: revenue, billing, forecasting ───────────────
        ("FINANCE_MART_DB", "TRANSFORM_HEAVY_WH", "dbt", "write", 8200),
        ("FINANCE_MART_DB", "TRANSFORM_HEAVY_WH", "Coalesce", "write", 3600),
        ("INTEGRATED_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 7600),
        ("CLEANED_ERP_DB", "TRANSFORM_HEAVY_WH", "dbt", "read", 4800),

        # ── MARKETING_MART_DB: attribution, campaigns, funnel ────────────
        ("MARKETING_MART_DB", "TRANSFORM_HEAVY_WH", "dbt", "write", 7400),
        ("MARKETING_MART_DB", "TRANSFORM_HEAVY_WH", "Snowpark", "write", 3100),
        ("INTEGRATED_DB", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 4200),
        ("CLEANED_CLICKSTREAM_DB", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 3600),

        # ── PRODUCT_MART_DB: usage metrics, feature adoption ─────────────
        ("PRODUCT_MART_DB", "TRANSFORM_HEAVY_WH", "dbt", "write", 6800),
        ("PRODUCT_MART_DB", "TRANSFORM_HEAVY_WH", "Airflow", "write", 2400),
        ("CLEANED_EVENTS_DB", "TRANSFORM_HEAVY_WH", "Airflow", "read", 2200),

        # ── EXECUTIVE_MART_DB: KPI roll-ups, board metrics ───────────────
        ("EXECUTIVE_MART_DB", "TRANSFORM_CORE_WH", "dbt", "write", 4200),
        ("FINANCE_MART_DB", "TRANSFORM_CORE_WH", "dbt", "read", 3800),
        ("MARKETING_MART_DB", "TRANSFORM_CORE_WH", "dbt", "read", 3400),
        ("PRODUCT_MART_DB", "TRANSFORM_CORE_WH", "dbt", "read", 3100),

        # ═══════════════════════════════════════════════════════════════════
        # GOLD READS — BI, reporting, and analytics consumers
        # ═══════════════════════════════════════════════════════════════════

        # ── Finance BI ───────────────────────────────────────────────────
        ("FINANCE_MART_DB", "BI_FINANCE_WH", "Tableau", "read", 9800),
        ("FINANCE_MART_DB", "BI_FINANCE_WH", "Power BI", "read", 7200),
        ("FINANCE_MART_DB", "BI_FINANCE_WH", "Excel", "read", 4800),
        ("FINANCE_MART_DB", "BI_FINANCE_WH", "Snowflake Web", "read", 3600),
        ("FINANCE_MART_DB", "BI_FINANCE_WH", "Looker", "read", 2400),

        # ── Marketing BI ─────────────────────────────────────────────────
        ("MARKETING_MART_DB", "BI_MARKETING_WH", "Looker", "read", 8600),
        ("MARKETING_MART_DB", "BI_MARKETING_WH", "Sigma", "read", 5400),
        ("MARKETING_MART_DB", "BI_MARKETING_WH", "Tableau", "read", 4200),
        ("MARKETING_MART_DB", "BI_MARKETING_WH", "Metabase", "read", 2800),
        ("MARKETING_MART_DB", "BI_MARKETING_WH", "Python", "read", 1600),

        # ── Product BI ───────────────────────────────────────────────────
        ("PRODUCT_MART_DB", "BI_PRODUCT_WH", "Looker", "read", 7200),
        ("PRODUCT_MART_DB", "BI_PRODUCT_WH", "Grafana", "read", 5600),
        ("PRODUCT_MART_DB", "BI_PRODUCT_WH", "Sigma", "read", 3400),
        ("PRODUCT_MART_DB", "BI_PRODUCT_WH", "Snowflake Web", "read", 2800),
        ("PRODUCT_MART_DB", "BI_PRODUCT_WH", "Metabase", "read", 1800),

        # ── Executive dashboards ─────────────────────────────────────────
        ("EXECUTIVE_MART_DB", "BI_EXEC_WH", "Tableau", "read", 6200),
        ("EXECUTIVE_MART_DB", "BI_EXEC_WH", "Power BI", "read", 5400),
        ("EXECUTIVE_MART_DB", "BI_EXEC_WH", "MicroStrategy", "read", 3800),
        ("EXECUTIVE_MART_DB", "BI_EXEC_WH", "Snowflake Web", "read", 2200),

        # ── Cross-mart analyst queries ───────────────────────────────────
        ("FINANCE_MART_DB", "ANALYST_WH", "DBeaver", "read", 2600),
        ("MARKETING_MART_DB", "ANALYST_WH", "DBeaver", "read", 1800),
        ("PRODUCT_MART_DB", "ANALYST_WH", "Python", "read", 3200),
        ("FINANCE_MART_DB", "ANALYST_WH", "Python", "read", 2400),
        ("INTEGRATED_DB", "ANALYST_WH", "DBeaver", "read", 1400),
        ("INTEGRATED_DB", "ANALYST_WH", "Snowflake Web", "read", 2200),
        ("MARKETING_MART_DB", "ANALYST_WH", "Snowflake Web", "read", 1600),

        # ═══════════════════════════════════════════════════════════════════
        # ML / DATA SCIENCE
        # ═══════════════════════════════════════════════════════════════════

        # ── ML_FEATURES_DB: feature store and model outputs ──────────────
        ("ML_FEATURES_DB", "ML_TRAINING_WH", "Snowpark", "write", 6200),
        ("ML_FEATURES_DB", "ML_TRAINING_WH", "Python", "write", 8400),
        ("ML_FEATURES_DB", "ML_TRAINING_WH", "Dataiku", "write", 4800),
        ("ML_FEATURES_DB", "ML_TRAINING_WH", "Airflow", "write", 2600),
        # ML reads from gold + integrated
        ("INTEGRATED_DB", "ML_TRAINING_WH", "Snowpark", "read", 5800),
        ("INTEGRATED_DB", "ML_TRAINING_WH", "Python", "read", 7600),
        ("PRODUCT_MART_DB", "ML_TRAINING_WH", "Dataiku", "read", 4200),
        ("MARKETING_MART_DB", "ML_TRAINING_WH", "Python", "read", 3400),
        ("CLEANED_EVENTS_DB", "ML_TRAINING_WH", "Snowpark", "read", 4600),
        # Inference serving reads feature store
        ("ML_FEATURES_DB", "ML_SERVING_WH", "Snowpark", "read", 14200),
        ("ML_FEATURES_DB", "ML_SERVING_WH", "Python", "read", 9800),

        # ═══════════════════════════════════════════════════════════════════
        # REVERSE ETL — pushing data back to operational systems
        # ═══════════════════════════════════════════════════════════════════
        ("REVERSE_ETL_DB", "REVERSE_ETL_WH", "Fivetran", "write", 3800),
        ("REVERSE_ETL_DB", "REVERSE_ETL_WH", "Airflow", "write", 2600),
        ("MARKETING_MART_DB", "REVERSE_ETL_WH", "Fivetran", "read", 3600),
        ("FINANCE_MART_DB", "REVERSE_ETL_WH", "Airflow", "read", 2400),
        ("ML_FEATURES_DB", "REVERSE_ETL_WH", "Fivetran", "read", 1800),
        # Salesforce sync
        ("REVERSE_ETL_DB", "REVERSE_ETL_WH", "Salesforce", "read", 4200),

        # ═══════════════════════════════════════════════════════════════════
        # GOVERNANCE & OBSERVABILITY
        # ═══════════════════════════════════════════════════════════════════
        ("GOVERNANCE_DB", "ADMIN_WH", "Snowflake Web", "write", 3200),
        ("GOVERNANCE_DB", "ADMIN_WH", "Airflow", "write", 1800),
        ("GOVERNANCE_DB", "ADMIN_WH", "Datadog", "read", 4600),
        ("GOVERNANCE_DB", "ADMIN_WH", "Snowflake Web", "read", 6800),
        ("GOVERNANCE_DB", "ADMIN_WH", "New Relic", "read", 2200),
        ("GOVERNANCE_DB", "ADMIN_WH", "Splunk", "read", 1400),
        # Observability reads across all layers
        ("RAW_EVENTS_DB", "ADMIN_WH", "Datadog", "read", 1200),
        ("CLEANED_EVENTS_DB", "ADMIN_WH", "Datadog", "read", 900),
        ("FINANCE_MART_DB", "ADMIN_WH", "Datadog", "read", 800),

        # ═══════════════════════════════════════════════════════════════════
        # SANDBOX — ad-hoc dev/exploration
        # ═══════════════════════════════════════════════════════════════════
        ("SANDBOX_DB", "ANALYST_WH", "DBeaver", "write", 1800),
        ("SANDBOX_DB", "ANALYST_WH", "Snowflake Web", "write", 3200),
        ("SANDBOX_DB", "ANALYST_WH", "Python", "write", 2600),
        ("SANDBOX_DB", "ANALYST_WH", "DBeaver", "read", 2200),
        ("SANDBOX_DB", "ANALYST_WH", "Snowflake Web", "read", 4800),
        ("SANDBOX_DB", "ANALYST_WH", "Python", "read", 3400),
        ("SANDBOX_DB", "ANALYST_WH", "VSCode", "read", 1200),
        ("SANDBOX_DB", "ANALYST_WH", "VSCode", "write", 800),
        # Sandbox users reading silver for experimentation
        ("CLEANED_CRM_DB", "ANALYST_WH", "DBeaver", "read", 900),
        ("CLEANED_ERP_DB", "ANALYST_WH", "Python", "read", 1100),
        ("INTEGRATED_DB", "ANALYST_WH", "Python", "read", 1600),
    ]

    n = len(rows)
    databases, warehouses, clients, directions, counts = zip(*rows)

    sample_data = {
        "ORGANIZATION_NAME": [org] * n,
        "ACCOUNT_NAME": [current_account] * n,
        "DATABASE": list(databases),
        "WAREHOUSE": list(warehouses),
        "CLIENT": list(clients),
        "DIRECTION": list(directions),
        "ACCESS_COUNT": list(counts),
    }
    st.info(
        "Using sample data. Connect to Snowflake to view live account usage data "
        "from Snowflake Horizon Catalog."
    )
    return pd.DataFrame(sample_data)


@st.cache_data(show_spinner=False, ttl=300)
def load_data(session) -> pd.DataFrame:
    """Load data from Snowflake account usage or return sample data."""
    if session is None:
        return sample_dataframe(session)
    try:
        ensure_tables_exist(session)
        query = """
            SELECT account_id AS ACCOUNT_NAME, * 
            FROM SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS.data_lake_access_30d 
            ORDER BY access_count DESC;
        """

        result_df = session.sql(query).to_pandas()
        if result_df.empty:
            st.warning("No data found. Using sample data.")
            return sample_dataframe(session)
        return result_df
    except Exception as exc:
        st.error(f"Unable to query account usage data. Falling back to sample data.\n\nError: {exc}")
        return sample_dataframe(session)


@st.cache_data(show_spinner=False)
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and aggregate dataframe."""
    if df.empty:
        return df
    df = df.dropna(how="any", axis=0)
    df = df.query("ACCESS_COUNT > 20")
    df = (
        df.groupby(
            ["DATABASE", "WAREHOUSE", "CLIENT", "DIRECTION", "ORGANIZATION_NAME", "ACCOUNT_NAME"],
            as_index=False,
        )
        .agg(ACCESS_COUNT=pd.NamedAgg(column="ACCESS_COUNT", aggfunc="sum"))
        .sort_values(by="ACCESS_COUNT", ascending=False)
    )
    return df


@st.cache_data(show_spinner=False)
def apply_filters(
    df: pd.DataFrame,
    database_names: Sequence[str],
    warehouse_names: Sequence[str],
    client_names: Sequence[str],
    org_filter: str,
    direction_filters: Sequence[str],
    access_count: int,
) -> pd.DataFrame:
    """Apply filters to dataframe with caching."""
    if df.empty:
        return df

    mask = df["ACCESS_COUNT"] > access_count

    if database_names:
        mask &= df["DATABASE"].astype(str).isin(list(database_names))
    if warehouse_names:
        mask &= df["WAREHOUSE"].astype(str).isin(list(warehouse_names))
    if client_names:
        mask &= df["CLIENT"].astype(str).isin(list(client_names))
    if org_filter:
        mask &= df["ORGANIZATION_NAME"].astype(str).str.contains(org_filter, na=False)
    if direction_filters:
        mask &= df["DIRECTION"].astype(str).isin(list(direction_filters))

    return df.loc[mask]


@st.cache_data(show_spinner=False)
def get_distinct_values(df: pd.DataFrame, column: str) -> list:
    """Get distinct values for filter dropdowns."""
    if df.empty:
        return []
    return sorted(df[column].astype(str).unique().tolist())
