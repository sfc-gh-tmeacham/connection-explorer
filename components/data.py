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
    """Generate sample data for demo/local development."""
    current_account = get_current_account(session)
    sample_data = {
        "ORGANIZATION_NAME": ["SAMPLE_ORG"] * 20,
        "ACCOUNT_NAME": [current_account] * 20,
        "DATABASE": [
            # Readers pulling from prod/analytics
            "PROD_DB", "PROD_DB", "ANALYTICS_DB", "ANALYTICS_DB",
            "REPORTING_DB", "PROD_DB", "ANALYTICS_DB", "PROD_DB",
            # Writers pushing into staging/prod
            "STAGING_DB", "STAGING_DB", "PROD_DB", "PROD_DB",
            "STAGING_DB", "PROD_DB", "STAGING_DB", "PROD_DB",
            # Dual-use: clients that both read and write
            "STAGING_DB", "ANALYTICS_DB", "PROD_DB", "PROD_DB",
        ],
        "WAREHOUSE": [
            # Readers
            "REPORTING_WH", "REPORTING_WH", "ANALYTICS_WH", "ANALYTICS_WH",
            "REPORTING_WH", "COMPUTE_WH", "ANALYTICS_WH", "COMPUTE_WH",
            # Writers
            "LOADING_WH", "LOADING_WH", "LOADING_WH", "TRANSFORM_WH",
            "LOADING_WH", "TRANSFORM_WH", "LOADING_WH", "TRANSFORM_WH",
            # Dual-use
            "TRANSFORM_WH", "ANALYTICS_WH", "TRANSFORM_WH", "ANALYTICS_WH",
        ],
        "CLIENT": [
            # Readers — BI tools and query tools
            "Power BI", "Tableau", "Python", "Snowflake Web",
            "Sigma", "DBeaver", "MicroStrategy", "Excel",
            # Writers — ELT/ETL and ingestion tools
            "Fivetran", "Airflow", "Snowpark", "Databricks/Spark",
            "Kafka", "Informatica Cloud", "Alteryx", "Coalesce",
            # Dual-use — Python also writes, Snowpark also reads
            "Python", "Snowpark", "Databricks/Spark", "Airflow",
        ],
        "DIRECTION": [
            # Readers
            "read", "read", "read", "read",
            "read", "read", "read", "read",
            # Writers
            "write", "write", "write", "write",
            "write", "write", "write", "write",
            # Dual-use
            "write", "read", "read", "read",
        ],
        "ACCESS_COUNT": [
            # Readers
            3200, 2800, 1900, 1500, 1100, 850, 750, 400,
            # Writers
            4500, 3100, 2600, 2200, 1800, 1400, 950, 600,
            # Dual-use
            1700, 1300, 900, 550,
        ],
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
