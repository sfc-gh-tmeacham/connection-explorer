"""Data loading, processing, and filtering.

Provides the full data pipeline for the app: loading from Snowflake (or
generating sample data for local development), cleaning/aggregating rows,
and applying sidebar filter selections.  All expensive operations are
wrapped with ``st.cache_data`` for performance.
"""

import logging
from typing import Sequence

logger = logging.getLogger(__name__)

import pandas as pd
import streamlit as st

from components.client_mappings import CLIENT_ICON_FILES
from components.setup import ensure_tables_exist


@st.cache_data(show_spinner=False, ttl=3600)
def get_current_account(_session) -> str:
    """Return the current Snowflake account name.

    Cached for 1 hour.  Falls back to ``"SAMPLE_ACCOUNT"`` when no active
    Snowflake session is available (e.g. local development).

    Args:
        session: A Snowpark ``Session`` object, or ``None``.

    Returns:
        The account name string.
    """
    if _session is not None:
        try:
            return _session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
        except Exception:
            pass
    return "SAMPLE_ACCOUNT"


def sample_dataframe(_session) -> pd.DataFrame:
    """Generate sample data for demo and local development.

    Models a realistic medallion architecture with multiple domain databases
    per layer, purpose-built warehouses, and realistic tool connections.

    Layers:
        - Bronze (raw): RAW_EVENTS_DB, RAW_ERP_DB, RAW_CRM_DB,
          RAW_CLICKSTREAM_DB
        - Silver (clean): CLEANED_EVENTS_DB, CLEANED_ERP_DB, CLEANED_CRM_DB,
          CLEANED_CLICKSTREAM_DB, INTEGRATED_DB
        - Gold (marts): FINANCE_MART_DB, MARKETING_MART_DB, PRODUCT_MART_DB,
          EXECUTIVE_MART_DB
        - Other: ML_FEATURES_DB, GOVERNANCE_DB, REVERSE_ETL_DB, SANDBOX_DB

    Args:
        session: A Snowpark ``Session`` (used only to resolve the current
            account name) or ``None``.

    Returns:
        A DataFrame with columns ORGANIZATION_NAME, ACCOUNT_NAME, DATABASE,
        SCHEMA_NAME, WAREHOUSE, CLIENT, DIRECTION, and ACCESS_COUNT.
    """
    current_account = get_current_account(_session)
    org = "SAMPLE_ORG"

    # Each row: (DATABASE, SCHEMA_NAME, WAREHOUSE, CLIENT, DIRECTION, ACCESS_COUNT)
    rows = [
        # ═══════════════════════════════════════════════════════════════════
        # BRONZE LAYER — raw ingestion from source systems
        # ═══════════════════════════════════════════════════════════════════

        # ── RAW_EVENTS_DB: application event streams ─────────────────────
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.STREAMING", "INGEST_STREAMING_WH", "Kafka", "write", 18200),
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.STREAMING", "INGEST_STREAMING_WH", "Snowpark", "write", 6400),
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.BATCH", "INGEST_STREAMING_WH", "Airflow", "write", 4100),

        # ── RAW_ERP_DB: SAP / finance source data ────────────────────────
        ("RAW_ERP_DB", "RAW_ERP_DB.SAP_EXTRACT", "INGEST_BATCH_WH", "Fivetran", "write", 14600),
        ("RAW_ERP_DB", "RAW_ERP_DB.SAP_EXTRACT", "INGEST_BATCH_WH", "Informatica Cloud", "write", 9200),
        ("RAW_ERP_DB", "RAW_ERP_DB.FINANCE_RAW", "INGEST_BATCH_WH", "Airflow", "write", 3800),

        # ── RAW_CRM_DB: Salesforce / HubSpot extracts ───────────────────
        ("RAW_CRM_DB", "RAW_CRM_DB.SALESFORCE", "INGEST_BATCH_WH", "Fivetran", "write", 11800),
        ("RAW_CRM_DB", "RAW_CRM_DB.HUBSPOT", "INGEST_BATCH_WH", "Airbyte", "write", 7600),
        ("RAW_CRM_DB", "RAW_CRM_DB.SALESFORCE", "INGEST_BATCH_WH", "MuleSoft", "write", 3200),

        # ── RAW_CLICKSTREAM_DB: web/mobile analytics ─────────────────────
        ("RAW_CLICKSTREAM_DB", "RAW_CLICKSTREAM_DB.WEB_EVENTS", "INGEST_STREAMING_WH", "Kafka", "write", 22400),
        ("RAW_CLICKSTREAM_DB", "RAW_CLICKSTREAM_DB.MOBILE_EVENTS", "INGEST_STREAMING_WH", "Airbyte", "write", 5800),
        ("RAW_CLICKSTREAM_DB", "RAW_CLICKSTREAM_DB.WEB_EVENTS", "INGEST_BATCH_WH", "Matillion", "write", 2900),

        # ═══════════════════════════════════════════════════════════════════
        # SILVER LAYER — cleansed, conformed, deduplicated
        # ═══════════════════════════════════════════════════════════════════

        # ── CLEANED_EVENTS_DB ────────────────────────────────────────────
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.TRANSFORMED", "TRANSFORM_CORE_WH", "dbt", "write", 11400),
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.STAGING", "TRANSFORM_CORE_WH", "Snowpark", "write", 5600),
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.STREAMING", "TRANSFORM_CORE_WH", "dbt", "read", 11200),
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.BATCH", "TRANSFORM_CORE_WH", "Snowpark", "read", 5400),

        # ── CLEANED_ERP_DB ───────────────────────────────────────────────
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.TRANSFORMED", "TRANSFORM_CORE_WH", "dbt", "write", 9800),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.STAGING", "TRANSFORM_CORE_WH", "Coalesce", "write", 4200),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.TRANSFORMED", "TRANSFORM_CORE_WH", "Airflow", "write", 3100),
        ("RAW_ERP_DB", "RAW_ERP_DB.SAP_EXTRACT", "TRANSFORM_CORE_WH", "dbt", "read", 9600),
        ("RAW_ERP_DB", "RAW_ERP_DB.SAP_EXTRACT", "TRANSFORM_CORE_WH", "Coalesce", "read", 4000),
        ("RAW_ERP_DB", "RAW_ERP_DB.FINANCE_RAW", "TRANSFORM_CORE_WH", "Airflow", "read", 2900),

        # ── CLEANED_CRM_DB ───────────────────────────────────────────────
        ("CLEANED_CRM_DB", "CLEANED_CRM_DB.TRANSFORMED", "TRANSFORM_CORE_WH", "dbt", "write", 8600),
        ("CLEANED_CRM_DB", "CLEANED_CRM_DB.STAGING", "TRANSFORM_CORE_WH", "Matillion", "write", 3400),
        ("RAW_CRM_DB", "RAW_CRM_DB.SALESFORCE", "TRANSFORM_CORE_WH", "dbt", "read", 8400),
        ("RAW_CRM_DB", "RAW_CRM_DB.HUBSPOT", "TRANSFORM_CORE_WH", "Matillion", "read", 3200),

        # ── CLEANED_CLICKSTREAM_DB ───────────────────────────────────────
        ("CLEANED_CLICKSTREAM_DB", "CLEANED_CLICKSTREAM_DB.TRANSFORMED", "TRANSFORM_CORE_WH", "dbt", "write", 13200),
        ("CLEANED_CLICKSTREAM_DB", "CLEANED_CLICKSTREAM_DB.STAGING", "TRANSFORM_CORE_WH", "Snowpark", "write", 6100),
        ("RAW_CLICKSTREAM_DB", "RAW_CLICKSTREAM_DB.WEB_EVENTS", "TRANSFORM_CORE_WH", "dbt", "read", 12800),
        ("RAW_CLICKSTREAM_DB", "RAW_CLICKSTREAM_DB.MOBILE_EVENTS", "TRANSFORM_CORE_WH", "Snowpark", "read", 5900),

        # ── INTEGRATED_DB: cross-domain entity resolution ────────────────
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "TRANSFORM_HEAVY_WH", "dbt", "write", 7800),
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "TRANSFORM_HEAVY_WH", "Snowpark", "write", 4600),
        ("INTEGRATED_DB", "INTEGRATED_DB.GRAPH", "TRANSFORM_HEAVY_WH", "Coalesce", "write", 2800),
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "dbt", "read", 7200),
        ("CLEANED_CRM_DB", "CLEANED_CRM_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "dbt", "read", 6800),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "dbt", "read", 5400),
        ("CLEANED_CLICKSTREAM_DB", "CLEANED_CLICKSTREAM_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "dbt", "read", 6100),
        ("CLEANED_CRM_DB", "CLEANED_CRM_DB.STAGING", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 3800),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.STAGING", "TRANSFORM_HEAVY_WH", "Coalesce", "read", 2600),

        # ═══════════════════════════════════════════════════════════════════
        # GOLD LAYER — business-ready dimensional marts
        # ═══════════════════════════════════════════════════════════════════

        # ── FINANCE_MART_DB: revenue, billing, forecasting ───────────────
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "TRANSFORM_HEAVY_WH", "dbt", "write", 8200),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.BILLING", "TRANSFORM_HEAVY_WH", "Coalesce", "write", 3600),
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "TRANSFORM_HEAVY_WH", "dbt", "read", 7600),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "dbt", "read", 4800),

        # ── MARKETING_MART_DB: attribution, campaigns, funnel ────────────
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "TRANSFORM_HEAVY_WH", "dbt", "write", 7400),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.CAMPAIGNS", "TRANSFORM_HEAVY_WH", "Snowpark", "write", 3100),
        ("INTEGRATED_DB", "INTEGRATED_DB.GRAPH", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 4200),
        ("CLEANED_CLICKSTREAM_DB", "CLEANED_CLICKSTREAM_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "Snowpark", "read", 3600),

        # ── PRODUCT_MART_DB: usage metrics, feature adoption ─────────────
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "TRANSFORM_HEAVY_WH", "dbt", "write", 6800),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.FEATURE_ADOPTION", "TRANSFORM_HEAVY_WH", "Airflow", "write", 2400),
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.TRANSFORMED", "TRANSFORM_HEAVY_WH", "Airflow", "read", 2200),

        # ── EXECUTIVE_MART_DB: KPI roll-ups, board metrics ───────────────
        ("EXECUTIVE_MART_DB", "EXECUTIVE_MART_DB.KPIS", "TRANSFORM_CORE_WH", "dbt", "write", 4200),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "TRANSFORM_CORE_WH", "dbt", "read", 3800),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "TRANSFORM_CORE_WH", "dbt", "read", 3400),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "TRANSFORM_CORE_WH", "dbt", "read", 3100),

        # ═══════════════════════════════════════════════════════════════════
        # GOLD READS — BI, reporting, and analytics consumers
        # ═══════════════════════════════════════════════════════════════════

        # ── Finance BI ───────────────────────────────────────────────────
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "BI_FINANCE_WH", "Tableau", "read", 9800),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.BILLING", "BI_FINANCE_WH", "Power BI", "read", 7200),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "BI_FINANCE_WH", "Excel", "read", 4800),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "BI_FINANCE_WH", "Snowflake Web", "read", 3600),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.BILLING", "BI_FINANCE_WH", "Looker", "read", 2400),

        # ── Marketing BI ─────────────────────────────────────────────────
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "BI_MARKETING_WH", "Looker", "read", 8600),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.CAMPAIGNS", "BI_MARKETING_WH", "Sigma", "read", 5400),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "BI_MARKETING_WH", "Tableau", "read", 4200),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.CAMPAIGNS", "BI_MARKETING_WH", "Metabase", "read", 2800),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "BI_MARKETING_WH", "Python", "read", 1600),

        # ── Product BI ───────────────────────────────────────────────────
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "BI_PRODUCT_WH", "Looker", "read", 7200),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "BI_PRODUCT_WH", "Grafana", "read", 5600),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.FEATURE_ADOPTION", "BI_PRODUCT_WH", "Sigma", "read", 3400),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "BI_PRODUCT_WH", "Snowflake Web", "read", 2800),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.FEATURE_ADOPTION", "BI_PRODUCT_WH", "Metabase", "read", 1800),

        # ── Executive dashboards ─────────────────────────────────────────
        ("EXECUTIVE_MART_DB", "EXECUTIVE_MART_DB.KPIS", "BI_EXEC_WH", "Tableau", "read", 6200),
        ("EXECUTIVE_MART_DB", "EXECUTIVE_MART_DB.KPIS", "BI_EXEC_WH", "Power BI", "read", 5400),
        ("EXECUTIVE_MART_DB", "EXECUTIVE_MART_DB.KPIS", "BI_EXEC_WH", "MicroStrategy", "read", 3800),
        ("EXECUTIVE_MART_DB", "EXECUTIVE_MART_DB.KPIS", "BI_EXEC_WH", "Snowflake Web", "read", 2200),

        # ── Cross-mart analyst queries ───────────────────────────────────
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "ANALYST_WH", "DBeaver", "read", 2600),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.CAMPAIGNS", "ANALYST_WH", "DBeaver", "read", 1800),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "ANALYST_WH", "Python", "read", 3200),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.BILLING", "ANALYST_WH", "Python", "read", 2400),
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "ANALYST_WH", "DBeaver", "read", 1400),
        ("INTEGRATED_DB", "INTEGRATED_DB.GRAPH", "ANALYST_WH", "Snowflake Web", "read", 2200),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "ANALYST_WH", "Snowflake Web", "read", 1600),

        # ═══════════════════════════════════════════════════════════════════
        # ML / DATA SCIENCE
        # ═══════════════════════════════════════════════════════════════════

        # ── ML_FEATURES_DB: feature store and model outputs ──────────────
        ("ML_FEATURES_DB", "ML_FEATURES_DB.FEATURE_STORE", "ML_TRAINING_WH", "Snowpark", "write", 6200),
        ("ML_FEATURES_DB", "ML_FEATURES_DB.EXPERIMENTS", "ML_TRAINING_WH", "Python", "write", 8400),
        ("ML_FEATURES_DB", "ML_FEATURES_DB.FEATURE_STORE", "ML_TRAINING_WH", "Dataiku", "write", 4800),
        ("ML_FEATURES_DB", "ML_FEATURES_DB.EXPERIMENTS", "ML_TRAINING_WH", "Airflow", "write", 2600),
        # ML reads from gold + integrated
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "ML_TRAINING_WH", "Snowpark", "read", 5800),
        ("INTEGRATED_DB", "INTEGRATED_DB.GRAPH", "ML_TRAINING_WH", "Python", "read", 7600),
        ("PRODUCT_MART_DB", "PRODUCT_MART_DB.USAGE_METRICS", "ML_TRAINING_WH", "Dataiku", "read", 4200),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.ATTRIBUTION", "ML_TRAINING_WH", "Python", "read", 3400),
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.TRANSFORMED", "ML_TRAINING_WH", "Snowpark", "read", 4600),
        # Inference serving reads feature store
        ("ML_FEATURES_DB", "ML_FEATURES_DB.FEATURE_STORE", "ML_SERVING_WH", "Snowpark", "read", 14200),
        ("ML_FEATURES_DB", "ML_FEATURES_DB.FEATURE_STORE", "ML_SERVING_WH", "Python", "read", 9800),

        # ═══════════════════════════════════════════════════════════════════
        # REVERSE ETL — pushing data back to operational systems
        # ═══════════════════════════════════════════════════════════════════
        ("REVERSE_ETL_DB", "REVERSE_ETL_DB.SYNCS", "REVERSE_ETL_WH", "Fivetran", "write", 3800),
        ("REVERSE_ETL_DB", "REVERSE_ETL_DB.SYNCS", "REVERSE_ETL_WH", "Airflow", "write", 2600),
        ("MARKETING_MART_DB", "MARKETING_MART_DB.CAMPAIGNS", "REVERSE_ETL_WH", "Fivetran", "read", 3600),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.BILLING", "REVERSE_ETL_WH", "Airflow", "read", 2400),
        ("ML_FEATURES_DB", "ML_FEATURES_DB.FEATURE_STORE", "REVERSE_ETL_WH", "Fivetran", "read", 1800),
        # Salesforce sync
        ("REVERSE_ETL_DB", "REVERSE_ETL_DB.SYNCS", "REVERSE_ETL_WH", "Salesforce", "read", 4200),

        # ═══════════════════════════════════════════════════════════════════
        # GOVERNANCE & OBSERVABILITY
        # ═══════════════════════════════════════════════════════════════════
        ("GOVERNANCE_DB", "GOVERNANCE_DB.POLICIES", "ADMIN_WH", "Snowflake Web", "write", 3200),
        ("GOVERNANCE_DB", "GOVERNANCE_DB.AUDIT", "ADMIN_WH", "Airflow", "write", 1800),
        ("GOVERNANCE_DB", "GOVERNANCE_DB.AUDIT", "ADMIN_WH", "Datadog", "read", 4600),
        ("GOVERNANCE_DB", "GOVERNANCE_DB.POLICIES", "ADMIN_WH", "Snowflake Web", "read", 6800),
        ("GOVERNANCE_DB", "GOVERNANCE_DB.AUDIT", "ADMIN_WH", "New Relic", "read", 2200),
        ("GOVERNANCE_DB", "GOVERNANCE_DB.AUDIT", "ADMIN_WH", "Splunk", "read", 1400),
        # Observability reads across all layers
        ("RAW_EVENTS_DB", "RAW_EVENTS_DB.STREAMING", "ADMIN_WH", "Datadog", "read", 1200),
        ("CLEANED_EVENTS_DB", "CLEANED_EVENTS_DB.TRANSFORMED", "ADMIN_WH", "Datadog", "read", 900),
        ("FINANCE_MART_DB", "FINANCE_MART_DB.REVENUE", "ADMIN_WH", "Datadog", "read", 800),

        # ═══════════════════════════════════════════════════════════════════
        # SANDBOX — ad-hoc dev/exploration
        # ═══════════════════════════════════════════════════════════════════
        ("SANDBOX_DB", "SANDBOX_DB.PUBLIC", "ANALYST_WH", "DBeaver", "write", 1800),
        ("SANDBOX_DB", "SANDBOX_DB.PUBLIC", "ANALYST_WH", "Snowflake Web", "write", 3200),
        ("SANDBOX_DB", "SANDBOX_DB.SCRATCH", "ANALYST_WH", "Python", "write", 2600),
        ("SANDBOX_DB", "SANDBOX_DB.PUBLIC", "ANALYST_WH", "DBeaver", "read", 2200),
        ("SANDBOX_DB", "SANDBOX_DB.PUBLIC", "ANALYST_WH", "Snowflake Web", "read", 4800),
        ("SANDBOX_DB", "SANDBOX_DB.SCRATCH", "ANALYST_WH", "Python", "read", 3400),
        ("SANDBOX_DB", "SANDBOX_DB.SCRATCH", "ANALYST_WH", "VSCode", "read", 1200),
        ("SANDBOX_DB", "SANDBOX_DB.PUBLIC", "ANALYST_WH", "VSCode", "write", 800),
        # Sandbox users reading silver for experimentation
        ("CLEANED_CRM_DB", "CLEANED_CRM_DB.TRANSFORMED", "ANALYST_WH", "DBeaver", "read", 900),
        ("CLEANED_ERP_DB", "CLEANED_ERP_DB.TRANSFORMED", "ANALYST_WH", "Python", "read", 1100),
        ("INTEGRATED_DB", "INTEGRATED_DB.ENTITIES", "ANALYST_WH", "Python", "read", 1600),
    ]

    n = len(rows)
    databases, schemas, warehouses, clients, directions, counts = zip(*rows)

    sample_data = {
        "ORGANIZATION_NAME": [org] * n,
        "ACCOUNT_NAME": [current_account] * n,
        "DATABASE": list(databases),
        "SCHEMA_NAME": list(schemas),
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
def load_data(_session) -> pd.DataFrame:
    """Load access data from Snowflake or fall back to sample data.

    Attempts to query the ``data_lake_access_30d`` table via the provided
    Snowpark session.  On failure (no session, empty result, or SQL error)
    returns ``sample_dataframe()`` instead.  Cached for 5 minutes.

    Args:
        session: A Snowpark ``Session`` object, or ``None`` for local dev.

    Returns:
        A DataFrame with the standard 8-column schema.
    """
    if _session is None:
        logger.info("No Snowflake session — loading sample data")
        return sample_dataframe(_session)
    try:
        ensure_tables_exist(_session)
        query = """
            SELECT account_id AS ACCOUNT_NAME, * 
            FROM CONNECTION_EXPLORER_APP_DB.APP.data_lake_access_30d 
            ORDER BY access_count DESC;
        """

        logger.info("Querying Snowflake for access data")
        result_df = _session.sql(query).to_pandas()
        if result_df.empty:
            logger.warning("Snowflake query returned empty — falling back to sample data")
            st.warning("No data found. Using sample data.")
            return sample_dataframe(_session)
        logger.info("Loaded %d rows from Snowflake", len(result_df))
        return result_df
    except Exception as exc:
        logger.error("Snowflake query failed: %s", exc)
        st.error(f"Unable to query account usage data. Falling back to sample data.\n\nError: {exc}")
        return sample_dataframe(_session)


@st.cache_data(show_spinner=False)
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and aggregate the raw access DataFrame.

    Drops rows with any null values and groups by the core dimensions to produce
    a single summed ACCESS_COUNT per unique combination.

    Args:
        df: Raw DataFrame from ``load_data``.

    Returns:
        A cleaned, aggregated DataFrame sorted by ACCESS_COUNT descending.
        Returns the input unchanged if it is empty.
    """
    if df.empty:
        return df
    df = df.dropna(how="any", axis=0)
    # Clean up "Snowflake Web App (feature_name)" → show only the feature name
    df = df.copy()
    mask = df["CLIENT"].str.startswith("Snowflake Web App (")
    extracted = df.loc[mask, "CLIENT"].str.extract(r'\(([^)]+)\)', expand=False)
    # Register extracted names so they get the snowflake icon
    for name in extracted.dropna().unique():
        if name not in CLIENT_ICON_FILES:
            CLIENT_ICON_FILES[name] = "snowflake.svg"
    df.loc[mask, "CLIENT"] = extracted
    df = (
        df.groupby(
            ["DATABASE", "SCHEMA_NAME", "WAREHOUSE", "CLIENT", "DIRECTION", "ORGANIZATION_NAME", "ACCOUNT_NAME"],
            as_index=False,
        )
        .agg(ACCESS_COUNT=pd.NamedAgg(column="ACCESS_COUNT", aggfunc="sum"))
        .sort_values(by="ACCESS_COUNT", ascending=False)
    )
    return df


def apply_filters(
    df: pd.DataFrame,
    database_names: Sequence[str],
    schema_names: Sequence[str],
    warehouse_names: Sequence[str],
    client_names: Sequence[str],
    org_filter: str,
    direction_filters: Sequence[str],
    access_count: int,
) -> pd.DataFrame:
    """Apply sidebar filter selections to the processed DataFrame.

    All filter parameters are optional in the sense that empty sequences
    or empty strings mean "no filter on this dimension".  Results are
    cached by Streamlit so identical filter combos are instant.

    Args:
        df: The processed DataFrame to filter.
        database_names: Database names to include (empty = all).
        schema_names: Schema names to include (empty = all).
        warehouse_names: Warehouse names to include (empty = all).
        client_names: Client names to include (empty = all).
        org_filter: Substring match on ORGANIZATION_NAME (empty = all).
        direction_filters: Direction values to include (empty = all).
        access_count: Minimum ACCESS_COUNT threshold (exclusive).

    Returns:
        A filtered copy of *df*.  Returns the input unchanged if empty.
    """
    if df.empty:
        return df

    mask = df["ACCESS_COUNT"] > access_count

    if database_names:
        mask &= df["DATABASE"].astype(str).isin(list(database_names))
    if schema_names:
        mask &= df["SCHEMA_NAME"].astype(str).isin(list(schema_names))
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
    """Return sorted unique values from a DataFrame column.

    Used to populate sidebar multiselect filter options.

    Args:
        df: The source DataFrame.
        column: Column name to extract distinct values from.

    Returns:
        A sorted list of unique string values, or an empty list if *df*
        is empty.
    """
    if df.empty:
        return []
    return sorted(df[column].astype(str).unique().tolist())