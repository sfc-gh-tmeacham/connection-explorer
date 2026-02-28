"""Auto-setup: create and seed Snowflake objects on first run.

Creates the ``SNOWFLAKE_DATA_LAKE.DATA_LAKE_ACCESS`` schema, the
``data_lake_access_30d`` access table, and the ``client_app_classification``
lookup table if they do not already exist.  The classification table is
seeded via MERGE from ``CLIENT_MAPPINGS`` so re-runs are idempotent.
"""

import streamlit as st

from components.client_mappings import CLIENT_MAPPINGS

DB = "SNOWFLAKE_DATA_LAKE"
SCHEMA = "DATA_LAKE_ACCESS"
FQ_SCHEMA = f"{DB}.{SCHEMA}"
FQ_ACCESS_TABLE = f"{FQ_SCHEMA}.data_lake_access_30d"
FQ_CLASSIFICATION_TABLE = f"{FQ_SCHEMA}.client_app_classification"


@st.cache_data(show_spinner=False, ttl=3600)
def ensure_tables_exist(session) -> None:
    """Create required Snowflake objects and seed the classification lookup.

    Idempotent -- safe to call on every app load.  Runs once per hour via
    ``st.cache_data`` TTL.  If the session lacks required privileges, a
    warning is shown and execution continues gracefully.

    Args:
        session: A Snowpark ``Session`` object, or ``None`` (in which case
            the function returns immediately).
    """
    if session is None:
        return

    try:
        # --- database & schema ---
        session.sql(f"CREATE DATABASE IF NOT EXISTS {DB}").collect()
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}").collect()

        # --- access data table ---
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {FQ_ACCESS_TABLE} (
                organization_name VARCHAR,
                account_id        VARCHAR,
                client            VARCHAR,
                warehouse         VARCHAR,
                database          VARCHAR,
                direction         VARCHAR,
                access_count      NUMBER
            )
        """).collect()

        # --- classification lookup table ---
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {FQ_CLASSIFICATION_TABLE} (
                priority       NUMBER,
                pattern        VARCHAR,
                source_field   VARCHAR,
                display_name   VARCHAR
            )
        """).collect()

        # Seed: MERGE so re-runs don't duplicate rows.
        # Build a VALUES clause from CLIENT_MAPPINGS.
        values_rows = ",\n".join(
            f"({i}, '{pat}', '{src}', '{name}')"
            for i, (pat, src, name) in enumerate(CLIENT_MAPPINGS)
        )

        session.sql(f"""
            MERGE INTO {FQ_CLASSIFICATION_TABLE} AS tgt
            USING (
                SELECT
                    column1 AS priority,
                    column2 AS pattern,
                    column3 AS source_field,
                    column4 AS display_name
                FROM VALUES
                    {values_rows}
            ) AS src
            ON tgt.pattern = src.pattern AND tgt.source_field = src.source_field
            WHEN NOT MATCHED THEN INSERT (priority, pattern, source_field, display_name)
                VALUES (src.priority, src.pattern, src.source_field, src.display_name)
        """).collect()

    except Exception as exc:
        st.warning(f"Auto-setup could not create tables (may lack privileges): {exc}")
