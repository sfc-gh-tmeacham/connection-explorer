"""Auto-setup: create and seed Snowflake objects on first run.

Creates the ``CONNECTION_EXPLORER_APP_DB.APP`` schema, the
``connection_access_30d`` access table, and the ``client_app_classification``
lookup table if they do not already exist.  The classification table is
seeded via MERGE from ``CLIENT_MAPPINGS`` so re-runs are idempotent.
"""

import logging

import streamlit as st

logger = logging.getLogger(__name__)

from components.client_mappings import CLIENT_MAPPINGS

DB = "CONNECTION_EXPLORER_APP_DB"
SCHEMA = "APP"
FQ_SCHEMA = f"{DB}.{SCHEMA}"
FQ_ACCESS_TABLE = f"{FQ_SCHEMA}.connection_access_30d"
FQ_CLASSIFICATION_TABLE = f"{FQ_SCHEMA}.client_app_classification"


@st.cache_data(show_spinner=False, ttl=3600)
def ensure_tables_exist(_session) -> None:
    """Create required Snowflake objects and seed the classification lookup.

    Idempotent -- safe to call on every app load.  Runs once per hour via
    ``st.cache_data`` TTL.  If the session lacks required privileges, a
    warning is shown and execution continues gracefully.

    Args:
        session: A Snowpark ``Session`` object, or ``None`` (in which case
            the function returns immediately).
    """
    if _session is None:
        return

    try:
        # --- database & schema ---
        _session.sql(f"CREATE DATABASE IF NOT EXISTS {DB}").collect()
        _session.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ_SCHEMA}").collect()

        # --- access data table ---
        _session.sql(f"""
            CREATE TABLE IF NOT EXISTS {FQ_ACCESS_TABLE} (
                organization_name VARCHAR,
                account_id        VARCHAR,
                client            VARCHAR,
                warehouse         VARCHAR,
                database          VARCHAR,
                schema_name       VARCHAR,
                direction         VARCHAR,
                access_count      NUMBER
            )
        """).collect()

        # --- classification lookup table ---
        _session.sql(f"""
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

        _session.sql(f"""
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
        logger.error("Auto-setup failed: %s", exc)
        st.warning(f"Auto-setup could not create tables (may lack privileges): {exc}")
