"""Auto-setup: ensure Snowflake tables exist and seed defaults on first run.

Detects the current database and schema from the active Snowpark session
(works in both Streamlit-in-Snowflake and local dev) and creates the
``connection_access_30d`` access table and ``client_app_classification``
lookup table if they do not already exist.  The classification table is
seeded via MERGE from ``CLIENT_MAPPINGS`` so re-runs are idempotent.
"""

import logging

import streamlit as st

logger = logging.getLogger(__name__)

from components.client_mappings import CLIENT_MAPPINGS

# Fallback database/schema used when no Snowflake session is available
# (demo mode).  These match the deploy script defaults so that references
# in log messages and sample-mode paths stay meaningful.
_FALLBACK_DB = "CONNECTION_EXPLORER_APP_DB"
_FALLBACK_SCHEMA = "APP"


@st.cache_data(show_spinner=False, ttl=3600)
def get_fq_names(_session) -> dict:
    """Derive fully-qualified table names from the session's current context.

    In Streamlit-in-Snowflake the session's database/schema is set by the
    deployment target.  Locally, it comes from the connection config in
    ``secrets.toml`` or ``connections.toml``.  This avoids hardcoding a
    database name that may differ across installations.

    Cached for 1 hour — the database/schema context doesn't change mid-session.

    Args:
        _session: A Snowpark ``Session`` object, or ``None`` (demo mode).

    Returns:
        A dict with keys ``"schema"`` (``DB.SCHEMA``), ``"access_table"``,
        and ``"classification_table"`` containing fully-qualified names.
    """
    if _session is None:
        fq_schema = f"{_FALLBACK_DB}.{_FALLBACK_SCHEMA}"
        return {
            "schema": fq_schema,
            "access_table": f"{fq_schema}.connection_access_30d",
            "classification_table": f"{fq_schema}.client_app_classification",
        }

    row = _session.sql("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()").collect()[0]
    db, schema = row[0], row[1]
    fq_schema = f"{db}.{schema}"
    logger.info("Resolved FQ schema: %s", fq_schema)
    return {
        "schema": fq_schema,
        "access_table": f"{fq_schema}.connection_access_30d",
        "classification_table": f"{fq_schema}.client_app_classification",
    }


@st.cache_data(show_spinner=False, ttl=3600)
def ensure_tables_exist(_session) -> None:
    """Create required tables and seed the classification lookup.

    Idempotent — safe to call on every app load.  Runs once per hour via
    ``st.cache_data`` TTL.  The database and schema are assumed to already
    exist (created by the deploy script or the SiS deployment target).
    If the session lacks required privileges, a warning is shown and
    execution continues gracefully.

    Args:
        _session: A Snowpark ``Session`` object, or ``None`` (in which case
            the function returns immediately).
    """
    if _session is None:
        return

    names = get_fq_names(_session)
    fq_access = names["access_table"]
    fq_classification = names["classification_table"]

    try:
        # --- Access data table ---
        # Stores the 30-day connection access rollup produced by the
        # REFRESH_CONNECTION_ACCESS() stored procedure.
        _session.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq_access} (
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

        # --- Classification lookup table ---
        # Maps raw CLIENT_APPLICATION_ID / APPLICATION values to friendly
        # display names via ILIKE patterns.  Priority determines evaluation
        # order (lower = first match wins).
        _session.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq_classification} (
                priority       NUMBER,
                pattern        VARCHAR,
                source_field   VARCHAR,
                display_name   VARCHAR
            )
        """).collect()

        # --- Seed classification rules ---
        # Uses MERGE (match on pattern + source_field) so re-runs are
        # idempotent: existing rows are left untouched, only missing rules
        # from CLIENT_MAPPINGS are inserted.  This preserves any user edits
        # made via the Classifications page.
        values_rows = ",\n".join(
            f"({i}, '{pat}', '{src}', '{name}')"
            for i, (pat, src, name) in enumerate(CLIENT_MAPPINGS)
        )

        _session.sql(f"""
            MERGE INTO {fq_classification} AS tgt
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
