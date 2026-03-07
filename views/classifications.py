"""Classifications page — view and edit client app classification rules."""

import pandas as pd
import streamlit as st

FQ_TABLE = "CONNECTION_EXPLORER_APP_DB.APP.client_app_classification"

# Sample data shown when no Snowflake session is available
SAMPLE_DATA = pd.DataFrame(
    {
        "PRIORITY": [0, 1, 2, 3, 4],
        "PATTERN": ["%snowpark%", "%PythonSnowpark%", "%SnowparkML%", "%SNOWPARK%", "%deployments%"],
        "SOURCE_FIELD": ["client_app_id", "application", "application", "application", "application"],
        "DISPLAY_NAME": ["Snowpark", "Python Snowpark", "SnowparkML", "Snowpark", "Kafka"],
    }
)


def _load_classifications(session) -> pd.DataFrame:
    """Fetch all classification rows ordered by priority."""
    return session.sql(f"SELECT * FROM {FQ_TABLE} ORDER BY PRIORITY").to_pandas()


def _save_classifications(session, df: pd.DataFrame) -> int:
    """Replace the classification table contents and return the new row count.

    Uses a transactional DELETE + INSERT approach so the table is never in a
    partially-updated state visible to other sessions.
    """
    session.sql("BEGIN").collect()
    try:
        session.sql(f"DELETE FROM {FQ_TABLE}").collect()

        if not df.empty:
            # Build a VALUES clause from the DataFrame
            rows = []
            for _, row in df.iterrows():
                pri = int(row["PRIORITY"])
                pat = str(row["PATTERN"]).replace("'", "''")
                src = str(row["SOURCE_FIELD"]).replace("'", "''")
                name = str(row["DISPLAY_NAME"]).replace("'", "''")
                rows.append(f"({pri}, '{pat}', '{src}', '{name}')")

            values_clause = ",\n".join(rows)
            session.sql(f"""
                INSERT INTO {FQ_TABLE} (PRIORITY, PATTERN, SOURCE_FIELD, DISPLAY_NAME)
                VALUES {values_clause}
            """).collect()

        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

    return len(df)


def run():
    st.markdown("### Client App Classifications")

    st.info(
        "**What is this table?**\n\n"
        "Classification rules map raw Snowflake `CLIENT_APPLICATION_ID` and "
        "`APPLICATION` values to friendly display names shown throughout this app. "
        "The `REFRESH_CONNECTION_ACCESS()` stored procedure joins against this "
        "table during each refresh to classify connection data.\n\n"
        "Each rule has:\n\n"
        "- **Priority** — Lower numbers are evaluated first. The first matching "
        "rule wins.\n"
        "- **Pattern** — A SQL `ILIKE` pattern (use `%` as wildcard, matching is "
        "case-insensitive) matched against the source field.\n"
        "- **Source Field** — Which column to match: `client_app_id` or "
        "`application`.\n"
        "- **Display Name** — The friendly label shown in charts and tables.\n\n"
        "Edit the table below and click **Save Changes** to persist updates to "
        "Snowflake. New rows can be added and existing rows deleted using the "
        "controls in the table. After saving, click **Refresh Data** to re-run "
        "the stored procedure so your changes are reflected in the access data.",
        icon=":material/info:",
    )

    session = st.session_state.get("snowflake_session")

    if session is None:
        st.warning("No Snowflake session — showing sample data (read-only).")
        st.dataframe(SAMPLE_DATA, hide_index=True, use_container_width=True)
        return

    # Load current data
    if "classifications_df" not in st.session_state:
        st.session_state["classifications_df"] = _load_classifications(session)

    col_config = {
        "PRIORITY": st.column_config.NumberColumn("Priority", help="Lower = higher priority", min_value=0, step=1, width="small"),
        "PATTERN": st.column_config.TextColumn("Pattern", help="SQL ILIKE pattern (use % as wildcard, case-insensitive)", width="large"),
        "SOURCE_FIELD": st.column_config.SelectboxColumn(
            "Source Field",
            help="Column to match against",
            options=["client_app_id", "application"],
            width="medium",
        ),
        "DISPLAY_NAME": st.column_config.TextColumn("Display Name", help="Friendly label for charts and tables", width="medium"),
    }

    edited_df = st.data_editor(
        st.session_state["classifications_df"],
        column_config=col_config,
        num_rows="dynamic",
        hide_index=True,
        use_container_width=True,
        key="classifications_editor",
    )

    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        save_clicked = st.button("Save Changes", type="primary", icon=":material/save:", help="Persist edits to the classification table in Snowflake")
    with col2:
        refresh_clicked = st.button(
            "Refresh Data",
            help="Re-run REFRESH_CONNECTION_ACCESS() to apply classification changes to access data",
            icon=":material/sync:",
        )
    with col3:
        reload_clicked = st.button("Reload", help="Discard edits and reload from Snowflake", icon=":material/refresh:")

    if save_clicked:
        # Validate: no empty patterns or display names
        if edited_df["PATTERN"].isna().any() or (edited_df["PATTERN"] == "").any():
            st.error("All rows must have a Pattern value.")
            return
        if edited_df["DISPLAY_NAME"].isna().any() or (edited_df["DISPLAY_NAME"] == "").any():
            st.error("All rows must have a Display Name value.")
            return

        try:
            count = _save_classifications(session, edited_df)
            st.session_state["classifications_df"] = edited_df.copy()
            st.success(f"Saved {count} classification rules to Snowflake.")
        except Exception as exc:
            st.error(f"Failed to save: {exc}")

    if refresh_clicked:
        with st.spinner("Running REFRESH_CONNECTION_ACCESS()... this may take a minute."):
            try:
                result = session.sql("CALL REFRESH_CONNECTION_ACCESS()").collect()
                msg = result[0][0] if result else "Procedure completed."
                st.success(msg)
                # Clear cached data so the app reloads fresh access data
                st.cache_data.clear()
            except Exception as exc:
                st.error(f"Refresh failed: {exc}")

    if reload_clicked:
        st.session_state["classifications_df"] = _load_classifications(session)
        st.rerun()

    st.caption(f"{len(edited_df)} rules")
