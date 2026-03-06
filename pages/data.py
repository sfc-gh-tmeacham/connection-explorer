"""Data page — interactive table view with filtering and grouping."""

import streamlit as st


def run():
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    st.markdown("### Raw Access Data")
    st.markdown(
        "Use the column headers to sort, filter, and search. "
        "Click column menu (⋮) for grouping options."
    )

    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "ORGANIZATION_NAME": st.column_config.TextColumn("Organization"),
            "ACCOUNT_ID": st.column_config.TextColumn("Account"),
            "CLIENT": st.column_config.TextColumn("Client"),
            "WAREHOUSE": st.column_config.TextColumn("Warehouse"),
            "DATABASE": st.column_config.TextColumn("Database"),
            "SCHEMA_NAME": st.column_config.TextColumn("Schema"),
            "DIRECTION": st.column_config.TextColumn("Direction"),
            "ACCESS_COUNT": st.column_config.NumberColumn(
                "Access Count", format="%d"
            ),
        },
    )

    st.caption(f"{len(df):,} rows")
