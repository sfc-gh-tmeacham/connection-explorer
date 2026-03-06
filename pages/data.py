"""Data page — interactive table view with filtering and grouping."""

import streamlit as st

# Columns available for grouping (exclude ACCESS_COUNT since it's the measure)
GROUP_COLUMNS = {
    "Client": "CLIENT",
    "Warehouse": "WAREHOUSE",
    "Database": "DATABASE",
    "Schema": "SCHEMA_NAME",
    "Direction": "DIRECTION",
}


def run():
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    st.markdown("### Access Data")

    # Group-by selector
    group_by_labels = st.multiselect(
        "Group by",
        options=list(GROUP_COLUMNS.keys()),
        default=[],
        help="Select columns to group by. Access counts will be summed.",
    )

    # Map labels to column names
    group_by_cols = [GROUP_COLUMNS[label] for label in group_by_labels]

    if group_by_cols:
        # Aggregate data by selected columns
        display_df = (
            df.groupby(group_by_cols, as_index=False)["ACCESS_COUNT"]
            .sum()
            .sort_values("ACCESS_COUNT", ascending=False)
        )
        row_label = "groups"
    else:
        display_df = df
        row_label = "rows"

    # Build column config for visible columns
    column_config = {
        "CLIENT": st.column_config.TextColumn("Client"),
        "WAREHOUSE": st.column_config.TextColumn("Warehouse"),
        "DATABASE": st.column_config.TextColumn("Database"),
        "SCHEMA_NAME": st.column_config.TextColumn("Schema (DB.SCHEMA)"),
        "DIRECTION": st.column_config.TextColumn("Direction"),
        "ACCESS_COUNT": st.column_config.NumberColumn("Access Count", format="%d"),
    }

    # Only include columns that exist in the display dataframe
    visible_config = {k: v for k, v in column_config.items() if k in display_df.columns}

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config=visible_config,
    )

    total_access = display_df["ACCESS_COUNT"].sum()
    st.caption(f"{len(display_df):,} {row_label} · {total_access:,} total accesses")
