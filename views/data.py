"""Data page — interactive table view with filtering and grouping."""

import streamlit as st

from components.client_mappings import generate_client_icon_uri

# Dimensions available for the "Group by" multiselect.  Maps user-friendly
# labels to actual DataFrame column names.  ACCESS_COUNT is excluded because
# it's the measure that gets summed during aggregation.
GROUP_COLUMNS = {
    "Client": "CLIENT",
    "Warehouse": "WAREHOUSE",
    "Database": "DATABASE",
    "Schema": "SCHEMA_NAME",
    "Direction": "DIRECTION",
}

# Display order for table columns.  ICON is a generated image column prepended
# when CLIENT is present; the rest follow the data's logical flow from client
# through warehouse/database/schema to the access metric.
COLUMN_ORDER = ["ICON", "CLIENT", "WAREHOUSE", "DATABASE", "SCHEMA_NAME", "DIRECTION", "ACCESS_COUNT"]


def run():
    """Render the Data page with an interactive table and optional grouping.

    Reads the filtered DataFrame from ``st.session_state["filtered_df"]``
    and displays it as a Streamlit dataframe.  A multiselect widget allows
    grouping by one or more dimensions (Client, Warehouse, Database,
    Schema, Direction), with access counts summed per group.  Client icons
    are prepended when the CLIENT column is present.
    """
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    st.markdown("### Access Data")

    # --- Group-by selector ---
    # Lets the user pivot the table by one or more dimensions.  When no
    # groups are selected the raw filtered rows are shown as-is.
    group_by_labels = st.multiselect(
        "Group by",
        options=list(GROUP_COLUMNS.keys()),
        default=[],
        help="Select columns to group by. Access counts will be summed.",
    )

    # Translate user-friendly labels ("Client") back to DataFrame column names
    # ("CLIENT") for the groupby operation.
    group_by_cols = [GROUP_COLUMNS[label] for label in group_by_labels]

    if group_by_cols:
        # Aggregate: sum ACCESS_COUNT per unique combination of selected
        # dimensions, then sort highest-access groups first.
        display_df = (
            df.groupby(group_by_cols, as_index=False)["ACCESS_COUNT"]
            .sum()
            .sort_values("ACCESS_COUNT", ascending=False)
        )
        row_label = "groups"
    else:
        display_df = df.copy()
        row_label = "rows"

    # Prepend a small icon column when CLIENT is present.  The icon is a
    # data-URI SVG generated from the client name (e.g. Snowpark, dbt, etc.).
    if "CLIENT" in display_df.columns:
        display_df["ICON"] = display_df["CLIENT"].apply(generate_client_icon_uri)

    # Reorder columns to match the logical data flow defined in COLUMN_ORDER,
    # dropping any that aren't present (e.g. ICON when CLIENT was grouped away).
    ordered_cols = [c for c in COLUMN_ORDER if c in display_df.columns]
    display_df = display_df[ordered_cols]

    # Column config maps DataFrame columns to Streamlit column types for
    # rendering (image for icons, formatted number for access counts, etc.).
    column_config = {
        "ICON": st.column_config.ImageColumn("", width="small"),
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
