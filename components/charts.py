"""Plotly bar charts and Sankey diagrams.

Renders interactive visualizations below the network graph: horizontal
bar charts for top databases, warehouses, and clients by access count,
plus read/write Sankey flow diagrams.
"""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.theme import SNOWFLAKE_BLUE, STAR_BLUE, AMBER, is_dark_theme


@st.cache_data(show_spinner=False)
def prepare_chart_data(df: pd.DataFrame, column: str, top_n: int = 10) -> pd.DataFrame:
    """Aggregate access counts by a single dimension and return the top N.

    Args:
        df: The filtered access DataFrame.
        column: Column name to group by (e.g. ``"CLIENT"``).
        top_n: Maximum number of rows to return.

    Returns:
        A DataFrame with columns ``[column.title(), "Access Count"]`` sorted
        descending by Access Count, or an empty DataFrame if *df* is empty.
    """
    if df.empty:
        return pd.DataFrame()

    return (
        df[[column, "ACCESS_COUNT"]]
        .groupby(column)["ACCESS_COUNT"]
        .sum()
        .reset_index()
        .rename(columns={column: column.title(), "ACCESS_COUNT": "Access Count"})
        .sort_values("Access Count", ascending=False)
        .head(top_n)
    )


def _build_bar_chart(
    data: pd.DataFrame,
    column: str,
    bar_color: str,
    grid_color: str,
    height: int = 350,
) -> go.Figure | None:
    """Build a horizontal Plotly bar chart matching the Snowflake theme.

    Text colors are intentionally omitted so that Streamlit's built-in Plotly
    theme integration (``theme="streamlit"``, the default) can inject the
    correct light/dark text color at render time.

    Args:
        data: Aggregated DataFrame from ``prepare_chart_data``.
        column: The dimension column name (e.g. ``"CLIENT"``).
        bar_color: CSS hex color for the bars.
        grid_color: CSS rgba color for the axis grid lines.
        height: Chart height in pixels.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *data* is empty.
    """
    if data.empty:
        return None

    # Reverse so highest value is at top (Plotly draws bottom-up)
    data = data.sort_values("Access Count", ascending=True)

    fig = go.Figure(go.Bar(
        x=data["Access Count"],
        y=data[column.title()],
        orientation="h",
        marker=dict(color=bar_color, cornerradius=4),
        hovertemplate="%{y}<br>Access Count: %{x:,}<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text=f"Access Count by {column.title()}",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        xaxis=dict(
            title=dict(text="Access Count"),
            gridcolor=grid_color,
            showline=True,
            linecolor=grid_color,
        ),
        yaxis=dict(
            title=dict(text=column.title()),
            gridcolor=grid_color,
            showline=True,
            linecolor=grid_color,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=50, b=40),
        height=height,
        bargap=0.15,
    )
    return fig


def _build_sankey(df: pd.DataFrame, direction: str) -> go.Figure | None:
    """Build a Sankey flow diagram for a single data-flow direction.

    For writes, flows run client (left) to database (right).  For reads,
    flows run database (left) to client (right).  Nodes on each side are
    sorted by total volume descending.

    Args:
        df: The filtered access DataFrame (all directions included; this
            function filters internally).
        direction: ``"read"`` or ``"write"``.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if no rows match
        the requested direction.
    """
    subset = df[df["DIRECTION"] == direction]
    if subset.empty:
        return None

    flows = (
        subset.groupby(["CLIENT", "DATABASE"], as_index=False)["ACCESS_COUNT"]
        .sum()
        .sort_values("ACCESS_COUNT", ascending=False)
    )

    # Sort source nodes (left side) by total volume descending.
    # Writes: clients on left → databases on right
    # Reads:  databases on left → clients on right
    if direction == "write":
        left_col, right_col = "CLIENT", "DATABASE"
    else:
        left_col, right_col = "DATABASE", "CLIENT"

    left_totals = flows.groupby(left_col)["ACCESS_COUNT"].sum().sort_values(ascending=False)
    right_totals = flows.groupby(right_col)["ACCESS_COUNT"].sum().sort_values(ascending=False)
    left_nodes = left_totals.index.tolist()
    right_nodes = right_totals.index.tolist()

    labels = left_nodes + right_nodes
    left_idx = {name: i for i, name in enumerate(left_nodes)}
    right_idx = {name: i + len(left_nodes) for i, name in enumerate(right_nodes)}

    sources, targets, values = [], [], []
    for _, row in flows.iterrows():
        li = left_idx[row[left_col]]
        ri = right_idx[row[right_col]]
        sources.append(li)
        targets.append(ri)
        values.append(row["ACCESS_COUNT"])

    if direction == "write":
        link_color = "rgba(245, 166, 35, 0.35)"
        node_colors = [AMBER] * len(left_nodes) + [SNOWFLAKE_BLUE] * len(right_nodes)
        title = "Write Flows — Client → Database"
    else:
        link_color = "rgba(41, 181, 232, 0.35)"
        node_colors = [SNOWFLAKE_BLUE] * len(left_nodes) + [AMBER] * len(right_nodes)
        title = "Read Flows — Database → Client"

    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=12,
            thickness=18,
            label=labels,
            color=node_colors,
            line=dict(width=0),
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=[link_color] * len(values),
        ),
    ))

    fig.update_layout(
        title=dict(
            text=title,
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=50, b=20),
        height=600,
    )
    return fig


def render_sankey(df: pd.DataFrame) -> None:
    """Render side-by-side Sankey diagrams for read and write flows.

    Args:
        df: The filtered access DataFrame.  No-ops if empty.
    """
    if df.empty:
        return

    write_fig = _build_sankey(df, "write")
    read_fig = _build_sankey(df, "read")

    col1, col2 = st.columns(2)
    with col1:
        if write_fig:
            st.plotly_chart(write_fig, width="stretch")
    with col2:
        if read_fig:
            st.plotly_chart(read_fig, width="stretch")


def render_bar_charts(df: pd.DataFrame) -> None:
    """Render bar charts for top clients, databases, and warehouses.

    Displays two side-by-side charts (Client, Database) followed by a
    full-width Warehouse chart, then the Sankey diagrams.  Adapts bar
    and grid colors to the current Streamlit theme.

    Args:
        df: The filtered access DataFrame.  Shows a warning if empty.
    """
    if df.empty:
        st.warning("Apply different filters or load more data to see charts.")
        return

    is_dark = is_dark_theme()
    bar_color = STAR_BLUE if is_dark else SNOWFLAKE_BLUE
    grid_color = "rgba(255,255,255,0.18)" if is_dark else "rgba(0,0,0,0.10)"

    client_data = prepare_chart_data(df, "CLIENT")
    db_data = prepare_chart_data(df, "DATABASE")
    wh_data = prepare_chart_data(df, "WAREHOUSE")

    col1, col2 = st.columns(2)
    with col1:
        fig = _build_bar_chart(client_data, "CLIENT", bar_color, grid_color)
        if fig:
            st.plotly_chart(fig, width="stretch")
    with col2:
        fig = _build_bar_chart(db_data, "DATABASE", bar_color, grid_color)
        if fig:
            st.plotly_chart(fig, width="stretch")

    fig = _build_bar_chart(wh_data, "WAREHOUSE", bar_color, grid_color)
    if fig:
        st.plotly_chart(fig, width="stretch")

    render_sankey(df)
