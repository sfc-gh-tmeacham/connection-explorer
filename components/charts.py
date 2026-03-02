"""Plotly charts: bar charts, heatmap, treemap, and Sankey diagrams.

Renders interactive visualizations below the network graph: horizontal
bar charts for top databases, warehouses, and clients by access count,
a Client x Database heatmap, a hierarchical treemap, and read/write
Sankey flow diagrams.
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

logger = logging.getLogger(__name__)

from components.theme import SNOWFLAKE_BLUE, STAR_BLUE, AMBER, READ_GREEN, is_dark_theme


@st.cache_data(show_spinner=False)
def prepare_chart_data(df: pd.DataFrame, column: str, top_n: int = 10) -> pd.DataFrame:
    """Aggregate access counts by a dimension and direction for stacked bars.

    Args:
        df: The filtered access DataFrame.
        column: Column name to group by (e.g. ``"CLIENT"``).
        top_n: Maximum number of categories to return.

    Returns:
        A DataFrame with columns ``[column.title(), "Direction", "Access Count"]``
        limited to the top *top_n* categories by total access count,
        or an empty DataFrame if *df* is empty.
    """
    if df.empty:
        return pd.DataFrame()

    # Normalise direction to Read / Write
    tmp = df[[column, "DIRECTION", "ACCESS_COUNT"]].copy()
    tmp["Direction"] = tmp["DIRECTION"].apply(
        lambda d: "Write" if d in ("write", "DML", "DDL") else "Read"
    )

    agg = (
        tmp.groupby([column, "Direction"])["ACCESS_COUNT"]
        .sum()
        .reset_index()
        .rename(columns={column: column.title(), "ACCESS_COUNT": "Access Count"})
    )

    # Keep only top_n categories by total
    totals = agg.groupby(column.title())["Access Count"].sum()
    top_cats = totals.sort_values(ascending=False).head(top_n).index
    return agg[agg[column.title()].isin(top_cats)]


def _build_bar_chart(
    data: pd.DataFrame,
    column: str,
    grid_color: str,
    height: int = 350,
) -> go.Figure | None:
    """Build a stacked horizontal bar chart with Read/Write segments.

    Bars are stacked by direction — green for Read, amber for Write.
    Categories are sorted so the highest total is at the top.

    Args:
        data: Aggregated DataFrame from ``prepare_chart_data`` with columns
            ``[column.title(), "Direction", "Access Count"]``.
        column: The dimension column name (e.g. ``"CLIENT"``).
        grid_color: CSS rgba color for the axis grid lines.
        height: Chart height in pixels.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *data* is empty.
    """
    if data.empty:
        return None

    col_title = column.title()

    # Sort categories by total descending, then reverse for bottom-up Plotly layout
    totals = data.groupby(col_title)["Access Count"].sum().sort_values(ascending=True)
    cat_order = totals.index.tolist()

    fig = go.Figure()

    for direction, color in [("Read", READ_GREEN), ("Write", AMBER)]:
        subset = data[data["Direction"] == direction]
        if subset.empty:
            continue
        # Reindex to match category order (fill missing with 0)
        subset = subset.set_index(col_title).reindex(cat_order).fillna(0).reset_index()
        fig.add_trace(go.Bar(
            x=subset["Access Count"],
            y=subset[col_title],
            name=direction,
            orientation="h",
            marker=dict(color=color, cornerradius=4),
            hovertemplate="%{y}<br>" + direction + ": %{x:,}<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        title=dict(
            text=f"Access Count by {col_title}",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        xaxis=dict(
            title=dict(text="Access Count"),
            gridcolor=grid_color,
            showline=True,
            linecolor=grid_color,
        ),
        yaxis=dict(
            title=dict(text=col_title),
            gridcolor=grid_color,
            showline=True,
            linecolor=grid_color,
            categoryorder="array",
            categoryarray=cat_order,
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

    Both read and write flows are laid out with clients on the left and
    databases on the right.  Nodes on each side are sorted by total
    volume descending.

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

    # Both directions: clients on left, databases on right
    left_col, right_col = "CLIENT", "DATABASE"

    left_totals = flows.groupby(left_col)["ACCESS_COUNT"].sum().sort_values(ascending=False)
    right_totals = flows.groupby(right_col)["ACCESS_COUNT"].sum().sort_values(ascending=False)
    left_nodes = left_totals.index.tolist()
    right_nodes = right_totals.index.tolist()

    labels = left_nodes + right_nodes
    left_idx = {name: i for i, name in enumerate(left_nodes)}
    right_idx = {name: i + len(left_nodes) for i, name in enumerate(right_nodes)}

    # Compute fixed x/y positions so Plotly preserves our sort order
    # (greatest flow at top, least at bottom).  Positions are
    # proportional to each node's flow volume so that tall bars don't
    # overlap — each node's y centre is placed after enough room for
    # all preceding nodes plus uniform gap padding.
    n_left = len(left_nodes)
    n_right = len(right_nodes)

    def _flow_weighted_positions(totals: "pd.Series", n: int) -> list[float]:
        """Return y-positions weighted by flow so tall bars get more room."""
        if n == 0:
            return []
        if n == 1:
            return [0.5]
        vals = [totals.iloc[i] for i in range(n)]
        total_flow = sum(vals)
        # Each node occupies space proportional to its flow share, plus
        # a fixed gap.  We reserve 15% of the range for gaps (split
        # among n-1 inter-node gaps) and 85% for bars.
        gap_share = 0.15
        bar_share = 1.0 - gap_share
        gap = gap_share / (n - 1) if n > 1 else 0
        positions: list[float] = []
        cursor = 0.01  # start just inside the range
        usable = 0.98  # total y-range we can use (0.01 .. 0.99)
        for i, v in enumerate(vals):
            bar_height = (v / total_flow) * bar_share * usable if total_flow else 0
            y_centre = cursor + bar_height / 2
            positions.append(min(y_centre, 0.99))
            cursor += bar_height + gap * usable
        return positions

    node_x: list[float] = []
    node_y: list[float] = []

    left_positions = _flow_weighted_positions(left_totals, n_left)
    for i in range(n_left):
        node_x.append(0.001)  # left column (avoid exact 0)
        node_y.append(left_positions[i])

    right_positions = _flow_weighted_positions(right_totals, n_right)
    for i in range(n_right):
        node_x.append(0.999)  # right column (avoid exact 1)
        node_y.append(right_positions[i])

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
        title = "Read Flows — Client ← Database"

    fig = go.Figure(go.Sankey(
        arrangement="fixed",
        node=dict(
            pad=20,
            thickness=18,
            label=labels,
            color=node_colors,
            x=node_x,
            y=node_y,
            line=dict(width=0),
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=[link_color] * len(values),
        ),
    ))

    # Scale height so each node gets ~35px of vertical space minimum
    n_max = max(n_left, n_right)
    chart_height = max(600, n_max * 35 + 100)

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
        height=chart_height,
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


def _build_heatmap(df: pd.DataFrame, grid_color: str) -> go.Figure | None:
    """Build a Client x Database heatmap showing access intensity.

    Pivots the DataFrame so clients are rows and databases are columns,
    with cell values being total access counts.  Limits to the top 15
    clients and top 15 databases by total volume to keep the chart
    readable.

    Args:
        df: The filtered access DataFrame.
        grid_color: CSS rgba color for axis lines.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *df* is empty.
    """
    if df.empty:
        return None

    agg = (
        df.groupby(["CLIENT", "DATABASE"], as_index=False)["ACCESS_COUNT"]
        .sum()
    )

    # Limit to top 15 clients and top 15 databases by total volume
    top_clients = (
        agg.groupby("CLIENT")["ACCESS_COUNT"].sum()
        .nlargest(15).index.tolist()
    )
    top_dbs = (
        agg.groupby("DATABASE")["ACCESS_COUNT"].sum()
        .nlargest(15).index.tolist()
    )
    agg = agg[agg["CLIENT"].isin(top_clients) & agg["DATABASE"].isin(top_dbs)]

    if agg.empty:
        return None

    pivot = agg.pivot_table(
        index="CLIENT", columns="DATABASE", values="ACCESS_COUNT", fill_value=0,
    )
    # Sort both axes by total descending
    pivot = pivot.loc[
        pivot.sum(axis=1).sort_values(ascending=False).index,
        pivot.sum(axis=0).sort_values(ascending=False).index,
    ]

    is_dark = is_dark_theme()
    colorscale = [
        [0, "rgba(41,181,232,0.05)"],
        [0.25, "rgba(41,181,232,0.25)"],
        [0.5, "rgba(41,181,232,0.5)"],
        [0.75, "rgba(41,181,232,0.75)"],
        [1, SNOWFLAKE_BLUE],
    ] if not is_dark else [
        [0, "rgba(113,211,220,0.05)"],
        [0.25, "rgba(113,211,220,0.25)"],
        [0.5, "rgba(113,211,220,0.5)"],
        [0.75, "rgba(113,211,220,0.75)"],
        [1, STAR_BLUE],
    ]

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=colorscale,
        hovertemplate="Client: %{y}<br>Database: %{x}<br>Access Count: %{z:,}<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text="Access Heatmap — Client × Database",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        xaxis=dict(
            title=dict(text="Database"),
            tickangle=-45,
            gridcolor=grid_color,
        ),
        yaxis=dict(
            title=dict(text="Client"),
            gridcolor=grid_color,
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=50, b=80),
        height=max(300, len(pivot) * 30 + 120),
    )
    return fig


def _build_treemap(df: pd.DataFrame) -> go.Figure | None:
    """Build a hierarchical treemap of access distribution.

    Hierarchy is Client -> Database -> Direction, sized by access count.
    Internal labels use unique IDs (e.g. ``"CLIENT/DB"``) to avoid
    collisions when the same database appears under multiple clients;
    ``textinfo`` shows only the leaf name.

    Args:
        df: The filtered access DataFrame.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *df* is empty.
    """
    if df.empty:
        return None

    agg = (
        df.groupby(["CLIENT", "DATABASE", "DIRECTION"], as_index=False)["ACCESS_COUNT"]
        .sum()
    )
    if agg.empty:
        return None

    ids = []
    labels = []
    parents = []
    values = []
    colors = []

    # Client level (top-level, no parent)
    client_totals = agg.groupby("CLIENT")["ACCESS_COUNT"].sum().sort_values(ascending=False)
    for client in client_totals.index:
        ids.append(client)
        labels.append(client)
        parents.append("")
        values.append(int(client_totals[client]))
        colors.append(SNOWFLAKE_BLUE)

    # Database level (unique id = "client/db")
    client_db = agg.groupby(["CLIENT", "DATABASE"])["ACCESS_COUNT"].sum()
    for (client, db), total in client_db.items():
        uid = f"{client}/{db}"
        ids.append(uid)
        labels.append(db)
        parents.append(client)
        values.append(int(total))
        colors.append(STAR_BLUE)

    # Direction level (unique id = "client/db/direction")
    for _, row in agg.iterrows():
        direction = row["DIRECTION"]
        uid = f"{row['CLIENT']}/{row['DATABASE']}/{direction}"
        parent_uid = f"{row['CLIENT']}/{row['DATABASE']}"
        ids.append(uid)
        labels.append(direction.upper())
        parents.append(parent_uid)
        values.append(int(row["ACCESS_COUNT"]))
        color = AMBER if direction in ("write", "DML", "DDL") else READ_GREEN
        colors.append(color)

    fig = go.Figure(go.Treemap(
        ids=ids,
        labels=labels,
        parents=parents,
        values=values,
        branchvalues="total",
        marker=dict(colors=colors),
        textinfo="label+percent parent",
        hovertemplate="<b>%{label}</b><br>Access Count: %{value:,}<br>%{percentParent:.1%} of parent<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text="Access Distribution — Client → Database → Direction",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=50, b=10),
        height=500,
    )
    return fig


def render_bar_charts(df: pd.DataFrame) -> None:
    """Render all charts below the network graph.

    Layout order:
    1. Bar charts — Client and Database side-by-side, Warehouse full-width
    2. Heatmap — Client x Database access intensity (full-width)
    3. Treemap — Hierarchical access distribution (full-width)
    4. Sankey diagrams — Read and Write flows side-by-side

    Adapts bar and grid colors to the current Streamlit theme.

    Args:
        df: The filtered access DataFrame.  Shows a warning if empty.
    """
    if df.empty:
        st.warning("Apply different filters or load more data to see charts.")
        return

    logger.info("Rendering charts for %d rows", len(df))

    is_dark = is_dark_theme()
    grid_color = "rgba(255,255,255,0.18)" if is_dark else "rgba(0,0,0,0.10)"

    client_data = prepare_chart_data(df, "CLIENT")
    db_data = prepare_chart_data(df, "DATABASE")
    wh_data = prepare_chart_data(df, "WAREHOUSE")

    col1, col2 = st.columns(2)
    with col1:
        fig = _build_bar_chart(client_data, "CLIENT", grid_color)
        if fig:
            st.plotly_chart(fig, width="stretch")
    with col2:
        fig = _build_bar_chart(db_data, "DATABASE", grid_color)
        if fig:
            st.plotly_chart(fig, width="stretch")

    fig = _build_bar_chart(wh_data, "WAREHOUSE", grid_color)
    if fig:
        st.plotly_chart(fig, width="stretch")

    # Heatmap
    fig = _build_heatmap(df, grid_color)
    if fig:
        st.plotly_chart(fig, width="stretch")

    # Treemap
    fig = _build_treemap(df)
    if fig:
        st.plotly_chart(fig, width="stretch")

    render_sankey(df)
