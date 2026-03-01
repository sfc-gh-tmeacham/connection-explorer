"""Plotly charts: bar charts, heatmap, treemap, read/write donuts, and Sankey diagrams.

Renders interactive visualizations below the network graph: horizontal
bar charts for top databases, warehouses, and clients by access count,
a Client x Database heatmap, a hierarchical treemap, per-database
read/write ratio donuts, and read/write Sankey flow diagrams.
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

logger = logging.getLogger(__name__)

from plotly.subplots import make_subplots

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


def _build_rw_donuts(df: pd.DataFrame) -> go.Figure | None:
    """Build read/write ratio donut charts for the top databases.

    Shows up to 6 databases in a 2x3 grid of donut charts, each showing
    the proportion of read vs write access.

    Args:
        df: The filtered access DataFrame.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *df* is empty
        or no databases have both read and write activity.
    """
    if df.empty:
        return None

    # Classify directions
    df_copy = df.copy()
    df_copy["RW"] = df_copy["DIRECTION"].apply(
        lambda d: "Write" if d in ("write", "DML", "DDL") else "Read"
    )

    rw_agg = (
        df_copy.groupby(["DATABASE", "RW"], as_index=False)["ACCESS_COUNT"]
        .sum()
    )

    # Pick top 6 databases by total access
    db_totals = rw_agg.groupby("DATABASE")["ACCESS_COUNT"].sum().nlargest(6)
    top_dbs = db_totals.index.tolist()

    if not top_dbs:
        return None

    n = len(top_dbs)
    ncols = min(n, 3)
    nrows = (n + ncols - 1) // ncols

    fig = make_subplots(
        rows=nrows, cols=ncols,
        specs=[[{"type": "pie"}] * ncols for _ in range(nrows)],
        subplot_titles=top_dbs,
    )

    for i, db in enumerate(top_dbs):
        row = i // ncols + 1
        col = i % ncols + 1
        db_data = rw_agg[rw_agg["DATABASE"] == db]

        rw_labels = db_data["RW"].tolist()
        rw_values = db_data["ACCESS_COUNT"].tolist()
        rw_colors = [READ_GREEN if lbl == "Read" else AMBER for lbl in rw_labels]

        fig.add_trace(
            go.Pie(
                labels=rw_labels,
                values=rw_values,
                hole=0.5,
                marker=dict(colors=rw_colors),
                textinfo="percent",
                hovertemplate="<b>%{label}</b><br>Access Count: %{value:,}<br>%{percent}<extra></extra>",
                showlegend=(i == 0),
            ),
            row=row, col=col,
        )

    fig.update_layout(
        title=dict(
            text="Read / Write Ratio by Database",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=80, b=10),
        height=300 * nrows + 80,
        legend=dict(orientation="h", yanchor="bottom", y=-0.05, xanchor="center", x=0.5),
    )
    return fig


def render_bar_charts(df: pd.DataFrame) -> None:
    """Render all charts below the network graph.

    Layout order:
    1. Bar charts — Client and Database side-by-side, Warehouse full-width
    2. Heatmap — Client x Database access intensity (full-width)
    3. Treemap — Hierarchical access distribution (full-width)
    4. Read/Write donuts — Per-database ratio (3-column grid)
    5. Sankey diagrams — Read and Write flows side-by-side

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

    # Read/Write donuts
    fig = _build_rw_donuts(df)
    if fig:
        st.plotly_chart(fig, width="stretch")

    render_sankey(df)
