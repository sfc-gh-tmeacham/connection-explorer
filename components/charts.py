"""Plotly charts: bar charts, heatmap, treemap, and Sankey diagrams.

Renders interactive visualizations below the network graph: horizontal
bar charts for top databases, warehouses, and clients by access count,
a Client x Database heatmap, a hierarchical treemap, and three-column
Sankey flow diagrams (Client → Warehouse → Database).
"""

import logging

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

logger = logging.getLogger(__name__)

from components.theme import SNOWFLAKE_BLUE, STAR_BLUE, AMBER, MID_BLUE, READ_GREEN, is_dark_theme


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
    """Build a 4-column Sankey: Client → Warehouse → Database → Schema.

    Nodes in each column are sorted by total volume descending, with
    flow-weighted y-positions so tall bars don't overlap.

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

    # Aggregate by the full path: client → warehouse → database → schema
    group_cols = ["CLIENT", "WAREHOUSE", "DATABASE"]
    has_schema = "SCHEMA_NAME" in subset.columns
    if has_schema:
        group_cols.append("SCHEMA_NAME")

    flows = (
        subset.groupby(group_cols, as_index=False)[
            "ACCESS_COUNT"
        ]
        .sum()
        .sort_values("ACCESS_COUNT", ascending=False)
    )

    # Totals per column, sorted descending
    client_totals = (
        flows.groupby("CLIENT")["ACCESS_COUNT"].sum().sort_values(ascending=False)
    )
    wh_totals = (
        flows.groupby("WAREHOUSE")["ACCESS_COUNT"].sum().sort_values(ascending=False)
    )
    db_totals = (
        flows.groupby("DATABASE")["ACCESS_COUNT"].sum().sort_values(ascending=False)
    )
    if has_schema:
        schema_totals = (
            flows.groupby("SCHEMA_NAME")["ACCESS_COUNT"].sum().sort_values(ascending=False)
        )
        schema_nodes = schema_totals.index.tolist()
        n_schema = len(schema_nodes)
    else:
        schema_nodes = []
        n_schema = 0

    client_nodes = client_totals.index.tolist()
    wh_nodes = wh_totals.index.tolist()
    db_nodes = db_totals.index.tolist()

    labels = client_nodes + wh_nodes + db_nodes + schema_nodes
    n_clients = len(client_nodes)
    n_wh = len(wh_nodes)
    n_db = len(db_nodes)

    client_idx = {name: i for i, name in enumerate(client_nodes)}
    wh_idx = {name: i + n_clients for i, name in enumerate(wh_nodes)}
    db_idx = {name: i + n_clients + n_wh for i, name in enumerate(db_nodes)}
    schema_idx = {name: i + n_clients + n_wh + n_db for i, name in enumerate(schema_nodes)}

    # ── flow-weighted y-positions ────────────────────────────────
    def _flow_weighted_positions(totals: "pd.Series", n: int) -> list[float]:
        """Return y-positions weighted by flow so tall bars get more room."""
        if n == 0:
            return []
        if n == 1:
            return [0.5]
        vals = [totals.iloc[i] for i in range(n)]
        total_flow = sum(vals)
        gap_share = 0.15
        bar_share = 1.0 - gap_share
        gap = gap_share / (n - 1) if n > 1 else 0
        positions: list[float] = []
        cursor = 0.01
        usable = 0.98
        for _i, v in enumerate(vals):
            bar_height = (v / total_flow) * bar_share * usable if total_flow else 0
            y_centre = cursor + bar_height / 2
            positions.append(min(y_centre, 0.99))
            cursor += bar_height + gap * usable
        return positions

    node_x: list[float] = []
    node_y: list[float] = []

    # Spread 4 columns evenly across x-axis
    n_columns = 4 if n_schema > 0 else 3
    x_positions = [0.001, 0.333, 0.666, 0.999] if n_columns == 4 else [0.001, 0.5, 0.999]

    for pos in _flow_weighted_positions(client_totals, n_clients):
        node_x.append(x_positions[0])
        node_y.append(pos)
    for pos in _flow_weighted_positions(wh_totals, n_wh):
        node_x.append(x_positions[1])
        node_y.append(pos)
    for pos in _flow_weighted_positions(db_totals, n_db):
        node_x.append(x_positions[2])
        node_y.append(pos)
    if n_schema > 0:
        for pos in _flow_weighted_positions(schema_totals, n_schema):
            node_x.append(x_positions[3])
            node_y.append(pos)

    # ── links: client→warehouse, warehouse→database, database→schema ──
    cw_flows = flows.groupby(["CLIENT", "WAREHOUSE"], as_index=False)[
        "ACCESS_COUNT"
    ].sum()
    wd_flows = flows.groupby(["WAREHOUSE", "DATABASE"], as_index=False)[
        "ACCESS_COUNT"
    ].sum()
    if has_schema:
        ds_flows = flows.groupby(["DATABASE", "SCHEMA_NAME"], as_index=False)[
            "ACCESS_COUNT"
        ].sum()

    sources, targets, values, link_colors = [], [], [], []

    if direction == "write":
        cw_color = "rgba(245, 166, 35, 0.35)"
        wd_color = "rgba(245, 166, 35, 0.25)"
        ds_color = "rgba(245, 166, 35, 0.15)"
        title = "Write Flows — Client → Warehouse → Database → Schema"
    else:
        cw_color = "rgba(41, 181, 232, 0.35)"
        wd_color = "rgba(41, 181, 232, 0.25)"
        ds_color = "rgba(41, 181, 232, 0.15)"
        title = "Read Flows — Client ← Warehouse ← Database ← Schema"

    for _, row in cw_flows.iterrows():
        sources.append(client_idx[row["CLIENT"]])
        targets.append(wh_idx[row["WAREHOUSE"]])
        values.append(row["ACCESS_COUNT"])
        link_colors.append(cw_color)

    for _, row in wd_flows.iterrows():
        sources.append(wh_idx[row["WAREHOUSE"]])
        targets.append(db_idx[row["DATABASE"]])
        values.append(row["ACCESS_COUNT"])
        link_colors.append(wd_color)

    if has_schema:
        for _, row in ds_flows.iterrows():
            sources.append(db_idx[row["DATABASE"]])
            targets.append(schema_idx[row["SCHEMA_NAME"]])
            values.append(row["ACCESS_COUNT"])
            link_colors.append(ds_color)

    # Node colors: clients, warehouses, databases, schemas
    SCHEMA_COLOR = "#1B9AAA"  # teal, distinct from Snowflake blue
    if direction == "write":
        node_colors = (
            [AMBER] * n_clients
            + [MID_BLUE] * n_wh
            + [SNOWFLAKE_BLUE] * n_db
            + [SCHEMA_COLOR] * n_schema
        )
    else:
        node_colors = (
            [SNOWFLAKE_BLUE] * n_clients
            + [MID_BLUE] * n_wh
            + [AMBER] * n_db
            + [SCHEMA_COLOR] * n_schema
        )

    fig = go.Figure(
        go.Sankey(
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
                color=link_colors,
            ),
        )
    )

    n_max = max(n_clients, n_wh, n_db, n_schema) if n_schema > 0 else max(n_clients, n_wh, n_db)
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
    """Render full-width Sankey diagrams for read and write flows.

    Each diagram shows four columns: Client → Warehouse → Database → Schema.

    Args:
        df: The filtered access DataFrame.  No-ops if empty.
    """
    if df.empty:
        return

    write_fig = _build_sankey(df, "write")
    read_fig = _build_sankey(df, "read")

    if write_fig:
        st.plotly_chart(write_fig, width="stretch")
    if read_fig:
        st.plotly_chart(read_fig, width="stretch")


def _build_heatmap(
    df: pd.DataFrame,
    grid_color: str,
    row_col: str = "CLIENT",
    col_col: str = "DATABASE",
) -> go.Figure | None:
    """Build a heatmap showing access intensity between two dimensions.

    Pivots the DataFrame so *row_col* values are rows and *col_col*
    values are columns, with cell values being total access counts.
    Limits to the top 15 values on each axis by total volume.

    Args:
        df: The filtered access DataFrame.
        grid_color: CSS rgba color for axis lines.
        row_col: Column name for the y-axis (rows).
        col_col: Column name for the x-axis (columns).

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *df* is empty.
    """
    if df.empty:
        return None

    agg = (
        df.groupby([row_col, col_col], as_index=False)["ACCESS_COUNT"]
        .sum()
    )

    # Limit to top 15 on each axis by total volume
    top_rows = (
        agg.groupby(row_col)["ACCESS_COUNT"].sum()
        .nlargest(15).index.tolist()
    )
    top_cols = (
        agg.groupby(col_col)["ACCESS_COUNT"].sum()
        .nlargest(15).index.tolist()
    )
    agg = agg[agg[row_col].isin(top_rows) & agg[col_col].isin(top_cols)]

    if agg.empty:
        return None

    pivot = agg.pivot_table(
        index=row_col, columns=col_col, values="ACCESS_COUNT", fill_value=0,
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

    row_label = row_col.replace("_", " ").title()
    col_label = col_col.replace("_", " ").title()

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale=colorscale,
        hovertemplate=f"{row_label}: %{{y}}<br>{col_label}: %{{x}}<br>Access Count: %{{z:,}}<extra></extra>",
    ))

    fig.update_layout(
        title=dict(
            text=f"Access Heatmap — {row_label} × {col_label}",
            font=dict(family="Lato, Arial, sans-serif", size=16),
            x=0.5,
            xanchor="center",
        ),
        font=dict(family="Lato, Arial, sans-serif", size=12),
        xaxis=dict(
            title=dict(text=col_label),
            tickangle=-45,
            gridcolor=grid_color,
        ),
        yaxis=dict(
            title=dict(text=row_label),
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

    Hierarchy is Client -> Database -> Schema -> Direction, sized by
    access count.  Internal labels use unique IDs (e.g. ``"CLIENT/DB"``)
    to avoid collisions when the same database appears under multiple
    clients; ``textinfo`` shows only the leaf name.

    Args:
        df: The filtered access DataFrame.

    Returns:
        A ``plotly.graph_objects.Figure``, or ``None`` if *df* is empty.
    """
    if df.empty:
        return None

    has_schema = "SCHEMA_NAME" in df.columns
    group_cols = ["CLIENT", "DATABASE"] + (["SCHEMA_NAME"] if has_schema else []) + ["DIRECTION"]
    agg = (
        df.groupby(group_cols, as_index=False)["ACCESS_COUNT"]
        .sum()
    )
    if agg.empty:
        return None

    ids = []
    labels = []
    parents = []
    values = []
    colors = []

    SCHEMA_COLOR = "#1B9AAA"

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

    if has_schema:
        # Schema level (unique id = "client/db/schema")
        client_db_schema = agg.groupby(["CLIENT", "DATABASE", "SCHEMA_NAME"])["ACCESS_COUNT"].sum()
        for (client, db, schema), total in client_db_schema.items():
            uid = f"{client}/{db}/{schema}"
            ids.append(uid)
            labels.append(schema)
            parents.append(f"{client}/{db}")
            values.append(int(total))
            colors.append(SCHEMA_COLOR)

        # Direction level (unique id = "client/db/schema/direction")
        for _, row in agg.iterrows():
            direction = row["DIRECTION"]
            schema = row["SCHEMA_NAME"]
            uid = f"{row['CLIENT']}/{row['DATABASE']}/{schema}/{direction}"
            parent_uid = f"{row['CLIENT']}/{row['DATABASE']}/{schema}"
            ids.append(uid)
            labels.append(direction.upper())
            parents.append(parent_uid)
            values.append(int(row["ACCESS_COUNT"]))
            color = AMBER if direction in ("write", "DML", "DDL") else READ_GREEN
            colors.append(color)
    else:
        # Direction level without schema (unique id = "client/db/direction")
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

    title_text = "Access Distribution — Client → Database → Schema → Direction" if has_schema else "Access Distribution — Client → Database → Direction"

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
            text=title_text,
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
    1. Bar charts — Client and Database side-by-side, Schema and Warehouse side-by-side
    2. Heatmaps — Client × Database, Database × Schema, Client × Warehouse (full-width)
    3. Treemap — Hierarchical access distribution (full-width)
    4. Sankey diagrams — Read and Write flows full-width (Client → Warehouse → Database → Schema)

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

    has_schema = "SCHEMA_NAME" in df.columns
    col3, col4 = st.columns(2)
    with col3:
        if has_schema:
            schema_data = prepare_chart_data(df, "SCHEMA_NAME")
            fig = _build_bar_chart(schema_data, "SCHEMA_NAME", grid_color)
            if fig:
                st.plotly_chart(fig, width="stretch")
    with col4:
        fig = _build_bar_chart(wh_data, "WAREHOUSE", grid_color)
        if fig:
            st.plotly_chart(fig, width="stretch")

    # Heatmaps
    fig = _build_heatmap(df, grid_color, "CLIENT", "DATABASE")
    if fig:
        st.plotly_chart(fig, width="stretch")

    if has_schema:
        fig = _build_heatmap(df, grid_color, "DATABASE", "SCHEMA_NAME")
        if fig:
            st.plotly_chart(fig, width="stretch")

    fig = _build_heatmap(df, grid_color, "CLIENT", "WAREHOUSE")
    if fig:
        st.plotly_chart(fig, width="stretch")

    # Treemap
    fig = _build_treemap(df)
    if fig:
        st.plotly_chart(fig, width="stretch")

    render_sankey(df)
