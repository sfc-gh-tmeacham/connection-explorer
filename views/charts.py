"""Charts page — bar charts, heatmap, treemap, donuts, and Sankey diagrams."""

import streamlit as st

from components.charts import render_bar_charts


def run():
    """Render the Charts page.

    Reads the filtered DataFrame from ``st.session_state["filtered_df"]``
    and delegates to ``render_bar_charts`` which produces bar charts,
    heatmaps, a treemap, and Sankey diagrams.  Shows an info message when
    no data is available.
    """
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    # All chart rendering (bar charts by dimension, client×database heatmap,
    # access treemap, donut breakdowns, and Sankey flow diagrams) is handled
    # by the components.charts module.  It receives the already-filtered
    # DataFrame so it doesn't need to know about sidebar filter state.
    render_bar_charts(df)
