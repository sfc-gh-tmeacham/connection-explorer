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

    render_bar_charts(df)
