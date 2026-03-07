"""Charts page — bar charts, heatmap, treemap, donuts, and Sankey diagrams."""

import streamlit as st

from components.charts import render_bar_charts


def run():
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    render_bar_charts(df)
