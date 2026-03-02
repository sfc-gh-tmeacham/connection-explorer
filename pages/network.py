"""Network Graph page — interactive vis.js network visualization."""

import streamlit as st

from components.assets import load_node_images
from components.network import render_network


def run():
    df = st.session_state.get("filtered_df")
    if df is None or df.empty:
        st.info("No data to display. Adjust filters in the sidebar.")
        return

    node_images = load_node_images()
    session = st.session_state.get("snowflake_session")

    title_col, toggle_col, btn_col = st.columns([7, 2.5, 0.5])
    with title_col:
        st.markdown(
            '<div style="padding-left: 50px;"><span class="network-title">Network Graph</span></div>',
            unsafe_allow_html=True,
        )
    with toggle_col:
        tc1, tc2 = st.columns(2)
        with tc1:
            hide_wh = st.checkbox("Hide Warehouses", key="hide_warehouses")
        with tc2:
            cluster_db = st.checkbox("Cluster Databases", key="cluster_databases")
    with btn_col:
        st.markdown('<div class="fullscreen-btn-container"></div>', unsafe_allow_html=True)
        if st.button("⛶", help="Full Screen"):
            st.session_state["full_screen_mode"] = True
            st.rerun()

    render_network(
        df, node_images, session,
        fullscreen=False, hide_warehouses=hide_wh, cluster_databases=cluster_db,
    )
