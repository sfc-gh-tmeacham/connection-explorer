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

    title_col, toggle_col, btn_col = st.columns([5, 4.5, 0.5])
    with title_col:
        st.markdown(
            '<div style="padding-left: 50px;"><span class="network-title">Network Graph</span></div>',
            unsafe_allow_html=True,
        )
    with toggle_col:
        tc1, tc2, tc3, tc4, tc5 = st.columns(5)

        # Read current hide states to enforce "at most 1 hidden" constraint.
        # If one is already checked, the others are disabled.
        cur_hide_wh = st.session_state.get("hide_warehouses", True)
        cur_hide_cl = st.session_state.get("hide_clients", False)
        cur_hide_db = st.session_state.get("hide_databases", False)
        cur_hide_sc = st.session_state.get("hide_schemas", False)
        one_hidden = cur_hide_wh or cur_hide_cl or cur_hide_db or cur_hide_sc

        with tc1:
            hide_wh = st.checkbox(
                "Hide Warehouses", key="hide_warehouses", value=True,
                disabled=one_hidden and not cur_hide_wh,
            )
        with tc2:
            hide_cl = st.checkbox(
                "Hide Clients", key="hide_clients",
                disabled=one_hidden and not cur_hide_cl,
            )
        with tc3:
            hide_db = st.checkbox(
                "Hide Databases", key="hide_databases",
                disabled=one_hidden and not cur_hide_db,
            )
        with tc4:
            hide_sc = st.checkbox(
                "Hide Schemas", key="hide_schemas",
                disabled=one_hidden and not cur_hide_sc,
            )
        with tc5:
            cluster_db = st.checkbox("Cluster Databases", key="cluster_databases")
    with btn_col:
        st.markdown('<div class="fullscreen-btn-container"></div>', unsafe_allow_html=True)
        if st.button("⛶", help="Full Screen"):
            st.session_state["full_screen_mode"] = True
            st.rerun()

    render_network(
        df, node_images, session,
        fullscreen=False, hide_warehouses=hide_wh, hide_clients=hide_cl,
        hide_databases=hide_db, hide_schemas=hide_sc, cluster_databases=cluster_db,
    )
