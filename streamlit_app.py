import streamlit as st

from components.assets import FAVICON_PATH, load_node_images, render_snowflake_header
from components.charts import render_bar_charts
from components.data import apply_filters, get_distinct_values, load_data, process_dataframe
from components.network import build_network_html
from components.theme import CUSTOM_CSS

try:  # Streamlit in Snowflake automatically injects a Snowpark session
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:  # Running locally without Snowflake
    session = None

st.set_page_config(
    page_title="Data Lake Explorer",
    page_icon=str(FAVICON_PATH),
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        "Get Help": "https://developers.snowflake.com",
        "About": "Data Lake Explorer - Full visibility into your Snowflake data access. "
        "Built with Snowpark for Python and Streamlit.",
    },
)


def sidebar_filters(df):
    """Render sidebar filters and return filtered dataframe."""
    st.sidebar.header("Filters")
    if df.empty:
        return df

    filter_configs = [
        ("database", "DATABASE", "Select Database"),
        ("warehouse", "WAREHOUSE", "Select Warehouse"),
        ("client", "CLIENT", "Select Client"),
        ("direction", "DIRECTION", "Statement Type"),
    ]

    values = {}
    for name, col, label in filter_configs:
        options = get_distinct_values(df, col)
        persist_key, widget_key = f"persist_filter_{name}", f"widget_filter_{name}"
        persisted = st.session_state.get(persist_key, [])
        if isinstance(persisted, str):
            persisted = [persisted] if persisted else []
        default_vals = [v for v in persisted if v in options]
        values[name] = st.sidebar.multiselect(label, options, default=default_vals, key=widget_key)
        st.session_state[persist_key] = values[name]

    if "widget_filter_access_count" not in st.session_state:
        st.session_state["widget_filter_access_count"] = st.session_state.get("persist_filter_access_count", 1)
    access_count = st.sidebar.number_input("Access Count Limit", min_value=1, max_value=1_000_000,
                                           step=10, key="widget_filter_access_count",
                                           help="Please enter a number between 1 and 1,000,000")
    st.session_state["persist_filter_access_count"] = access_count

    row_limit_options = [100, 250, 500, 1000, 2500]
    if "widget_filter_row_limit" not in st.session_state:
        persist_row = st.session_state.get("persist_filter_row_limit", 500)
        st.session_state["widget_filter_row_limit"] = persist_row if persist_row in row_limit_options else 500
    row_limit = st.sidebar.selectbox("Graph Node Limit", row_limit_options, key="widget_filter_row_limit",
                                     help="Limit nodes shown in the network graph (by top access count)")
    st.session_state["persist_filter_row_limit"] = row_limit

    filtered_df = apply_filters(
        df,
        tuple(values["database"]),
        tuple(values["warehouse"]),
        tuple(values["client"]),
        "",
        tuple(values["direction"]),
        access_count,
    )
    return filtered_df.head(row_limit) if len(filtered_df) > row_limit else filtered_df


def render_network_section(df, network_html):
    """Render network visualization with charts below."""
    title_col, btn_col = st.columns([8, 1])
    with title_col:
        st.markdown(
            '<div style="padding-left: 50px;"><span class="network-title">Network Graph</span></div>',
            unsafe_allow_html=True
        )
    with btn_col:
        st.markdown('<div class="fullscreen-btn-container"></div>', unsafe_allow_html=True)
        if st.button("⛶", help="Full Screen"):
            st.session_state["full_screen_mode"] = True
            st.rerun()

    st.components.v1.html(network_html, height=800)
    render_bar_charts(df)


def main():
    filter_defaults = {
        "persist_filter_database": [],
        "persist_filter_warehouse": [],
        "persist_filter_client": [],
        "persist_filter_org": "",
        "persist_filter_direction": [],
        "persist_filter_access_count": 1,
        "persist_filter_row_limit": 500,
    }
    for key, default in filter_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    for key in (
        "persist_filter_database",
        "persist_filter_warehouse",
        "persist_filter_client",
        "persist_filter_direction",
    ):
        val = st.session_state.get(key, [])
        if isinstance(val, str):
            st.session_state[key] = [val] if val else []

    is_fullscreen = st.session_state.get("full_screen_mode", False)

    if is_fullscreen:
        st.markdown("""
            <style>
                header, footer, .stDeployButton, [data-testid="stHeader"], 
                [data-testid="stToolbar"], [data-testid="stDecoration"],
                [data-testid="stStatusWidget"], [data-testid="stSidebar"],
                #MainMenu, .stAppHeader {
                    display: none !important;
                    visibility: hidden !important;
                    height: 0 !important;
                    width: 0 !important;
                    overflow: hidden !important;
                }
                .stApp, .stAppViewContainer, .stMain, 
                [data-testid="stAppViewBlockContainer"],
                .stMainBlockContainer, div.block-container,
                [data-testid="stVerticalBlock"], section.main {
                    padding: 0 !important;
                    margin: 0 !important;
                    max-width: 100vw !important;
                    width: 100vw !important;
                }
                div[data-testid="stIFrame"],
                div[data-testid="stIFrame"] > div,
                iframe {
                    position: fixed !important;
                    top: 0 !important;
                    left: 0 !important;
                    width: 100vw !important;
                    height: 100vh !important;
                    max-width: none !important;
                    max-height: none !important;
                    margin: 0 !important;
                    padding: 0 !important;
                    border: none !important;
                    z-index: 999998 !important;
                }
                div[data-testid="stButton"] {
                    position: fixed !important;
                    top: 8px !important;
                    right: 8px !important;
                    z-index: 999999 !important;
                }
                div[data-testid="stButton"] button {
                    background-color: transparent !important;
                    border-radius: 4px !important;
                    width: 32px !important;
                    height: 32px !important;
                    min-width: 32px !important;
                    min-height: 32px !important;
                    padding: 0 !important;
                    font-size: 16px !important;
                    font-weight: normal !important;
                    box-shadow: none !important;
                    border: none !important;
                    color: rgba(128, 128, 128, 0.8) !important;
                    display: flex !important;
                    align-items: center !important;
                    justify-content: center !important;
                }
                div[data-testid="stButton"] button:hover {
                    background-color: rgba(128, 128, 128, 0.2) !important;
                    color: rgba(128, 128, 128, 1) !important;
                    transform: none !important;
                }
            </style>
        """, unsafe_allow_html=True)

        if st.button("⤢", key="exit_fullscreen", help="Exit Full Screen"):
            st.session_state["full_screen_mode"] = False
            st.rerun()

        raw_df = load_data(session)
        processed_df = process_dataframe(raw_df)
        filtered_df = apply_filters(
            processed_df,
            database_names=tuple(st.session_state.get("persist_filter_database", []) or []),
            warehouse_names=tuple(st.session_state.get("persist_filter_warehouse", []) or []),
            client_names=tuple(st.session_state.get("persist_filter_client", []) or []),
            org_filter=st.session_state.get("persist_filter_org", ""),
            direction_filters=tuple(st.session_state.get("persist_filter_direction", []) or []),
            access_count=st.session_state.get("persist_filter_access_count", 1),
        )
        row_limit = st.session_state.get("persist_filter_row_limit", 500)
        if len(filtered_df) > row_limit:
            filtered_df = filtered_df.head(row_limit)

        node_images = load_node_images()
        network_html = build_network_html(filtered_df, node_images, session, fullscreen=True)
        st.components.v1.html(network_html, height=2000, scrolling=False)
        return

    # Normal mode
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    col1, col2 = st.columns([6, 1])
    with col1:
        render_snowflake_header()
    with col2:
        if st.button("↻", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()

    raw_df = load_data(session)
    processed_df = process_dataframe(raw_df)
    filtered_df = sidebar_filters(processed_df)

    node_images = load_node_images()
    network_html = build_network_html(filtered_df, node_images, session, fullscreen=False)

    render_network_section(filtered_df, network_html)
    st.sidebar.markdown("Powered by Streamlit :streamlit:")

if __name__ == "__main__":
    main()
