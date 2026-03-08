"""Snowflake Connection Explorer — main Streamlit application entry point.

Loads Snowflake access data, presents sidebar filters, and routes between
the Network Graph and Charts pages using st.navigation with top positioning.
"""

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

import streamlit as st

from components.assets import FAVICON_PATH, load_node_images, render_snowflake_header
from components.data import apply_filters, get_distinct_values, load_data, process_dataframe
from components.network import render_network
from components.theme import CUSTOM_CSS

# Snowpark session acquisition — dual-environment pattern:
#   1. In Streamlit-in-Snowflake (SiS), a session is auto-injected via get_active_session().
#   2. Locally, we fall back to st.connection() which reads .streamlit/secrets.toml.
#   3. If neither works (e.g. no credentials configured), session is None and
#      the app runs in demo mode with sample data.
try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:
    try:
        conn = st.connection("snowflake")
        session = conn.session()
    except Exception:
        session = None

# Sidebar starts collapsed so the network graph gets maximum viewport width
# on first load.  Users can expand it to access filters.
st.set_page_config(
    page_title="Snowflake Connection Explorer",
    page_icon=str(FAVICON_PATH),
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        "Get Help": "https://developers.snowflake.com",
        "About": "Snowflake Connection Explorer - Full visibility into your Snowflake data access. "
        "Built with Snowpark for Python and Streamlit.",
    },
)


def sidebar_filters(df):
    """Render sidebar filter widgets and return the filtered DataFrame.

    Creates multiselect widgets for database, warehouse, client, and
    direction, plus a numeric access-count threshold and a row-limit
    selector.  Widget values are persisted in ``st.session_state`` so
    they survive Streamlit reruns.

    Args:
        df: The fully processed DataFrame to filter.  If empty the
            function returns it unchanged.

    Returns:
        A filtered (and possibly row-limited) copy of *df*.
    """
    st.sidebar.header("Filters")
    st.sidebar.caption("Data covers the last 30 days")
    if df.empty:
        return df

    filter_configs = [
        ("database", "DATABASE", ":material/database: Database", "Filter by database name"),
        ("schema", "SCHEMA_NAME", ":material/schema: Schema", "Filter by schema (DB.SCHEMA)"),
        ("warehouse", "WAREHOUSE", ":material/warehouse: Warehouse", "Filter by warehouse name"),
        ("client", "CLIENT", ":material/devices: Client", "Filter by client application"),
        ("direction", "DIRECTION", ":material/swap_vert: Statement Type", "Filter by access direction (read, write, DDL, metadata)"),
    ]

    def _sync_filter(persist_key: str, widget_key: str):
        """Copy a widget's current value to its corresponding persist key.

        Used as an ``on_change`` callback for multiselect filter widgets so
        that user selections survive Streamlit reruns.

        Args:
            persist_key: The ``st.session_state`` key where the value is
                persisted across reruns.
            widget_key: The ``st.session_state`` key bound to the widget
                via its ``key`` parameter.
        """
        st.session_state[persist_key] = st.session_state[widget_key]

    values = {}
    for name, col, label, help_text in filter_configs:
        options = get_distinct_values(df, col)
        persist_key = f"persist_filter_{name}"
        widget_key = f"widget_filter_{name}"

        persisted = st.session_state.get(persist_key, [])
        if isinstance(persisted, str):
            persisted = [persisted] if persisted else []
        default_vals = [v for v in persisted if v in options]

        # Only pass default when the widget key is not already in session
        # state.  If a callback (e.g. click-to-filter) pre-set the widget
        # key, passing default= would conflict and raise an error.
        ms_kwargs = dict(
            key=widget_key,
            on_change=_sync_filter,
            args=(persist_key, widget_key),
            help=help_text,
        )
        if widget_key not in st.session_state:
            ms_kwargs["default"] = default_vals

        values[name] = st.sidebar.multiselect(label, options, **ms_kwargs)
        st.session_state[persist_key] = values[name]

    if "widget_filter_access_count" not in st.session_state:
        st.session_state["widget_filter_access_count"] = st.session_state.get("persist_filter_access_count", 10)
    access_count = st.sidebar.number_input(":material/filter_alt: Access Count Limit", min_value=1, max_value=1_000_000,
                                           step=10, key="widget_filter_access_count",
                                           help="Exclude rows with fewer accesses than this threshold")
    st.session_state["persist_filter_access_count"] = access_count

    row_limit_options = [100, 250, 500, 1000, 2500, 5000]
    if "widget_filter_row_limit" not in st.session_state:
        persist_row = st.session_state.get("persist_filter_row_limit", 500)
        st.session_state["widget_filter_row_limit"] = persist_row if persist_row in row_limit_options else 500
    row_limit = st.sidebar.selectbox(":material/hub: Graph Node Limit", row_limit_options, key="widget_filter_row_limit",
                                     help="Limit nodes shown in the network graph (by top access count)")
    st.session_state["persist_filter_row_limit"] = row_limit

    filtered_df = apply_filters(
        df,
        tuple(values["database"]),
        tuple(values["schema"]),
        tuple(values["warehouse"]),
        tuple(values["client"]),
        "",
        tuple(values["direction"]),
        access_count,
    )
    return filtered_df.head(row_limit) if len(filtered_df) > row_limit else filtered_df


def main():
    """Application entry point — orchestrates data loading, filtering, and page routing.

    Initialises session-state defaults for all sidebar filter persist keys,
    handles fullscreen mode (bypassing navigation to render a borderless
    network graph), and in normal mode loads data, renders sidebar filters,
    and sets up ``st.navigation`` with the Network Graph, Charts, Data, and
    Classifications pages.  The filtered DataFrame and Snowflake session are
    stored in ``st.session_state`` so page modules can access them.
    """
    # Default values for all persist_* session-state keys.  These keys store
    # the user's current filter selections and survive Streamlit reruns.
    # Each has a matching widget_* key that is bound to the actual widget.
    filter_defaults = {
        "persist_filter_database": [],
        "persist_filter_schema": [],
        "persist_filter_warehouse": [],
        "persist_filter_client": [],
        "persist_filter_org": "",
        "persist_filter_direction": [],
        "persist_filter_access_count": 10,
        "persist_filter_row_limit": 500,
    }
    for key, default in filter_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default

    # Guard against persist keys that were stored as a bare string instead of
    # a list (can happen with older session-state snapshots or click-to-filter
    # writing a single value).  Normalise them to lists so multiselect widgets
    # don't break.
    for key in (
        "persist_filter_database",
        "persist_filter_schema",
        "persist_filter_warehouse",
        "persist_filter_client",
        "persist_filter_direction",
    ):
        val = st.session_state.get(key, [])
        if isinstance(val, str):
            st.session_state[key] = [val] if val else []

    is_fullscreen = st.session_state.get("full_screen_mode", False)
    logger.debug("App started — fullscreen=%s", is_fullscreen)

    # --- Fullscreen mode ---
    # When the user clicks the fullscreen button on the Network Graph page,
    # we bypass st.navigation entirely and render a borderless, chrome-free
    # network graph that fills the viewport.  CSS hides the Streamlit header,
    # sidebar, toolbar, and deploy button, then stretches the iframe to 100vw
    # x 100vh.  A small exit button (top-right) restores normal mode.
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

        if st.button("", key="exit_fullscreen", help="Exit Full Screen", icon=":material/fullscreen_exit:"):
            st.session_state["full_screen_mode"] = False
            st.rerun()

        raw_df = load_data(session)
        processed_df = process_dataframe(raw_df)
        filtered_df = apply_filters(
            processed_df,
            database_names=tuple(st.session_state.get("persist_filter_database", []) or []),
            schema_names=tuple(st.session_state.get("persist_filter_schema", []) or []),
            warehouse_names=tuple(st.session_state.get("persist_filter_warehouse", []) or []),
            client_names=tuple(st.session_state.get("persist_filter_client", []) or []),
            org_filter=st.session_state.get("persist_filter_org", ""),
            direction_filters=tuple(st.session_state.get("persist_filter_direction", []) or []),
            access_count=st.session_state.get("persist_filter_access_count", 10),
        )
        row_limit = st.session_state.get("persist_filter_row_limit", 500)
        if len(filtered_df) > row_limit:
            filtered_df = filtered_df.head(row_limit)

        node_images = load_node_images()
        hide_wh = st.session_state.get("hide_warehouses", True)
        hide_cl = st.session_state.get("hide_clients", False)
        hide_db = st.session_state.get("hide_databases", False)
        hide_sc = st.session_state.get("hide_schemas", False)
        cluster_db = st.session_state.get("cluster_databases", False)
        combine_rw = st.session_state.get("combine_rw", True)
        render_network(filtered_df, node_images, session, fullscreen=True, hide_warehouses=hide_wh, hide_clients=hide_cl, hide_databases=hide_db, hide_schemas=hide_sc, cluster_databases=cluster_db, combine_rw=combine_rw)
        return

    # --- Normal mode ---
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    render_snowflake_header()

    # Load and filter data, store in session state for pages
    raw_df = load_data(session)
    processed_df = process_dataframe(raw_df)
    filtered_df = sidebar_filters(processed_df)
    st.session_state["filtered_df"] = filtered_df
    st.session_state["snowflake_session"] = session

    if st.sidebar.button(":material/refresh: Reload Data", help="Clear cache and reload data from Snowflake", use_container_width=True):
        st.cache_data.clear()
        # Clear only the vis.js component cache (not the Snowflake connection)
        from components.network import _get_component
        _get_component.clear()
        st.rerun()
    st.sidebar.markdown("Powered by Streamlit :streamlit:")
    st.sidebar.markdown("Built with Cortex Code :material/terminal:")

    # --- Page navigation ---
    # Imports are deferred to here (rather than top-of-file) so that the
    # fullscreen early-return path above doesn't pay for unnecessary module
    # loads.  Each view module exposes a run() function registered as a Page.
    from views.network import run as network_page
    from views.charts import run as charts_page
    from views.data import run as data_page
    from views.classifications import run as classifications_page

    pg = st.navigation(
        [
            st.Page(network_page, title="Network Graph", default=True, url_path="network", icon=":material/hub:"),
            st.Page(charts_page, title="Charts", url_path="charts", icon=":material/bar_chart:"),
            st.Page(data_page, title="Data", url_path="data", icon=":material/table_view:"),
            st.Page(classifications_page, title="Classifications", url_path="classifications", icon=":material/label:"),
        ],
        position="top",
    )
    pg.run()


main()
