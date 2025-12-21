import base64
from pathlib import Path
from typing import Dict, Optional, Tuple

import altair as alt
import pandas as pd
import streamlit as st
from pyvis.network import Network

# Type alias for RGB tuple
RGB = Tuple[int, int, int]

# Streamlit renders Altair/Vega charts using Vega-Embed. By default it may use the
# canvas renderer, which can't be restyled via CSS. Force SVG so theme-driven CSS
# can reliably control chart text colors in dark/light mode.
try:
    alt.renderers.set_embed_options(renderer="svg", actions=False)
except Exception:
    pass

try:  # Streamlit in Snowflake automatically injects a Snowpark session
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except Exception:  # Running locally without Snowflake
    session = None

APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
FAVICON_PATH = STATIC_DIR / "snowflake-bug-logo.png"

st.set_page_config(
    page_title="Data Lakeview",
    page_icon=str(FAVICON_PATH),
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        "Get Help": "https://developers.snowflake.com",
        "About": "Data Lakeview - Full visibility into your Snowflake data access. "
        "Built with Snowpark for Python and Streamlit.",
    },
)

# Snowflake Brand Colors (https://www.snowflake.com/brand-guidelines/)
SNOWFLAKE_BLUE = "#29B5E8"  # Core Brand Blue
MID_BLUE = "#11567F"        # Contrasting Blue
STAR_BLUE = "#71D3DC"       # Accent Blue (used in dark mode charts)
MIDNIGHT = "#000000"        # Dark text fallback

CUSTOM_CSS = """
    <style>
        /* Snowflake Brand Typography: Lato for body text (TEXTA for headlines is proprietary) */
        @import url('https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap');
        
        /* CSS Variables for Snowflake Brand Colors */
        :root {
            --snowflake-blue: #29B5E8;
            --mid-blue: #11567F;
            --star-blue: #71D3DC;
            --hover-bg: rgba(41, 181, 232, 0.1);
        }
        
        .stApp {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        header[data-testid="stHeader"] {
            margin-bottom: 0;
            padding-top: 0.25rem;
            padding-bottom: 0;
            background-color: transparent;
        }
        /* IMPORTANT: Do NOT hardcode text colors. Let Streamlit's theme handle dark/light.
           We only set typography + Snowflake accent styling. */
        h1, h2, h3, h4, h5, h6 {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
            font-weight: 900;
            padding-top: 0px;
            padding-bottom: 0px;
            text-align: center;
            letter-spacing: 0.02em;
            color: var(--text-color, inherit);
        }
        /* Ensure sidebar widget labels always follow Streamlit theme text color */
        .stSidebar label,
        .stSidebar [data-testid="stWidgetLabel"],
        .stSidebar .stSelectbox label,
        .stSidebar .stNumberInput label,
        .stSidebar .stRadio label,
        .stSidebar .stCheckbox label {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
            font-weight: 700;
            color: var(--text-color, inherit) !important;
        }
        /* ============================================
           Common styles (theme-independent)
           ============================================ */
        .stMainBlockContainer {
            padding: 0.5rem 1rem 0rem;
        }
        .stDivider {
            padding: 0rem;
            height: 0rem;
        }
        div.block-container {
            padding-top: 0.5rem;
        }
        .stSidebar > div {
            border-right: 2px solid var(--snowflake-blue);
        }
        .stSidebar .stRadio > div {
            background-color: transparent !important;
        }
        .stSidebar .stRadio > div > label > div[role="radiogroup"] > label {
            border: 1px solid var(--snowflake-blue) !important;
            border-radius: 80px !important;
            padding: 0.5rem 1rem !important;
            margin: 0.25rem 0 !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
        }
        .stSidebar .stRadio > div > label > div[role="radiogroup"] > label:hover {
            background-color: var(--hover-bg) !important;
        }
        .stSidebar .stRadio > div > label > div[role="radiogroup"] > label[data-checked="true"] {
            background-color: var(--snowflake-blue) !important;
            color: white !important;
        }
        div[data-testid="stImage"] {
            margin-top: 0;
            padding-top: 0;
            margin-bottom: 0.5rem;
        }
        div[data-testid="stImage"] img {
            width: 260px;
            margin-left: auto;
            margin-right: auto;
        }
        .stColumn > div {
            padding-top: 0rem !important;
        }
        .network-title, .page-title {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
            font-weight: 900 !important;
            font-size: 18px !important;
            letter-spacing: 0.02em !important;
            color: var(--text-color, inherit) !important;
            margin: 0 !important;
            padding: 0 !important;
            text-align: left !important;
            line-height: 1.2 !important;
            display: block !important;
        }
        .page-title {
            margin-bottom: 0.5rem !important;
        }
        iframe[title="streamlit_components_v1.components_v1_html"] {
            margin-top: -1rem !important;
        }
        div[data-testid="stIFrame"] {
            margin-top: -1rem !important;
            padding-top: 0 !important;
        }
        /* Make HTML component iframes blend with Streamlit dark theme */
        div[data-testid="stIFrame"],
        div[data-testid="stIFrame"] iframe,
        iframe[title="streamlit_components_v1.components_v1_html"] {
            background: transparent !important;
        }

        .stMarkdown {
            margin-bottom: 0 !important;
        }
        .title-container {
            margin-bottom: 0.25rem !important;
        }
        .network-title + div {
            margin-top: -2rem !important;
        }
        .stButton > button {
            margin-bottom: 0rem !important;
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
            font-weight: 900;
            background-color: var(--snowflake-blue) !important;
            color: white !important;
            border: none !important;
            border-radius: 80px !important;
            text-transform: uppercase !important;
            letter-spacing: 0.05em !important;
            padding: 0.5rem 1.5rem !important;
        }
        .stButton > button:hover {
            background-color: var(--mid-blue) !important;
            color: white !important;
        }
        .stButton > button[kind="secondary"] {
            background-color: transparent !important;
            border: 1px solid var(--snowflake-blue) !important;
            color: var(--snowflake-blue) !important;
        }
        .stButton > button[kind="secondary"]:hover {
            background-color: var(--hover-bg) !important;
        }
        .stSelectbox label, .stNumberInput label {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
        }
        .stAlert > div {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
            border-radius: 8px;
        }
        div[data-testid="stAlert"][data-baseweb="notification"] {
            border-left: 4px solid var(--snowflake-blue);
        }
        /* Altair / Vega charts: keep Snowflake typography */
        .vega-embed,
        .vega-embed * {
            font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        }
        /*
          IMPORTANT: Streamlit's theme toggle is client-side, so Python can't reliably detect it.
          Vega charts often render text with hardcoded fills, so we override via CSS based on
          Streamlit's theme CSS variables (works for both dark & light without detection).
        */
        .vega-embed svg text {
            fill: var(--text-color, currentColor) !important;
        }
        .stSelectbox > div > div {
            border-radius: 8px;
        }
        .stSelectbox > div > div:focus-within {
            border-color: var(--snowflake-blue) !important;
            box-shadow: 0 0 0 1px var(--snowflake-blue) !important;
        }
        .stNumberInput > div > div > input {
            border-radius: 8px;
        }
        .stNumberInput > div > div > input:focus {
            border-color: var(--snowflake-blue) !important;
            box-shadow: 0 0 0 1px var(--snowflake-blue) !important;
        }
        .stSidebar h1, .stSidebar h2, .stSidebar h3 {
            text-align: left !important;
        }
    </style>
    """

@st.cache_resource(show_spinner=False)
def load_snowflake_logo() -> str:
    """Load Snowflake bug logo as base64 encoded string."""
    with open(STATIC_DIR / "snowflake-bug-logo.png", "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return encoded


def render_snowflake_header() -> None:
    """Render Snowflake branded header with bug logo and Horizon Catalog text."""
    logo_b64 = load_snowflake_logo()
    
    # Left-justified header with logo and text aligned
    # Added left padding to avoid overlap with sidebar toggle arrows
    st.markdown(
        f'''
        <div style="display: flex; align-items: center; gap: 12px; padding: 0.25rem 0; padding-left: 50px;">
            <img src="data:image/png;base64,{logo_b64}" width="50" height="50" style="display: block;">
            <span style="font-family: Lato, sans-serif; font-size: 32px; font-weight: 900; 
                         color: #249EDD; letter-spacing: 0.02em; line-height: 1;">
                Horizon Catalog
            </span>
        </div>
        ''',
        unsafe_allow_html=True
    )


@st.cache_resource(show_spinner=False)
def load_node_images() -> Dict[str, str]:
    def encode_image(file_name: str) -> str:
        with open(STATIC_DIR / file_name, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode("utf-8")
        return f"data:image/png;base64,{encoded}"

    return {
        "database": encode_image("snowflake-database.png"),
        "warehouse": encode_image("snowflake-warehouse.png"),
    }


@st.cache_data(show_spinner=False, ttl=3600)
def get_current_account() -> str:
    """Get current Snowflake account name (cached for 1 hour)."""
    if session is not None:
        try:
            return session.sql("SELECT CURRENT_ACCOUNT()").collect()[0][0]
        except Exception:
            pass
    return "SAMPLE_ACCOUNT"


def sample_dataframe() -> pd.DataFrame:
    """Generate sample data for demo/local development."""
    current_account = get_current_account()
    sample_data = {
        "ORGANIZATION_NAME": ["SAMPLE_ORG"] * 8,
        "ACCOUNT_NAME": [current_account] * 8,  # Use dynamic account name
        "DATABASE": [
            "PROD_DB",
            "TEST_DB", 
            "DEV_DB",
            "ANALYTICS_DB",
            "PROD_DB",
            "TEST_DB",
            "REPORTING_DB",
            "STAGING_DB",
        ],
        "WAREHOUSE": [
            "COMPUTE_WH",
            "TEST_WH",
            "DEV_WH", 
            "ANALYTICS_WH",
            "COMPUTE_WH",
            "TEST_WH",
            "REPORTING_WH",
            "STAGING_WH",
        ],
        "CLIENT": [
            "CLIENT_A",
            "CLIENT_B",
            "CLIENT_C",
            "CLIENT_A", 
            "CLIENT_B",
            "CLIENT_C",
            "CLIENT_A",
            "CLIENT_B",
        ],
        "DIRECTION": ["out", "in", "out", "in", "out", "in", "out", "in"],
        "ACCESS_COUNT": [1500, 2300, 800, 1200, 950, 1800, 2100, 750],
    }
    st.info(
        "Using sample data. Connect to Snowflake to view live account usage data "
        "from Snowflake Horizon Catalog."
    )
    return pd.DataFrame(sample_data)

@st.cache_data(show_spinner=False, ttl=300)  # Cache for 5 minutes
def load_data() -> pd.DataFrame:
    """Load data from Snowflake account usage or return sample data."""
    if session is None:
        return sample_dataframe()
    try:
        query = """
        WITH client_mappings AS (
            -- Maintainable lookup for application name patterns -> friendly client names
            -- Order matters: more specific patterns should come before general ones
            SELECT column1 AS pattern, column2 AS client_name FROM VALUES
                ('snowpark', 'Snowpark'),
                ('deployments', 'Kafka'),
                ('cosmos', 'COSMOS'),
                ('rappid', 'RAPPID'),
                ('dtlk', 'DTLK'),
                ('nice', 'NICE'),
                ('nexis', 'NEXIS'),
                ('mashup', 'Power BI'),
                ('powerbi', 'Power BI'),
                ('grafana', 'Grafana'),
                ('cirrus', 'Cirrus CI'),
                ('toad', 'Toad'),
                ('syniti', 'Syniti Replication'),
                ('wherescapered', 'WhereScape RED'),
                ('powershell', 'MS PowerShell'),
                ('bootstrap', 'Tomcat'),
                ('qlikrepl', 'Qlik Replicate'),
                ('rstudio', 'RStudio'),
                ('microstrat', 'MicroStrategy'),
                ('tableau', 'Tableau'),
                ('hyperion', 'Hyperion'),
                ('softoffice', 'Microsoft Office'),
                ('msacces', 'Microsoft Access'),
                ('databricks', 'Databricks/Spark'),
                ('dbatch', 'Databricks/Spark'),
                ('spark', 'Databricks/Spark'),
                ('dtexec', 'SSIS'),
                ('dts', 'SSIS'),
                ('alteryx', 'Alteryx'),
                ('cdata', 'CData'),
                ('reportserver', 'SSRS/PBIRS'),
                ('msrs', 'SSRS/PBIRS'),
                ('reportingservice', 'SSRS/PBIRS'),
                ('reportbuilder', 'SSRS/PBIRS'),
                ('visualstudio', 'SSRS/PBIRS'),
                ('sqlse', 'SQL Server'),
                ('perl', 'Perl'),
                ('iis', 'Microsoft IIS'),
                ('inets', 'Microsoft IIS'),
                ('businessobjects', 'BOBJ'),
                ('domo', 'Domo'),
                ('datafactory', 'Azure Data Factory'),
                ('integrationruntime', 'Azure Data Factory'),
                ('excel', 'Excel'),
                ('snowflake', 'Snowflake Web'),
                ('jarvis', 'Jarvis'),
                ('webjobs', 'Azure WebJobs'),
                ('jenkins', 'Jenkins'),
                ('kafka', 'Kafka'),
                ('airflow', 'Airflow'),
                ('starburst', 'Starburst'),
                ('prest', 'Presto'),
                ('boomi', 'Boomi'),
                ('sas', 'SAS'),
                ('arcgis', 'ArcGIS'),
                ('dbeave', 'DBeaver'),
                ('vscode', 'VSCode'),
                ('teradata', 'Teradata'),
                ('fads', 'Fads'),
                ('nimbus', 'Nimbus'),
                ('advancedquery', 'AdvancedQueryTool'),
                ('jdbc', 'JDBC'),
                ('python', 'Python')
        ),
        
        raw_access AS (
            SELECT
                s.client_application_id,
                PARSE_JSON(s.client_environment):APPLICATION::VARCHAR AS application,
                q.warehouse_name AS warehouse,
                q.query_type,
                q.query_id,
                SPLIT_PART(t.VALUE:objectName::VARCHAR, '.', 1) AS database
            FROM snowflake.account_usage.sessions s
            INNER JOIN snowflake.account_usage.query_history q 
                ON q.session_id = s.session_id
            INNER JOIN snowflake.account_usage.access_history a 
                ON q.query_id = a.query_id,
            TABLE(FLATTEN(a.direct_objects_accessed)) t
            WHERE q.start_time > DATEADD(day, -7, CURRENT_DATE())
                AND q.query_type != 'CALL'
                AND s.client_application_id NOT LIKE 'SYSTEM%'
        )
        
        SELECT
            CURRENT_ORGANIZATION_NAME() AS organization_name,
            CURRENT_ACCOUNT() AS account_name,
            COALESCE(cm.client_name, r.application, r.client_application_id) AS client,
            r.warehouse,
            r.database,
            CASE
                WHEN r.query_type LIKE 'CREATE%' THEN 'DDL'
                WHEN r.query_type IN ('SELECT', 'UNLOAD', 'GET_FILES') THEN 'read'
                ELSE 'write'
            END AS direction,
            COUNT(DISTINCT r.query_id) AS access_count
        FROM raw_access r
        LEFT JOIN client_mappings cm 
            ON LOWER(r.application) LIKE '%' || cm.pattern || '%'
        GROUP BY ALL
        """
 
        result_df = session.sql(query).to_pandas()
        if result_df.empty:
            st.warning("No data found for the last 7 days. Using sample data.")
            return sample_dataframe()
        return result_df
    except Exception as exc:
        st.error(f"Unable to query account usage data. Falling back to sample data.\n\nError: {exc}")
        return sample_dataframe()


@st.cache_data(show_spinner=False)
def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and aggregate dataframe."""
    if df.empty:
        return df
    df = df.dropna(how="any", axis=0)
    df = df.query("ACCESS_COUNT > 20")
    df = (
        df.groupby(
            ["DATABASE", "WAREHOUSE", "CLIENT", "DIRECTION", "ORGANIZATION_NAME", "ACCOUNT_NAME"],
            as_index=False,
        )
        .agg(ACCESS_COUNT=pd.NamedAgg(column="ACCESS_COUNT", aggfunc="sum"))
        .sort_values(by="ACCESS_COUNT", ascending=False)
    )
    return df


def _hex_to_rgb(hex_color: str) -> Optional[RGB]:
    """Convert hex color string to RGB tuple."""
    hex_color = (hex_color or "").strip().lstrip("#")
    if len(hex_color) != 6:
        return None
    try:
        return (
            int(hex_color[0:2], 16),
            int(hex_color[2:4], 16),
            int(hex_color[4:6], 16),
        )
    except ValueError:
        return None


def _relative_luminance(rgb: RGB) -> float:
    """Calculate relative luminance (0-1) from RGB tuple."""
    return (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255


def get_streamlit_theme_is_dark() -> bool:
    """Best-effort detection of Streamlit theme (works in local + SiS)."""
    try:
        base = st.get_option("theme.base")
        if base == "dark":
            return True
        if base == "light":
            return False

        # Prefer background colors if present
        for opt in ("theme.backgroundColor", "theme.secondaryBackgroundColor"):
            bg = st.get_option(opt)
            rgb = _hex_to_rgb(bg) if isinstance(bg, str) else None
            if rgb is not None:
                return _relative_luminance(rgb) < 0.5

        # Fall back to textColor if present (light text usually implies dark theme)
        txt = st.get_option("theme.textColor")
        rgb = _hex_to_rgb(txt) if isinstance(txt, str) else None
        if rgb is not None:
            return _relative_luminance(rgb) > 0.5
    except Exception:
        pass
    return False


def get_streamlit_theme_text_color() -> str:
    """Get a readable text color that matches Streamlit's theme."""
    try:
        txt = st.get_option("theme.textColor")
        if isinstance(txt, str) and _hex_to_rgb(txt) is not None:
            return txt
    except Exception:
        pass
    return "#fafafa" if get_streamlit_theme_is_dark() else MIDNIGHT


@st.cache_data(show_spinner=False)
def apply_filters(df: pd.DataFrame, database_name: str, warehouse_name: str, 
                 client_name: str, org_filter: str, direction_filter: str, 
                 access_count: int) -> pd.DataFrame:
    """Apply filters to dataframe with caching"""
    query_parts = [f"ACCESS_COUNT > {access_count}"]
    if database_name:
        query_parts.append(f"DATABASE.str.contains('{database_name}')")
    if warehouse_name:
        query_parts.append(f"WAREHOUSE.str.contains('{warehouse_name}')")
    if client_name:
        query_parts.append(f"CLIENT.str.contains('{client_name}')")
    if org_filter:
        query_parts.append(f"ORGANIZATION_NAME.str.contains('{org_filter}')")
    if direction_filter:
        query_parts.append(f"DIRECTION == '{direction_filter}'")

    filtered = df
    if query_parts:
        filtered = filtered.query(" & ".join(query_parts))
    return filtered

def build_network_html(df: pd.DataFrame, _node_images: Dict[str, str], dark_mode: bool = False) -> str:
    """Build network visualization HTML for either light or dark mode."""
    if df.empty:
        return "<p style='color: inherit;'>No rows available to render.</p>"

    current_account = get_current_account()

    # Set colors based on mode
    if dark_mode:
        bg_color = "#0e1117"  # Streamlit dark theme background
        font_color = "#fafafa"
    else:
        bg_color = "#ffffff"
        font_color = "#000000"

    net = Network(
        height="680px",
        width="100%",
        bgcolor=bg_color,
        font_color=font_color,
        notebook=False,
        directed=True,
        cdn_resources="in_line",
    )
    
    # Set global options
    options = f"""
        {{
          "nodes": {{
            "font": {{
              "size": 100,
              "color": "{font_color}",
               "shapeProperties": {{
                "useBorderWithImage": true,
                "borderType": "circle"  
              }}
            }}
          }},
          "edges": {{
            "font": {{
              "align": "middle",
              "size": 48,
              "strokeWidth": 2,
              "strokeColor": "{font_color}",
              "color": "{font_color}"
            }},
            "scaling": {{
              "label": {{
                "enabled": false
              }}
            }}
          }},
          "physics": {{
            "enabled": true,
            "barnesHut": {{
              "gravitationalConstant": -80000,
              "springLength": 200,
              "springConstant": 0.00007
            }},
            "minVelocity": 0.75
          }}
        }}
        """
    net.set_options(options)

    for _, row in df.iterrows():
        database = row["DATABASE"]
        warehouse = row["WAREHOUSE"]
        ac = int(row["ACCESS_COUNT"])
        direction = row["DIRECTION"]
        org_name = row["ORGANIZATION_NAME"]
        client = row["CLIENT"]

        if direction == "in":
            src = warehouse
            dst = database
            src_type = "Warehouse"
            dst_type = "Database"
            src_image = _node_images["warehouse"]
            dst_image = _node_images["database"]
        else:
            src = database
            dst = warehouse
            src_type = "Database"
            dst_type = "Warehouse"
            src_image = _node_images["database"]
            dst_image = _node_images["warehouse"]

        net.add_node(
            src,
            label=src,
            title=f"{src_type}: {src}\nOrganization: {org_name}\nAccount: {current_account}",
            size=200,
            color={"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)", "highlight": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"}},
            shape="image",
            shapeProperties={"useBorderWithImage": True, "borderType": "circle"},
            borderWidth=0,
            image=src_image,
        )
        net.add_node(
            dst,
            label=dst,
            title=f"{dst_type}: {dst}\nOrganization: {org_name}\nAccount: {current_account}",
            size=200,
            color={"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)", "highlight": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"}},
            shape="image",
            shapeProperties={"useBorderWithImage": True, "borderType": "circle"},
            image=dst_image,
        )
        
        net.add_edge(
            src, 
            dst, 
            value=ac, 
            color=SNOWFLAKE_BLUE,
            label=client, 
            arrowStrikethrough=False,
            font={
                'size': 48,
                'align': 'middle',
                'strokeWidth': 2,
                'strokeColor': font_color,
                'color': font_color
            }
        )

    html = net.generate_html(notebook=False)

    # JavaScript that polls parent theme and uses actual computed colors
    theme_js = """
    <script>
    (function() {
      var DARK_FG = '#fafafa';
      var LIGHT_FG = '#000000';
      var lastBg = null;
      var lastFg = null;

      function getParentBackground() {
        // Get actual computed background color from Streamlit's .stApp div
        try {
          var pdoc = window.parent.document;
          var app = pdoc.querySelector('.stApp');
          if (app) {
            var bg = window.parent.getComputedStyle(app).backgroundColor;
            if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') {
              return bg;
            }
          }
          // Fallback: try body background
          var body = pdoc.body;
          if (body) {
            var bg = window.parent.getComputedStyle(body).backgroundColor;
            if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') {
              return bg;
            }
          }
        } catch (e) {}
        return 'rgb(255, 255, 255)'; // Default fallback
      }

      function getParentTextColor() {
        // Get actual computed text color from .network-title element
        try {
          var pdoc = window.parent.document;
          var titleEl = pdoc.querySelector('.network-title');
          if (titleEl) {
            var color = window.parent.getComputedStyle(titleEl).color;
            if (color && color !== 'transparent' && color !== 'rgba(0, 0, 0, 0)') {
              return color;
            }
          }
          // Fallback: try .stApp text color
          var app = pdoc.querySelector('.stApp');
          if (app) {
            var color = window.parent.getComputedStyle(app).color;
            if (color && color !== 'transparent' && color !== 'rgba(0, 0, 0, 0)') {
              return color;
            }
          }
        } catch (e) {}
        return null; // Will use luminance-based fallback
      }

      function getLuminance(rgbStr) {
        // Parse rgb(r, g, b) or rgba(r, g, b, a) string
        var m = rgbStr.match(/\\d+/g);
        if (m && m.length >= 3) {
          var r = parseInt(m[0]);
          var g = parseInt(m[1]);
          var b = parseInt(m[2]);
          return (0.299 * r + 0.587 * g + 0.114 * b) / 255;
        }
        return 1; // Default to light
      }

      function applyTheme() {
        var bgColor = getParentBackground();
        var fgColor = getParentTextColor();
        
        // If no explicit text color found, derive from background luminance
        if (!fgColor) {
          var lum = getLuminance(bgColor);
          var isDark = lum < 0.5;
          fgColor = isDark ? DARK_FG : LIGHT_FG;
        }
        
        // Skip if nothing changed
        if (bgColor === lastBg && fgColor === lastFg) return;
        lastBg = bgColor;
        lastFg = fgColor;

        // Set backgrounds to match parent exactly
        document.body.style.backgroundColor = bgColor;
        var container = document.getElementById('mynetwork');
        if (container) container.style.backgroundColor = bgColor;

        // Update canvas background
        var canvas = document.querySelector('canvas');
        if (canvas) {
          var ctx = canvas.getContext('2d');
          ctx.fillStyle = bgColor;
          ctx.fillRect(0, 0, canvas.width, canvas.height);
        }

        // Update vis.js network
        if (typeof network !== 'undefined' && network) {
          // Update label colors
          if (network.setOptions) {
            network.setOptions({
              nodes: { font: { color: fgColor } },
              edges: { font: { color: fgColor, strokeColor: fgColor } }
            });
          }
          // Update existing nodes (font color only, keep transparent background)
          try {
            var nodes = network.body.data.nodes;
            if (nodes && nodes.getIds) {
              var ids = nodes.getIds();
              var updates = [];
              for (var i = 0; i < ids.length; i++) {
                updates.push({ id: ids[i], font: { color: fgColor } });
              }
              nodes.update(updates);
            }
          } catch (e) {}
          // Update existing edges
          try {
            var edges = network.body.data.edges;
            if (edges && edges.getIds) {
              var ids = edges.getIds();
              var updates = [];
              for (var i = 0; i < ids.length; i++) {
                updates.push({ id: ids[i], font: { color: fgColor, strokeColor: fgColor } });
              }
              edges.update(updates);
            }
          } catch (e) {}
          // Force redraw
          network.redraw();
        }

        // Update tooltip styling based on theme
        var lum = getLuminance(bgColor);
        var isDark = lum < 0.5;
        var tooltipBg = isDark ? '#374151' : '#ffffff';
        var tooltipText = isDark ? '#f9fafb' : '#1f2937';
        var tooltipShadow = isDark ? 'rgba(0, 0, 0, 0.4)' : 'rgba(0, 0, 0, 0.15)';
        var tooltipBorder = isDark ? '#4b5563' : '#e5e7eb';
        document.documentElement.style.setProperty('--tooltip-bg', tooltipBg);
        document.documentElement.style.setProperty('--tooltip-text', tooltipText);
        document.documentElement.style.setProperty('--tooltip-shadow', tooltipShadow);
        document.documentElement.style.setProperty('--tooltip-border', tooltipBorder);
      }

      // Poll every 200ms to catch theme changes
      setInterval(applyTheme, 200);
      window.addEventListener('load', applyTheme);
      // Also run immediately
      applyTheme();
    })();
    </script>
    <style>
      html, body { margin: 0; padding: 0; background: transparent !important; }
      #mynetwork { border: 0 !important; }
      /* Style vis.js tooltips to match Altair/Vega tooltips - theme-aware */
      :root {
        --tooltip-bg: #ffffff;
        --tooltip-text: #1f2937;
        --tooltip-shadow: rgba(0, 0, 0, 0.15);
        --tooltip-border: #e5e7eb;
      }
      div.vis-tooltip {
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 12px !important;
        background-color: var(--tooltip-bg) !important;
        color: var(--tooltip-text) !important;
        border: 1px solid var(--tooltip-border) !important;
        border-radius: 4px !important;
        padding: 8px 10px !important;
        box-shadow: 0 2px 8px var(--tooltip-shadow) !important;
        line-height: 1.5 !important;
        white-space: pre-line !important;
        max-width: 300px !important;
      }
    </style>
    """

    # Insert before closing body
    if "</body>" in html:
        html = html.replace("</body>", theme_js + "\n</body>")
    else:
        html = html + theme_js

    return html


@st.cache_data(show_spinner=False)
def prepare_chart_data(df: pd.DataFrame, column: str, top_n: int = 10) -> pd.DataFrame:
    """Prepare aggregated data for charts"""
    if df.empty:
        return pd.DataFrame()
    
    return (
        df[[column, "ACCESS_COUNT"]]
        .groupby(column)["ACCESS_COUNT"]
        .sum()
        .reset_index()
        .rename(columns={column: column.title(), "ACCESS_COUNT": "Access Count"})
        .sort_values("Access Count", ascending=False)
        .head(top_n)
    )


@st.cache_data(show_spinner=False)
def get_distinct_values(df: pd.DataFrame, column: str) -> list:
    """Get distinct values for filter dropdowns"""
    if df.empty:
        return [""]
    return [""] + sorted(df[column].astype(str).unique().tolist())


def sidebar_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Render sidebar filters and return filtered dataframe."""
    st.sidebar.header("Filters")

    if df.empty:
        return df

    database_options = get_distinct_values(df, "DATABASE")
    warehouse_options = get_distinct_values(df, "WAREHOUSE")
    client_options = get_distinct_values(df, "CLIENT")
    org_options = get_distinct_values(df, "ORGANIZATION_NAME")
    direction_options = get_distinct_values(df, "DIRECTION")

    database_name = st.sidebar.selectbox("Select Database", database_options)
    warehouse_name = st.sidebar.selectbox("Select Warehouse", warehouse_options)
    client_name = st.sidebar.selectbox("Select Client", client_options)
    org_filter = st.sidebar.selectbox("Select Organization", org_options)
    direction_filter = st.sidebar.selectbox("Select Direction", direction_options)
    access_count = st.sidebar.number_input(
        label="Access Count",
        min_value=1,
        max_value=1_000_000,
        step=10,
        value=1,
        help="Please enter a number between 1 and 1,000,000",
    )

    return apply_filters(df, database_name, warehouse_name, client_name,
                         org_filter, direction_filter, access_count)

def render_bar_charts(df: pd.DataFrame, dark_mode: bool = False) -> None:
    if df.empty:
        st.warning("Apply different filters or load more data to see charts.")
        return

    # Theme-aware colors: derive from Streamlit theme (more reliable than passing flags around)
    is_dark = get_streamlit_theme_is_dark()
    text_color = get_streamlit_theme_text_color()
    bar_color = STAR_BLUE if is_dark else SNOWFLAKE_BLUE
    grid_color = "rgba(255,255,255,0.18)" if is_dark else "rgba(0,0,0,0.10)"
    domain_color = "rgba(255,255,255,0.28)" if is_dark else "rgba(0,0,0,0.25)"
    muted_text = "rgba(255,255,255,0.72)" if is_dark else "rgba(0,0,0,0.72)"

    def bar_chart(column, width=400):
        # Use cached chart data preparation
        data = prepare_chart_data(df, column)
        if data.empty:
            return
            
        chart = (
            alt.Chart(data)
            .mark_bar(
                color=bar_color,
                cornerRadiusEnd=4,
            )
            .encode(
                x=alt.X("Access Count:Q", title="Access Count"),
                y=alt.Y(f"{column.title()}:N", sort="-x", title=column.title()),
                tooltip=[
                    alt.Tooltip(column.title() + ":N", title=column.title()),
                    alt.Tooltip("Access Count:Q", title="Access Count", format=",")
                ],
            )
            .properties(
                width=width, 
                height=350, 
                title=alt.TitleParams(
                    text=f"Access Count by {column.title()}",
                    fontSize=16,
                    fontWeight="bold",
                    color=text_color,
                    font="Lato"
                )
            )
            .configure_axis(
                labelFont="Lato",
                titleFont="Lato",
                labelFontSize=12,
                titleFontSize=12,
                labelColor=text_color,
                titleColor=text_color,
                gridColor=grid_color,
                domainColor=domain_color,
                tickColor=domain_color,
                labelPadding=6,
                titlePadding=10,
            )
            .configure_legend(
                labelFont="Lato",
                titleFont="Lato",
                labelColor=text_color,
                titleColor=text_color,
            )
            .configure_title(
                font="Lato",
                color=text_color,
            )
            .configure_view(
                strokeWidth=0
            )
            .configure(background="transparent")
        )
        st.altair_chart(chart, use_container_width=True)

    col21, col22 = st.columns(2)
    with col21:
        bar_chart("CLIENT")
    with col22:
        bar_chart("DATABASE")

    bar_chart("WAREHOUSE", width=800)


def render_network_section(df: pd.DataFrame, network_html: str) -> None:
    """Render network visualization."""
    if st.session_state.get("full_screen_mode", False):
        title_col, button_col = st.columns([9, 1])
        with title_col:
            st.markdown('<div class="network-title">Data Lakeview - Full Screen View</div>', unsafe_allow_html=True)
        with button_col:
            if st.button("⊞", help="Return to Split View"):
                st.session_state["full_screen_mode"] = False
                st.rerun()
        
        st.components.v1.html(network_html, height=800)
        return

    col1, col2 = st.columns(2, gap="small")
    with col1:
        title_col, button_col = st.columns([9, 1])
        with title_col:
            st.markdown('<div class="network-title">Data Lakeview</div>', unsafe_allow_html=True)
        with button_col:
            if st.button("⛶", help="Full Screen"):
                st.session_state["full_screen_mode"] = True
                st.rerun()
        
        st.components.v1.html(network_html, height=800, width=800)
    
    with col2:
        render_bar_charts(df)


def main() -> None:
    # Apply CSS (let Streamlit theme control light/dark text colors)
    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)
    
    # Snowflake branded header with refresh button
    col1, col2 = st.columns([6, 1])
    with col1:
        render_snowflake_header()
    with col2:
        if st.button("↻", help="Clear cache and reload data"):
            st.cache_data.clear()
            st.rerun()

    # Load and process data
    raw_df = load_data()
    processed_df = process_dataframe(raw_df)
    filtered_df = sidebar_filters(processed_df)

    node_images = load_node_images()
    network_html = build_network_html(filtered_df, node_images)

    render_network_section(filtered_df, network_html)
    st.sidebar.markdown("Powered by Streamlit :streamlit:")

if __name__ == "__main__":
    main()
