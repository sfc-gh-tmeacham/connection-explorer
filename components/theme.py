"""Snowflake brand theming: colors, CSS, and theme detection."""

from typing import Optional, Tuple

import streamlit as st

# Type alias for RGB tuple
RGB = Tuple[int, int, int]

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
        .network-title {
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
        .st-emotion-cache-wfksaw {
            gap: 0rem !important;
        }
        /* Fullscreen button alignment - target the column containing the marker */
        .stColumn:has(.fullscreen-btn-container) > div {
            display: flex !important;
            flex-direction: column !important;
            align-items: flex-end !important;
        }
    </style>
    """


def _hex_to_rgb(hex_color: str) -> Optional[RGB]:
    """Convert hex color string to RGB tuple."""
    hex_color = (hex_color or "").strip().lstrip("#")
    if len(hex_color) != 6:
        return None
    try:
        return (int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16))
    except ValueError:
        return None


def _relative_luminance(rgb: RGB) -> float:
    """Calculate relative luminance (0-1) from RGB tuple."""
    return (0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]) / 255


def get_theme_colors() -> Tuple[bool, str]:
    """Return (is_dark, text_color) tuple for current Streamlit theme."""
    is_dark = False
    try:
        base = st.get_option("theme.base")
        if base == "dark":
            is_dark = True
        elif base != "light":
            for opt in ("theme.backgroundColor", "theme.secondaryBackgroundColor"):
                bg = st.get_option(opt)
                rgb = _hex_to_rgb(bg) if isinstance(bg, str) else None
                if rgb:
                    is_dark = _relative_luminance(rgb) < 0.5
                    break
            else:
                txt = st.get_option("theme.textColor")
                rgb = _hex_to_rgb(txt) if isinstance(txt, str) else None
                if rgb:
                    is_dark = _relative_luminance(rgb) > 0.5
    except Exception:
        pass

    text_color = "#fafafa" if is_dark else MIDNIGHT
    try:
        txt = st.get_option("theme.textColor")
        if isinstance(txt, str) and _hex_to_rgb(txt):
            text_color = txt
    except Exception:
        pass

    return is_dark, text_color
