"""Static asset loading and header rendering."""

import base64
from pathlib import Path
from typing import Dict

import streamlit as st

from components.theme import SNOWFLAKE_BLUE

APP_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = APP_DIR / "static"
FAVICON_PATH = STATIC_DIR / "snowflake-bug-logo.png"


@st.cache_resource(show_spinner=False)
def load_snowflake_logo() -> str:
    """Load Snowflake bug logo as base64 encoded string."""
    with open(STATIC_DIR / "snowflake-bug-logo.png", "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return encoded


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


def render_snowflake_header() -> None:
    """Render header with Data Lake Explorer as main title and Horizon Catalog as subheader."""
    logo_b64 = load_snowflake_logo()

    st.markdown(
        f'''
        <div style="padding: 0.25rem 0; padding-left: 50px;">
            <h1 style="font-family: Lato, sans-serif; font-size: 32px; font-weight: 900; 
                       color: inherit; letter-spacing: 0.02em; line-height: 1.2; 
                       margin: 0; padding: 0; text-align: left;">
                Data Lake Explorer
            </h1>
            <div style="display: flex; align-items: center; gap: 8px; margin-top: 8px;">
                <img src="data:image/png;base64,{logo_b64}" width="24" height="24" style="display: block;">
                <span style="font-family: Lato, sans-serif; font-size: 18px; font-weight: 700; 
                             color: {SNOWFLAKE_BLUE}; letter-spacing: 0.02em; line-height: 1;">
                    Horizon Catalog
                </span>
            </div>
        </div>
        ''',
        unsafe_allow_html=True
    )
