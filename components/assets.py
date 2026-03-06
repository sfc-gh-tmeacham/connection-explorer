"""Static asset loading and header rendering.

Handles base64-encoding of image files (logo, node icons) and renders
the branded header block at the top of the Streamlit app.
"""

import base64
import logging
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)

import streamlit as st

from components.theme import MID_BLUE, SNOWFLAKE_BLUE

APP_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = APP_DIR / "static"
FAVICON_PATH = STATIC_DIR / "snowflake-bug-logo.svg"


@st.cache_resource(show_spinner=False)
def load_snowflake_logo() -> str:
    """Load the Snowflake bug logo as a base64-encoded string.

    Cached via ``st.cache_resource`` so the file is read only once per
    Streamlit server lifetime.

    Returns:
        A UTF-8 base64 string of the SVG image data.
    """
    with open(STATIC_DIR / "snowflake-bug-logo.svg", "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")
    return encoded


@st.cache_resource(show_spinner=False)
def load_node_images() -> Dict[str, str]:
    """Load database and warehouse node images as base64 data URIs.

    Returns:
        A dict mapping node type names (``"database"``, ``"warehouse"``) to
        ``data:image/svg+xml;base64,...`` URI strings suitable for vis.js
        image nodes.
    """

    def encode_image(file_name: str) -> str:
        """Encode a single SVG file from the static directory.

        Args:
            file_name: Filename relative to ``STATIC_DIR``.

        Returns:
            A ``data:image/svg+xml;base64,...`` URI string.
        """
        with open(STATIC_DIR / file_name, "rb") as image_file:
            encoded = base64.b64encode(image_file.read()).decode("utf-8")
        return f"data:image/svg+xml;base64,{encoded}"

    # Wrap warehouse icon inside a diamond-shaped badge (matches the circle
    # treatment that client nodes get in client_mappings.py).
    with open(STATIC_DIR / "snowflake-warehouse.svg", "r") as f:
        wh_inner = f.read()
    # Strip the outer <svg> wrapper so we can nest the content inside our own.
    # The inner content is a <g> with the potrace path data.  Change the fill
    # from Snowflake blue to white so the gear is visible on the blue diamond.
    inner_content = wh_inner.split(">", 1)[1].rsplit("</svg>", 1)[0]
    inner_content = inner_content.replace('fill="#29B5E8"', 'fill="white"')
    diamond_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
        f'<polygon points="64,4 124,64 64,124 4,64" fill="{MID_BLUE}" stroke="white" stroke-width="3"/>'
        '<svg x="28" y="28" width="72" height="72" viewBox="0 0 324 324">'
        f'{inner_content}'
        '</svg>'
        '</svg>'
    )
    wh_encoded = base64.b64encode(diamond_svg.encode()).decode("utf-8")
    logger.info("Loaded node images: warehouse (diamond), database (square)")

    # Wrap database icon inside a rounded-square badge.
    with open(STATIC_DIR / "snowflake-database.svg", "r") as f:
        db_inner = f.read()
    db_content = db_inner.split(">", 1)[1].rsplit("</svg>", 1)[0]
    db_content = db_content.replace('fill="#29B5E8"', 'fill="white"')
    square_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
        '<rect x="4" y="4" width="120" height="120" rx="12" ry="12" fill="#29B5E8" stroke="white" stroke-width="3"/>'
        '<svg x="28" y="28" width="72" height="72" viewBox="0 0 200 203">'
        f'{db_content}'
        '</svg>'
        '</svg>'
    )
    db_encoded = base64.b64encode(square_svg.encode()).decode("utf-8")

    return {
        "database": f"data:image/svg+xml;base64,{db_encoded}",
        "warehouse": f"data:image/svg+xml;base64,{wh_encoded}",
    }


def render_snowflake_header() -> None:
    """Render the app header with the Snowflake Connection Explorer title and Horizon Catalog badge.

    Injects an ``unsafe_allow_html`` markdown block containing the Snowflake
    bug logo, the app title, and the Horizon Catalog sub-header styled with
    Snowflake brand colors.
    """
    logo_b64 = load_snowflake_logo()

    st.markdown(
        f'''
        <div style="padding: 0.25rem 0; padding-left: 50px;">
            <h1 style="font-family: Lato, sans-serif; font-size: 32px; font-weight: 900; 
                       color: inherit; letter-spacing: 0.02em; line-height: 1.2; 
                       margin: 0; padding: 0; text-align: left;">
                Snowflake Connection Explorer
            </h1>
            <div style="display: flex; align-items: center; gap: 8px; margin-top: 8px;">
                <img src="data:image/svg+xml;base64,{logo_b64}" width="24" height="24" style="display: block;">
                <span style="font-family: Lato, sans-serif; font-size: 18px; font-weight: 700; 
                             color: {SNOWFLAKE_BLUE}; letter-spacing: 0.02em; line-height: 1;">
                    Horizon Catalog
                </span>
            </div>
        </div>
        ''',
        unsafe_allow_html=True
    )
