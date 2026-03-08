"""About page — Cortex Code productivity comparison."""

import streamlit as st
import streamlit.components.v1 as components

from components.theme import SNOWFLAKE_BLUE, MID_BLUE, STAR_BLUE, AMBER, READ_GREEN, is_dark_theme


# ---------------------------------------------------------------------------
# Data: component-level time estimates (hours)
# ---------------------------------------------------------------------------
COMPONENTS = [
    ("Debugging & Iteration", 20, 1.0),
    ("Theme, Styling & UI Polish", 15, 1.0),
    ("Client Classification\n(257 mappings + SVGs)", 15, 1.0),
    ("Data Layer &\nDeploy Scripts", 10, 1.0),
    ("Testing &\nDocumentation", 10, 0.5),
    ("Charts (Bar / Heatmap\n/ Treemap / Sankey)", 20, 1.0),
    ("Network Graph\n(vis.js + Components v2)", 30, 2.0),
]

MANUAL_TOTAL = sum(r[1] for r in COMPONENTS)
CORTEX_TOTAL = sum(r[2] for r in COMPONENTS)
SPEEDUP = round(MANUAL_TOTAL / CORTEX_TOTAL)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    """Convert a hex color like '#29B5E8' to (r, g, b)."""
    h = hex_color.lstrip("#")
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


# Pre-computed RGB tuples for brand colors (avoids repeated _hex_to_rgb calls)
_RGB_SNOWFLAKE = _hex_to_rgb(SNOWFLAKE_BLUE)
_RGB_STAR = _hex_to_rgb(STAR_BLUE)
_RGB_AMBER = _hex_to_rgb(AMBER)
_RGB_MID = _hex_to_rgb(MID_BLUE)


def _theme_palette(is_dark: bool) -> tuple[str, str, str, str]:
    """Return (bg_color, text_color, muted_color, label_color) for the current theme."""
    bg = "#0E1117" if is_dark else "#FFFFFF"
    text = "#E0E0E0" if is_dark else "#333"
    muted = "#999" if is_dark else "#666"
    label = "#CCC" if is_dark else "#444"
    return bg, text, muted, label


def _base_css(bg_color: str, extra_body: str = "") -> str:
    """Return the common CSS reset + body rule shared by all HTML blocks."""
    return f"""* {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: {bg_color};
    overflow: hidden;
    {extra_body}
  }}"""


@st.cache_data
def _section_title_html(title: str, svg_icon: str, is_dark: bool) -> str:
    """Render a section heading as styled HTML matching the hero gradient."""
    bg_color, _, _, _ = _theme_palette(is_dark)
    br, bg_, bb = _RGB_SNOWFLAKE
    sr, sg, sb = _RGB_STAR
    return f"""<!DOCTYPE html>
<html><head><style>
  {_base_css(bg_color, "padding: 0 4px;")}
  .section-title {{
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    font-size: 18px;
    font-weight: 800;
    opacity: 0;
    animation: fadeUp 0.5s ease 0.05s forwards;
  }}
  .section-title .icon {{
    display: flex;
    align-items: center;
    line-height: 0;
  }}
  .section-title .text {{
    background: linear-gradient(135deg, {SNOWFLAKE_BLUE}, {STAR_BLUE});
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }}
  @keyframes fadeUp {{
    0% {{ opacity: 0; transform: translateY(8px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
  }}
</style></head>
<body>
  <div class="section-title">
    <span class="icon">{svg_icon}</span>
    <span class="text">{title}</span>
  </div>
</body></html>"""


# SVG icons for section titles (18x18)
_ICON_COMPARE = f'<svg viewBox="0 0 18 18" fill="none" width="22" height="22"><path d="M5 4l-3.5 5L5 14" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><path d="M13 4l3.5 5L13 14" stroke="{STAR_BLUE}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><line x1="3" y1="9" x2="15" y2="9" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.2" stroke-linecap="round" opacity="0.5"/></svg>'
_ICON_SPARKLE = f'<svg viewBox="0 0 18 18" fill="none" width="22" height="22"><path d="M9 1.5v3M9 13.5v3M1.5 9h3M13.5 9h3M3.7 3.7l2.1 2.1M12.2 12.2l2.1 2.1M14.3 3.7l-2.1 2.1M5.8 12.2l-2.1 2.1" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.3" stroke-linecap="round"/><circle cx="9" cy="9" r="2" stroke="{STAR_BLUE}" stroke-width="1.3"/></svg>'
_ICON_DASHBOARD = f'<svg viewBox="0 0 18 18" fill="none" width="22" height="22"><rect x="1.5" y="1.5" width="6" height="6" rx="1.5" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.3"/><rect x="10.5" y="1.5" width="6" height="6" rx="1.5" stroke="{STAR_BLUE}" stroke-width="1.3"/><rect x="1.5" y="10.5" width="6" height="6" rx="1.5" stroke="{STAR_BLUE}" stroke-width="1.3"/><rect x="10.5" y="10.5" width="6" height="6" rx="1.5" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.3"/></svg>'


@st.cache_data
def _build_css_chart_html(is_dark: bool) -> str:
    """Build a pure CSS/HTML animated horizontal bar chart."""
    max_val = max(r[1] for r in COMPONENTS)
    bg_color, text_color, _, label_color = _theme_palette(is_dark)
    muted_color = "#888" if is_dark else "#999"
    row_hover = "rgba(255,255,255,0.03)" if is_dark else "rgba(0,0,0,0.02)"

    r, g, b = _RGB_AMBER
    amber_bar = f"rgba({r},{g},{b},0.7)"
    amber_glow = f"rgba({r},{g},{b},0.15)"
    br, bg_, bb = _RGB_SNOWFLAKE
    blue_bar = f"rgba({br},{bg_},{bb},0.85)"
    blue_glow = f"rgba({br},{bg_},{bb},0.15)"

    rows_html = ""
    for i, (name, manual, cortex) in enumerate(COMPONENTS):
        label = name.replace("\n", "<br>")
        manual_pct = (manual / max_val) * 100
        cortex_pct = (cortex / max_val) * 100
        delay = i * 0.08

        rows_html += f"""
        <div class="chart-row" style="animation-delay: {delay}s;">
          <div class="row-label">{label}</div>
          <div class="row-bars">
            <div class="bar-group">
              <div class="bar manual-bar" style="--target-width: {manual_pct}%; animation-delay: {delay + 0.3}s;">
              </div>
              <span class="bar-value" style="animation-delay: {delay + 0.3}s;">{manual}h</span>
            </div>
            <div class="bar-group">
              <div class="bar cortex-bar" style="--target-width: {cortex_pct}%; animation-delay: {delay + 0.45}s;">
              </div>
              <span class="bar-value" style="animation-delay: {delay + 0.45}s;">{cortex}h</span>
            </div>
          </div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
  {_base_css(bg_color, f"color: {text_color}; padding: 12px 16px 8px 16px;")}

  .legend {{
    display: flex;
    justify-content: center;
    gap: 28px;
    margin-bottom: 16px;
    font-size: 12.5px;
    font-weight: 600;
    letter-spacing: 0.02em;
  }}
  .legend-item {{
    display: flex;
    align-items: center;
    gap: 8px;
    color: {muted_color};
  }}
  .legend-dot {{
    width: 12px;
    height: 12px;
    border-radius: 3px;
  }}
  .legend-dot.manual {{ background: {amber_bar}; }}
  .legend-dot.cortex {{ background: {blue_bar}; }}

  .chart-row {{
    display: flex;
    align-items: center;
    padding: 6px 0;
    border-radius: 6px;
    transition: background 0.2s;
    opacity: 0;
    animation: fadeInRow 0.4s ease forwards;
  }}
  .chart-row:hover {{
    background: {row_hover};
  }}

  .row-label {{
    width: 200px;
    min-width: 200px;
    text-align: right;
    padding-right: 16px;
    font-size: 12px;
    line-height: 1.35;
    color: {label_color};
    font-weight: 500;
  }}

  .row-bars {{
    flex: 1;
    display: flex;
    flex-direction: column;
    gap: 3px;
    min-width: 0;
    padding-right: 44px;
  }}

  .bar-group {{
    height: 16px;
    position: relative;
    display: flex;
    align-items: center;
  }}

  .bar {{
    height: 100%;
    border-radius: 4px;
    width: 0;
    animation: growBar 0.9s cubic-bezier(0.22, 0.61, 0.36, 1) forwards;
    display: flex;
    align-items: center;
    position: relative;
    min-width: 0;
  }}

  .manual-bar {{
    background: linear-gradient(90deg, {amber_bar}, {AMBER});
    box-shadow: 0 1px 8px {amber_glow};
  }}
  .cortex-bar {{
    background: linear-gradient(90deg, {blue_bar}, {SNOWFLAKE_BLUE});
    box-shadow: 0 1px 8px {blue_glow};
    overflow: hidden;
  }}
  .cortex-bar::after {{
    content: '';
    position: absolute;
    top: 0;
    left: -100%;
    width: 100%;
    height: 100%;
    background: linear-gradient(
      90deg,
      transparent 0%,
      rgba(255,255,255,0.25) 45%,
      rgba(255,255,255,0.35) 50%,
      rgba(255,255,255,0.25) 55%,
      transparent 100%
    );
    animation: shimmer 1.8s ease-in-out infinite;
    animation-delay: 1.2s;
    border-radius: inherit;
  }}

  .bar-value {{
    position: relative;
    margin-left: 6px;
    font-size: 11px;
    font-weight: 700;
    white-space: nowrap;
    opacity: 0;
    animation: fadeIn 0.3s ease forwards;
    flex-shrink: 0;
  }}
  .manual-bar + .bar-value {{
    color: {AMBER if is_dark else "#B8860B"};
  }}
  .cortex-bar + .bar-value {{
    color: {SNOWFLAKE_BLUE};
  }}

  @keyframes growBar {{
    0% {{ width: 0; }}
    100% {{ width: var(--target-width); }}
  }}
  @keyframes fadeIn {{
    0% {{ opacity: 0; transform: translateX(-4px); }}
    100% {{ opacity: 1; transform: translateX(0); }}
  }}
  @keyframes fadeInRow {{
    0% {{ opacity: 0; transform: translateX(-8px); }}
    100% {{ opacity: 1; transform: translateX(0); }}
  }}
  @keyframes shimmer {{
    0% {{ left: -100%; }}
    100% {{ left: 200%; }}
  }}
</style>
</head>
<body>
  <div class="legend">
    <div class="legend-item"><div class="legend-dot manual"></div>Manual Estimate</div>
    <div class="legend-item"><div class="legend-dot cortex"></div>With Cortex Code</div>
  </div>
  {rows_html}
</body>
</html>"""


@st.cache_data
def _build_hero_html(is_dark: bool) -> str:
    """Build hero title + 4 metric cards as a single styled HTML block."""
    bg_color, text_color, muted_color, _ = _theme_palette(is_dark)
    accent = STAR_BLUE if is_dark else SNOWFLAKE_BLUE

    br, bg_, bb = _RGB_SNOWFLAKE
    sr, sg, sb = _RGB_STAR
    ar, ag, ab = _RGB_AMBER

    # Card data: (label, value, svg_icon, color)
    # Each card gets a unique icon and distinct color
    cards = [
        ("Development Time", f"~{CORTEX_TOTAL:.0f} hrs",
         '<svg viewBox="0 0 24 24" fill="none" width="24" height="24"><rect x="5" y="9" width="14" height="10" rx="2" stroke="{c}" stroke-width="1.5"/><circle cx="9" cy="14" r="1.5" stroke="{c}" stroke-width="1.3"/><circle cx="15" cy="14" r="1.5" stroke="{c}" stroke-width="1.3"/><path d="M10 17.5h4" stroke="{c}" stroke-width="1.3" stroke-linecap="round"/><path d="M12 9V6" stroke="{c}" stroke-width="1.5" stroke-linecap="round"/><circle cx="12" cy="4.5" r="1.5" stroke="{c}" stroke-width="1.3"/><path d="M3 13h2M19 13h2" stroke="{c}" stroke-width="1.3" stroke-linecap="round"/></svg>',
         accent),
        ("Manual Estimate", f"~{MANUAL_TOTAL} hrs",
         '<svg viewBox="0 0 24 24" fill="none" width="24" height="24"><circle cx="12" cy="12" r="9" stroke="{c}" stroke-width="1.5"/><path d="M12 7v5l3.5 3.5" stroke="{c}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
         AMBER),
        ("Speedup", f"~{SPEEDUP}x",
         '<svg viewBox="0 0 24 24" fill="none" width="24" height="24"><circle cx="12" cy="12" r="9" stroke="{c}" stroke-width="1.5"/><path d="M12 12l4-3" stroke="{c}" stroke-width="1.8" stroke-linecap="round"/><circle cx="12" cy="12" r="1" fill="{c}"/><path d="M7.5 17.5h9" stroke="{c}" stroke-width="1.3" stroke-linecap="round"/></svg>',
         READ_GREEN),
        ("Lines of Code", "5,096",
         '<svg viewBox="0 0 24 24" fill="none" width="24" height="24"><path d="M7 8L3 12l4 4" stroke="{c}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M17 8l4 4-4 4" stroke="{c}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><line x1="14" y1="4" x2="10" y2="20" stroke="{c}" stroke-width="1.3" stroke-linecap="round"/></svg>',
         STAR_BLUE),
    ]

    cards_html = ""
    for i, (label, value, svg, color) in enumerate(cards):
        cr, cg, cb = _hex_to_rgb(color)
        icon_svg = svg.replace("{c}", color)
        delay = 0.15 + i * 0.1
        cards_html += f"""
        <div class="metric-card" style="
          --card-accent: {color};
          --card-bg: rgba({cr},{cg},{cb}, 0.06);
          --card-border: rgba({cr},{cg},{cb}, 0.25);
          --card-glow: rgba({cr},{cg},{cb}, 0.10);
          animation-delay: {delay}s;
        ">
          <div class="metric-icon">{icon_svg}</div>
          <div class="metric-value" style="color: {color};">{value}</div>
          <div class="metric-label">{label}</div>
        </div>
        """

    return f"""<!DOCTYPE html>
<html><head><style>
  {_base_css(bg_color, "padding: 0 4px;")}

  .hero-title {{
    font-size: 22px;
    font-weight: 800;
    text-align: center;
    margin-bottom: 6px;
    opacity: 0;
    animation: fadeUp 0.5s ease 0.05s forwards;
  }}
  .hero-title .bolt {{
    display: inline-block;
    vertical-align: middle;
    margin-right: 6px;
    margin-top: -2px;
  }}
  .hero-title .text {{
    background: linear-gradient(135deg, {SNOWFLAKE_BLUE}, {STAR_BLUE});
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
  }}

  .hero-sub {{
    text-align: center;
    font-size: 13.5px;
    color: {muted_color};
    margin-bottom: 20px;
    opacity: 0;
    animation: fadeUp 0.5s ease 0.1s forwards;
    line-height: 1.5;
  }}
  .hero-sub strong {{
    color: {text_color};
    font-weight: 700;
  }}

  .metrics-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
  }}

  .metric-card {{
    background: var(--card-bg);
    border: 1px solid var(--card-border);
    border-radius: 12px;
    padding: 1.1rem 1rem;
    text-align: center;
    opacity: 0;
    animation: fadeUp 0.5s ease forwards;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
  }}
  .metric-card:hover {{
    transform: translateY(-2px);
    box-shadow: 0 4px 16px var(--card-glow);
  }}

  .metric-icon {{
    margin-bottom: 6px;
    line-height: 0;
  }}

  .metric-value {{
    font-size: 28px;
    font-weight: 900;
    line-height: 1.2;
    margin-bottom: 4px;
    letter-spacing: -0.02em;
  }}

  .metric-label {{
    font-size: 11px;
    font-weight: 600;
    color: {muted_color};
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }}

  @keyframes fadeUp {{
    0% {{ opacity: 0; transform: translateY(10px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
  }}
</style></head>
<body>
  <div class="hero-title">
    <span class="bolt"><svg viewBox="0 0 20 20" fill="none" width="20" height="20"><path d="M11 1L3.5 11.5h5L7 19l9-11h-5.5L11 1z" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.5" fill="rgba({br},{bg_},{bb},0.15)" stroke-linecap="round" stroke-linejoin="round"/></svg></span>
    <span class="text">Built with Cortex Code</span>
  </div>
  <div class="hero-sub">
    This application was built collaboratively with
    <strong>Snowflake Cortex Code CLI</strong> &mdash; an AI-powered coding
    assistant that accelerates development from architecture to deployment.
  </div>
  <div class="metrics-grid">
    {cards_html}
  </div>
</body></html>"""


@st.cache_data
def _build_capability_cards_html(is_dark: bool) -> str:
    """Build a pure CSS/HTML capability cards section with icons and animations."""
    bg_color, text_color, _, _ = _theme_palette(is_dark)
    muted_color = "#AAA" if is_dark else "#666"

    br, bg_, bb = _RGB_SNOWFLAKE
    blue_bg = f"rgba({br},{bg_},{bb}, 0.06)" if is_dark else f"rgba({br},{bg_},{bb}, 0.04)"
    blue_border = f"rgba({br},{bg_},{bb}, 0.3)"
    blue_glow = f"rgba({br},{bg_},{bb}, 0.12)"

    # SVG icon paths (16x16 viewBox) — one per bullet item
    code_gen_icons = [
        # Network/nodes
        '<svg viewBox="0 0 16 16" fill="none"><circle cx="4" cy="4" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="12" cy="12" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="12" cy="4" r="2" stroke="{c}" stroke-width="1.2"/><line x1="5.5" y1="5" x2="10.5" y2="11" stroke="{c}" stroke-width="1"/><line x1="6" y1="4" x2="10" y2="4" stroke="{c}" stroke-width="1"/></svg>',
        # Tag/label
        '<svg viewBox="0 0 16 16" fill="none"><path d="M2 3h5.5l6 6-4.5 4.5-6-6V3z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><circle cx="5.5" cy="5.5" r="1" fill="{c}"/></svg>',
        # Bar chart
        '<svg viewBox="0 0 16 16" fill="none"><rect x="2" y="8" width="3" height="6" rx="0.5" stroke="{c}" stroke-width="1.2"/><rect x="6.5" y="5" width="3" height="9" rx="0.5" stroke="{c}" stroke-width="1.2"/><rect x="11" y="2" width="3" height="12" rx="0.5" stroke="{c}" stroke-width="1.2"/></svg>',
        # Database
        '<svg viewBox="0 0 16 16" fill="none"><ellipse cx="8" cy="4" rx="5" ry="2" stroke="{c}" stroke-width="1.2"/><path d="M3 4v8c0 1.1 2.24 2 5 2s5-.9 5-2V4" stroke="{c}" stroke-width="1.2"/><path d="M3 8c0 1.1 2.24 2 5 2s5-.9 5-2" stroke="{c}" stroke-width="1.2"/></svg>',
        # Checkmark/shield
        '<svg viewBox="0 0 16 16" fill="none"><path d="M8 1.5L2.5 4v4c0 3.5 2.3 5.8 5.5 6.5 3.2-.7 5.5-3 5.5-6.5V4L8 1.5z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><path d="M5.5 8l2 2 3.5-3.5" stroke="{c}" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
        # Document
        '<svg viewBox="0 0 16 16" fill="none"><path d="M4 2h5.5L13 5.5V14H4V2z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><path d="M9.5 2v3.5H13" stroke="{c}" stroke-width="1.2"/><line x1="6" y1="8" x2="11" y2="8" stroke="{c}" stroke-width="1"/><line x1="6" y1="10.5" x2="10" y2="10.5" stroke="{c}" stroke-width="1"/></svg>',
    ]

    knowledge_icons = [
        # Graph/network
        '<svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="3" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="3" cy="12" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="13" cy="12" r="2" stroke="{c}" stroke-width="1.2"/><line x1="7" y1="5" x2="4" y2="10" stroke="{c}" stroke-width="1"/><line x1="9" y1="5" x2="12" y2="10" stroke="{c}" stroke-width="1"/><line x1="5" y1="12" x2="11" y2="12" stroke="{c}" stroke-width="1"/></svg>',
        # Puzzle piece
        '<svg viewBox="0 0 16 16" fill="none"><path d="M3 5h2.5c0-1.2.9-2 2-2s2 .8 2 2H12v3h-1c-1 0-1.8.9-1.8 2s.8 2 1.8 2h1v2H3V5z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/></svg>',
        # Refresh/lifecycle
        '<svg viewBox="0 0 16 16" fill="none"><path d="M2.5 8a5.5 5.5 0 019.2-4" stroke="{c}" stroke-width="1.2" stroke-linecap="round"/><path d="M13.5 8a5.5 5.5 0 01-9.2 4" stroke="{c}" stroke-width="1.2" stroke-linecap="round"/><path d="M11 2l1 2.5-2.5.5" stroke="{c}" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/><path d="M5 14l-1-2.5 2.5-.5" stroke="{c}" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/></svg>',
        # Key
        '<svg viewBox="0 0 16 16" fill="none"><circle cx="5.5" cy="10" r="3" stroke="{c}" stroke-width="1.2"/><line x1="7.5" y1="8" x2="14" y2="2" stroke="{c}" stroke-width="1.2" stroke-linecap="round"/><line x1="12" y1="2" x2="12" y2="5" stroke="{c}" stroke-width="1.2" stroke-linecap="round"/><line x1="14" y1="2" x2="14" y2="4.5" stroke="{c}" stroke-width="1.2" stroke-linecap="round"/></svg>',
        # State/memory
        '<svg viewBox="0 0 16 16" fill="none"><rect x="2" y="3" width="12" height="10" rx="2" stroke="{c}" stroke-width="1.2"/><line x1="2" y1="7" x2="14" y2="7" stroke="{c}" stroke-width="1"/><circle cx="5" cy="5" r="0.8" fill="{c}"/><circle cx="7.5" cy="5" r="0.8" fill="{c}"/></svg>',
        # Sun-moon (theme)
        '<svg viewBox="0 0 16 16" fill="none"><circle cx="8" cy="8" r="3.5" stroke="{c}" stroke-width="1.2"/><path d="M8 2v1.5M8 12.5V14M2 8h1.5M12.5 8H14M3.8 3.8l1 1M11.2 11.2l1 1M12.2 3.8l-1 1M4.8 11.2l-1 1" stroke="{c}" stroke-width="1" stroke-linecap="round"/></svg>',
    ]

    code_gen_items = [
        "<b>1,328-line</b> network component with embedded JS/CSS",
        "<b>257 client-to-brand</b> mappings with SVG icons",
        "<b>679-line</b> charts module (bar, heatmap, treemap, Sankey)",
        "<b>339-line</b> SQL setup &amp; deploy scripts",
        "<b>421 lines</b> of unit tests across 5 modules",
        "<b>CHANGELOG.md</b> and structured documentation",
    ]

    knowledge_items = [
        "vis-network.js API and force-directed layout tuning",
        "UMD module patterns inside ES module contexts",
        "Streamlit Components v2 lifecycle and re-render behavior",
        "Edge deduplication with canonical key algorithms",
        "Snowflake session state vs cache interactions",
        "Dark/light theme detection heuristics",
    ]

    def _render_items(items, icons, color):
        html = ""
        for idx, (item, icon_svg) in enumerate(zip(items, icons)):
            svg = icon_svg.replace("{c}", color)
            delay = 0.15 + idx * 0.08
            html += f'''
            <div class="item" style="animation-delay: {delay}s;">
                <div class="item-icon">{svg}</div>
                <div class="item-text">{item}</div>
            </div>'''
        return html

    blue_items = _render_items(code_gen_items, code_gen_icons, SNOWFLAKE_BLUE)
    blue_items2 = _render_items(knowledge_items, knowledge_icons, SNOWFLAKE_BLUE)

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
  {_base_css(bg_color, f"color: {text_color}; padding: 0;")}

  .cards-grid {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    padding: 4px;
  }}

  .card {{
    position: relative;
    border-radius: 14px;
    padding: 1.4rem 1.5rem;
    transition: transform 0.25s ease, box-shadow 0.25s ease;
    opacity: 0;
    animation: fadeInCard 0.5s ease forwards;
  }}
  .card:hover {{
    transform: translateY(-3px);
  }}

  .card-blue {{
    background: {blue_bg};
    border: 1px solid {blue_border};
    box-shadow: 0 2px 16px {blue_glow}, inset 0 1px 0 rgba({br},{bg_},{bb}, 0.1);
    animation-delay: 0.05s;
  }}
  .card-blue:hover {{
    box-shadow: 0 8px 32px {blue_glow}, 0 0 0 1px {blue_border};
  }}

  /* Gradient top accent bar */
  .card::before {{
    content: '';
    position: absolute;
    top: 0;
    left: 16px;
    right: 16px;
    height: 3px;
    border-radius: 0 0 3px 3px;
  }}
  .card-blue::before {{
    background: linear-gradient(90deg, {SNOWFLAKE_BLUE}, {STAR_BLUE});
  }}

  .card-title {{
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 1rem;
    font-size: 15px;
    font-weight: 800;
    letter-spacing: 0.03em;
    text-transform: uppercase;
  }}
  .card-blue .card-title {{
    color: {SNOWFLAKE_BLUE};
  }}

  .title-icon {{
    width: 28px;
    height: 28px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 16px;
    flex-shrink: 0;
  }}
  .card-blue .title-icon {{
    background: rgba({br},{bg_},{bb}, 0.15);
  }}

  .item {{
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 6px 0;
    opacity: 0;
    animation: fadeInItem 0.35s ease forwards;
  }}

  .item-icon {{
    width: 18px;
    height: 18px;
    flex-shrink: 0;
    margin-top: 1px;
  }}
  .item-icon svg {{
    width: 100%;
    height: 100%;
  }}

  .item-text {{
    font-size: 13.5px;
    line-height: 1.45;
    color: {text_color};
  }}
  .item-text b {{
    font-weight: 700;
  }}

  @keyframes fadeInCard {{
    0% {{ opacity: 0; transform: translateY(12px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
  }}
  @keyframes fadeInItem {{
    0% {{ opacity: 0; transform: translateX(-8px); }}
    100% {{ opacity: 1; transform: translateX(0); }}
  }}
</style>
</head>
<body>
  <div class="cards-grid">
    <div class="card card-blue">
      <div class="card-title">
        <div class="title-icon"><svg viewBox="0 0 16 16" fill="none" width="16" height="16"><path d="M5 4L1.5 8 5 12" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><path d="M11 4l3.5 4L11 12" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><line x1="9.5" y1="2.5" x2="6.5" y2="13.5" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.2" stroke-linecap="round"/></svg></div>
        Code Generation
      </div>
      {blue_items}
    </div>
    <div class="card card-blue">
      <div class="card-title">
        <div class="title-icon"><svg viewBox="0 0 16 16" fill="none" width="16" height="16"><path d="M8 1.5v2" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.3" stroke-linecap="round"/><path d="M8 5.5a2.5 2.5 0 00-2.5 2.5c0 1 .6 1.8 1.5 2.2V12h2v-1.8c.9-.4 1.5-1.2 1.5-2.2A2.5 2.5 0 008 5.5z" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.2" stroke-linejoin="round"/><line x1="6.5" y1="13.5" x2="9.5" y2="13.5" stroke="{SNOWFLAKE_BLUE}" stroke-width="1.2" stroke-linecap="round"/><path d="M3.5 4l1 1M12.5 4l-1 1M2 8h1.5M12.5 8H14" stroke="{SNOWFLAKE_BLUE}" stroke-width="1" stroke-linecap="round"/></svg></div>
        Knowledge &amp; Research
      </div>
      {blue_items2}
    </div>
  </div>
</body>
</html>"""


@st.cache_data
def _build_stats_grid_html(is_dark: bool) -> str:
    """Build a pure CSS/HTML stats grid for the Project at a Glance section."""
    bg_color, text_color, _, _ = _theme_palette(is_dark)
    label_color = "#999" if is_dark else "#777"

    br, bg_, bb = _RGB_SNOWFLAKE
    sr, sg, sb = _RGB_STAR
    accent = STAR_BLUE if is_dark else SNOWFLAKE_BLUE
    ar, ag, ab = _RGB_STAR if is_dark else _RGB_SNOWFLAKE
    card_bg = f"rgba({ar},{ag},{ab}, 0.04)" if is_dark else f"rgba({ar},{ag},{ab}, 0.03)"
    card_border = f"rgba({ar},{ag},{ab}, 0.18)"
    card_glow = f"rgba({ar},{ag},{ab}, 0.08)"
    icon_bg = f"rgba({ar},{ag},{ab}, 0.12)"

    # (label, value, svg_icon_template)
    stats = [
        ("Python Files", "15",
         '<svg viewBox="0 0 16 16" fill="none"><path d="M4 2h5.5L13 5.5V14H4V2z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><path d="M9.5 2v3.5H13" stroke="{c}" stroke-width="1.2"/><path d="M7 8l-1.5 2L7 12" stroke="{c}" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/><path d="M10 8l1.5 2L10 12" stroke="{c}" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/></svg>'),
        ("SVG Brand Icons", "170",
         '<svg viewBox="0 0 16 16" fill="none"><rect x="2" y="2" width="12" height="12" rx="2" stroke="{c}" stroke-width="1.2"/><circle cx="6" cy="6.5" r="1.5" stroke="{c}" stroke-width="1"/><path d="M2 11l3.5-3 2.5 2 2.5-3L14 11" stroke="{c}" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round"/></svg>'),
        ("Test Functions", "16",
         '<svg viewBox="0 0 16 16" fill="none"><path d="M8 1.5L2.5 4v4c0 3.5 2.3 5.8 5.5 6.5 3.2-.7 5.5-3 5.5-6.5V4L8 1.5z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><path d="M5.5 8l2 2 3.5-3.5" stroke="{c}" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>'),
        ("Git Commits", "107",
         '<svg viewBox="0 0 16 16" fill="none"><circle cx="5" cy="4" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="11" cy="4" r="2" stroke="{c}" stroke-width="1.2"/><circle cx="8" cy="12" r="2" stroke="{c}" stroke-width="1.2"/><path d="M5 6v2c0 1.1.9 2 2 2h1M11 6v2c0 1.1-.9 2-2 2h-1" stroke="{c}" stroke-width="1.1"/></svg>'),
        ("Lines of Code", "5,096",
         '<svg viewBox="0 0 16 16" fill="none"><path d="M5 3L1 8l4 5" stroke="{c}" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><path d="M11 3l4 5-4 5" stroke="{c}" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><line x1="9.5" y1="2" x2="6.5" y2="14" stroke="{c}" stroke-width="1.1" stroke-linecap="round"/></svg>'),
        ("Lessons Learned", "483 lines",
         '<svg viewBox="0 0 16 16" fill="none"><path d="M3 2h10v12H3z" stroke="{c}" stroke-width="1.2" stroke-linejoin="round"/><path d="M8 2v12" stroke="{c}" stroke-width="0.8"/><path d="M3 2c0 0 2.5 1.5 5 0s5 0 5 0" stroke="{c}" stroke-width="1.1"/></svg>'),
        ("App Pages", "5",
         '<svg viewBox="0 0 16 16" fill="none"><rect x="2" y="2" width="12" height="12" rx="2" stroke="{c}" stroke-width="1.2"/><line x1="2" y1="6" x2="14" y2="6" stroke="{c}" stroke-width="1"/><line x1="6" y1="6" x2="6" y2="14" stroke="{c}" stroke-width="1"/></svg>'),
        ("Components Built", "12+",
         '<svg viewBox="0 0 16 16" fill="none"><rect x="1.5" y="1.5" width="5" height="5" rx="1" stroke="{c}" stroke-width="1.1"/><rect x="9.5" y="1.5" width="5" height="5" rx="1" stroke="{c}" stroke-width="1.1"/><rect x="1.5" y="9.5" width="5" height="5" rx="1" stroke="{c}" stroke-width="1.1"/><rect x="9.5" y="9.5" width="5" height="5" rx="1" stroke="{c}" stroke-width="1.1"/></svg>'),
    ]

    cards_html = ""
    for idx, (label, value, icon_svg) in enumerate(stats):
        svg = icon_svg.replace("{c}", accent)
        delay = 0.05 + idx * 0.06
        cards_html += f'''
        <div class="stat-card" style="animation-delay: {delay}s;">
            <div class="stat-icon">{svg}</div>
            <div class="stat-value">{value}</div>
            <div class="stat-label">{label}</div>
        </div>'''

    return f"""<!DOCTYPE html>
<html>
<head>
<style>
  {_base_css(bg_color, f"color: {text_color}; padding: 0;")}

  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    padding: 4px;
  }}

  .stat-card {{
    position: relative;
    border-radius: 12px;
    padding: 1rem 1rem 0.85rem;
    text-align: center;
    background: {card_bg};
    border: 1px solid {card_border};
    box-shadow: 0 2px 12px {card_glow};
    transition: transform 0.25s ease, box-shadow 0.25s ease;
    opacity: 0;
    animation: fadeInStat 0.45s ease forwards;
  }}
  .stat-card:hover {{
    transform: translateY(-2px);
    box-shadow: 0 6px 24px {card_glow}, 0 0 0 1px {card_border};
  }}

  .stat-icon {{
    width: 28px;
    height: 28px;
    margin: 0 auto 0.5rem;
    border-radius: 8px;
    background: {icon_bg};
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 5px;
  }}
  .stat-icon svg {{
    width: 100%;
    height: 100%;
  }}

  .stat-value {{
    font-size: 24px;
    font-weight: 900;
    color: {accent};
    line-height: 1.15;
    margin-bottom: 0.2rem;
    letter-spacing: -0.02em;
  }}

  .stat-label {{
    font-size: 11px;
    font-weight: 600;
    color: {label_color};
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }}

  @keyframes fadeInStat {{
    0% {{ opacity: 0; transform: translateY(10px) scale(0.97); }}
    100% {{ opacity: 1; transform: translateY(0) scale(1); }}
  }}
</style>
</head>
<body>
  <div class="stats-grid">
    {cards_html}
  </div>
</body>
</html>"""


@st.cache_data
def _build_cta_html(is_dark: bool) -> str:
    """Build the Call to Action section with animated button and floating orbs."""
    bg_color, _, muted_color, _ = _theme_palette(is_dark)
    br, bg_, bb = _RGB_SNOWFLAKE
    sr, sg, sb = _RGB_STAR
    heading_color = "#F0F0F0" if is_dark else "#222"

    return f"""<!DOCTYPE html>
<html><head><style>
  {_base_css(bg_color)}
  .cta {{
    position: relative;
    text-align: center;
    padding: 2.2rem 2rem 2rem;
    border-radius: 16px;
    background: linear-gradient(135deg,
      rgba({br},{bg_},{bb}, 0.12) 0%,
      rgba({sr},{sg},{sb}, 0.06) 50%,
      rgba({br},{bg_},{bb}, 0.10) 100%);
    border: 1px solid rgba({br},{bg_},{bb}, 0.3);
    overflow: hidden;
    opacity: 0;
    animation: fadeUp 0.6s ease 0.1s forwards;
  }}
  /* Animated gradient sweep */
  .cta::before {{
    content: '';
    position: absolute;
    top: 0; left: -100%; width: 300%; height: 100%;
    background: linear-gradient(90deg,
      transparent 0%,
      rgba({br},{bg_},{bb}, 0.06) 25%,
      rgba({sr},{sg},{sb}, 0.10) 50%,
      rgba({br},{bg_},{bb}, 0.06) 75%,
      transparent 100%);
    animation: shimmerCTA 6s ease-in-out infinite;
    pointer-events: none;
  }}
  /* Floating orbs */
  .orb {{
    position: absolute; border-radius: 50%;
    opacity: 0.12; pointer-events: none;
    animation: float 8s ease-in-out infinite;
  }}
  .orb-1 {{ width: 120px; height: 120px; top: -30px; left: 8%;
    background: radial-gradient(circle, {SNOWFLAKE_BLUE}, transparent 70%);
    animation-delay: 0s; }}
  .orb-2 {{ width: 80px; height: 80px; bottom: -20px; right: 12%;
    background: radial-gradient(circle, {STAR_BLUE}, transparent 70%);
    animation-delay: -3s; }}
  .orb-3 {{ width: 60px; height: 60px; top: 10px; right: 25%;
    background: radial-gradient(circle, {AMBER}, transparent 70%);
    animation-delay: -5s; opacity: 0.08; }}
  .cta-content {{ position: relative; z-index: 1; }}
  .cta-speedup {{
    display: inline-block;
    font-size: 42px; font-weight: 900;
    background: linear-gradient(135deg, {SNOWFLAKE_BLUE}, {STAR_BLUE});
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text;
    line-height: 1; margin-bottom: 4px;
    animation: fadeUp 0.5s ease 0.3s both;
  }}
  .cta-speedup-label {{
    font-size: 13px; font-weight: 600;
    color: rgba({br},{bg_},{bb}, 0.7);
    text-transform: uppercase; letter-spacing: 0.08em;
    margin-bottom: 10px;
    animation: fadeUp 0.5s ease 0.4s both;
  }}
  .cta-heading {{
    font-size: 20px; font-weight: 700;
    color: {heading_color};
    margin-bottom: 6px;
    animation: fadeUp 0.5s ease 0.5s both;
  }}
  .cta-sub {{
    font-size: 13.5px;
    color: {muted_color};
    margin-bottom: 18px;
    animation: fadeUp 0.5s ease 0.55s both;
  }}
  .cta-btn {{
    display: inline-flex; align-items: center; gap: 8px;
    padding: 12px 32px;
    background: linear-gradient(135deg, {MID_BLUE}, {SNOWFLAKE_BLUE});
    color: #fff; font-size: 15px; font-weight: 700;
    border-radius: 10px; text-decoration: none;
    transition: transform 0.2s ease, box-shadow 0.2s ease;
    box-shadow: 0 4px 20px rgba({br},{bg_},{bb}, 0.35);
    animation: fadeUp 0.5s ease 0.65s both;
    position: relative; overflow: hidden;
  }}
  .cta-btn::after {{
    content: '';
    position: absolute; top: 0; left: -100%; width: 100%; height: 100%;
    background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
    animation: btnShine 3s ease-in-out 1.5s infinite;
  }}
  .cta-btn:hover {{
    transform: translateY(-2px) scale(1.02);
    box-shadow: 0 8px 32px rgba({br},{bg_},{bb}, 0.5);
  }}
  .cta-btn svg {{ flex-shrink: 0; }}
  /* Pulse ring behind button */
  .btn-wrap {{ position: relative; display: inline-block;
    animation: fadeUp 0.5s ease 0.65s both; }}
  .btn-wrap::before {{
    content: '';
    position: absolute;
    top: 50%; left: 50%;
    width: 110%; height: 140%;
    transform: translate(-50%, -50%);
    border-radius: 14px;
    background: rgba({br},{bg_},{bb}, 0.15);
    animation: pulse 2.5s ease-in-out 1s infinite;
    pointer-events: none;
  }}
  @keyframes fadeUp {{
    0% {{ opacity: 0; transform: translateY(12px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
  }}
  @keyframes shimmerCTA {{
    0%, 100% {{ transform: translateX(0); }}
    50% {{ transform: translateX(33%); }}
  }}
  @keyframes float {{
    0%, 100% {{ transform: translateY(0) scale(1); }}
    50% {{ transform: translateY(-12px) scale(1.05); }}
  }}
  @keyframes pulse {{
    0%, 100% {{ opacity: 0; transform: translate(-50%, -50%) scale(0.95); }}
    50% {{ opacity: 1; transform: translate(-50%, -50%) scale(1); }}
  }}
  @keyframes btnShine {{
    0% {{ left: -100%; }}
    20% {{ left: 100%; }}
    100% {{ left: 100%; }}
  }}
</style></head>
<body>
  <div class="cta">
    <div class="orb orb-1"></div>
    <div class="orb orb-2"></div>
    <div class="orb orb-3"></div>
    <div class="cta-content">
      <div class="cta-speedup">{SPEEDUP}x</div>
      <div class="cta-speedup-label">Idea to Production</div>
      <div class="cta-heading">Go from idea to production on governed data, faster.</div>
      <div class="cta-sub">The AI coding agent built for your Snowflake environment &mdash; not just your repo.</div>
      <div class="btn-wrap">
        <a class="cta-btn" href="https://www.snowflake.com/en/product/features/cortex-code/" target="_blank" rel="noopener">
          Get Started with Cortex Code
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M3 8h10M9 4l4 4-4 4" stroke="#fff" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </a>
      </div>
    </div>
  </div>
</body></html>"""


def run():
    """Render the About page with Cortex Code productivity comparison."""

    is_dark = is_dark_theme()

    # ── Hero Section + Headline Metrics ─────────────────────────
    components.html(_build_hero_html(is_dark), height=210)

    st.markdown("")

    # ── Time Comparison Chart ─────────────────────────────────────
    components.html(_section_title_html("Time Comparison by Component", _ICON_COMPARE, is_dark), height=36)

    # Pure CSS animated bar chart — no external JS dependencies
    components.html(_build_css_chart_html(is_dark), height=380)

    st.markdown("")

    # ── What Cortex Code Accelerated ──────────────────────────────
    components.html(_section_title_html("What Cortex Code Accelerated", _ICON_SPARKLE, is_dark), height=36)

    st.markdown("")

    # Pure CSS/HTML capability cards — full styling control in iframe
    components.html(_build_capability_cards_html(is_dark), height=300)

    st.markdown("")

    # ── Project at a Glance ───────────────────────────────────────
    components.html(_section_title_html("Project at a Glance", _ICON_DASHBOARD, is_dark), height=36)

    st.markdown("")

    # Pure CSS/HTML stats grid — matches capability cards styling
    components.html(_build_stats_grid_html(is_dark), height=270)

    st.markdown("")

    # ── Call to Action ────────────────────────────────────────────
    components.html(_build_cta_html(is_dark), height=250)

    st.markdown("")

    # ── Footer ────────────────────────────────────────────────────
    st.caption(
        "Time estimates are approximate and based on comparable projects. "
        "Learn more about [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code)."
    )
