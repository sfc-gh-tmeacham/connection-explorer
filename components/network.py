"""PyVis network graph construction with theme-aware CSS/JS injection."""

from typing import Dict

import pandas as pd
import streamlit as st
from pyvis.network import Network

from components.data import get_current_account
from components.theme import SNOWFLAKE_BLUE, get_theme_colors


def build_network_html(df: pd.DataFrame, _node_images: Dict[str, str], session, fullscreen: bool = False) -> str:
    """Build network visualization HTML."""
    if df.empty:
        return "<p style='color: inherit;'>No rows available to render.</p>"

    current_account = get_current_account(session)

    # Detect theme server-side to set correct initial colors (avoids flash on load)
    is_dark, text_color = get_theme_colors()
    try:
        theme_bg = st.get_option("theme.backgroundColor")
        bg_color = theme_bg if isinstance(theme_bg, str) and theme_bg.startswith("#") else ("#0e1117" if is_dark else "#ffffff")
    except Exception:
        bg_color = "#0e1117" if is_dark else "#ffffff"
    font_color = text_color

    # Use 100% height for fullscreen mode
    network_height = "100vh" if fullscreen else "680px"

    net = Network(
        height=network_height,
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

        if direction == "write" or direction == "DML":
            src = warehouse
            dst = database
            src_type = "Warehouse"
            dst_type = "Database"
            src_image = _node_images["warehouse"]
            dst_image = _node_images["database"]
        else: # read
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

    # CSS to inject into <head> - detects actual parent colors, falls back to media queries
    head_css = """
    <link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap" rel="stylesheet">
    <style>
      /* Default: light theme */
      :root {
        --pyvis-bg: #ffffff;
        --pyvis-fg: #14171a;
      }
      /* Dark theme via media query as fallback */
      @media (prefers-color-scheme: dark) {
        :root {
          --pyvis-bg: #0e1117;
          --pyvis-fg: #fafafa;
        }
      }
    </style>
    <script>
    // Immediately detect actual parent colors and override CSS variables
    (function() {
      try {
        var pdoc = window.parent.document;
        var app = pdoc.querySelector('.stApp');
        if (app) {
          var bg = window.parent.getComputedStyle(app).backgroundColor;
          var fg = window.parent.getComputedStyle(app).color;
          if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') {
            document.documentElement.style.setProperty('--pyvis-bg', bg);
          }
          if (fg && fg !== 'transparent' && fg !== 'rgba(0, 0, 0, 0)') {
            document.documentElement.style.setProperty('--pyvis-fg', fg);
          }
        }
      } catch(e) {}
    })();
    </script>
    <style>
      html, body { 
        margin: 0; 
        padding: 0; 
        background-color: var(--pyvis-bg) !important;
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
      }
      #mynetwork { 
        border: 0 !important; 
        background-color: var(--pyvis-bg) !important;
      }
      /* Style vis.js loading bar - transparent background, no border/shadow, Snowflake font */
      #loadingBar {
        background: transparent !important;
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
      }
      #loadingBar .outerBorder {
        background: transparent !important;
        border: 2px solid rgba(128, 128, 128, 0.5) !important;
        box-shadow: none !important;
      }
      #loadingBar .border {
        background: transparent !important;
        border: 1px solid rgba(128, 128, 128, 0.5) !important;
        box-shadow: none !important;
      }
      #loadingBar #text {
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-weight: 400 !important;
      }
      #loadingBar #percentage {
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-weight: 700 !important;
      }
      #loadingBar #bar {
        border: none !important;
        box-shadow: none !important;
      }
      /* Style vis.js tooltips - theme-aware */
      div.vis-tooltip {
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 12px !important;
        background-color: var(--pyvis-bg) !important;
        color: var(--pyvis-fg) !important;
        border: 1px solid rgba(128, 128, 128, 0.3) !important;
        border-radius: 4px !important;
        padding: 8px 10px !important;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15) !important;
        line-height: 1.5 !important;
        white-space: pre-line !important;
        max-width: 300px !important;
      }
    </style>
    """

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

        // Update CSS custom properties so all styled elements update
        document.documentElement.style.setProperty('--pyvis-bg', bgColor);
        document.documentElement.style.setProperty('--pyvis-fg', fgColor);
        
        // Also set inline styles for immediate effect
        document.body.style.backgroundColor = bgColor;
        document.body.style.color = fgColor;
        var container = document.getElementById('mynetwork');
        if (container) container.style.backgroundColor = bgColor;
        
        // Style loading bar text to match theme
        var loadingText = document.getElementById('text');
        var loadingPct = document.getElementById('percentage');
        if (loadingText) loadingText.style.color = fgColor;
        if (loadingPct) loadingPct.style.color = fgColor;

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
    """

    # Inject CSS into <head> so it applies before any rendering
    if "</head>" in html:
        html = html.replace("</head>", head_css + "\n</head>")
    
    # Inject JavaScript before closing body
    if "</body>" in html:
        html = html.replace("</body>", theme_js + "\n</body>")
    else:
        html = html + head_css + theme_js

    return html
