"""PyVis network graph construction with theme-aware CSS/JS injection."""

import math
from collections import defaultdict
from typing import Dict

import pandas as pd
import streamlit as st
from pyvis.network import Network

from components.client_mappings import generate_client_icon_uri
from components.data import get_current_account
from components.theme import AMBER, SNOWFLAKE_BLUE, get_theme_colors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _aggregate_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate duplicate edges by summing ACCESS_COUNT."""
    group_cols = ["CLIENT", "WAREHOUSE", "DATABASE", "DIRECTION"]
    # Keep first ORGANIZATION_NAME / ACCOUNT_NAME per group
    agg = (
        df.groupby(group_cols, as_index=False)
        .agg(ACCESS_COUNT=("ACCESS_COUNT", "sum"),
             ORGANIZATION_NAME=("ORGANIZATION_NAME", "first"),
             ACCOUNT_NAME=("ACCOUNT_NAME", "first"))
    )
    return agg.sort_values("ACCESS_COUNT", ascending=False)


def _compute_node_stats(df: pd.DataFrame) -> Dict[str, dict]:
    """Compute per-node stats: total access, read/write breakdown, top connections."""
    stats: Dict[str, dict] = defaultdict(lambda: {
        "total": 0, "read": 0, "write": 0, "connections": defaultdict(int),
    })

    for _, row in df.iterrows():
        ac = int(row["ACCESS_COUNT"])
        direction = row["DIRECTION"]
        rw = "write" if direction in ("write", "DML", "DDL") else "read"

        for node in (row["DATABASE"], row["WAREHOUSE"], row["CLIENT"]):
            s = stats[node]
            s["total"] += ac
            s[rw] += ac

        # Track connections: database ↔ client (through warehouse)
        stats[row["DATABASE"]]["connections"][row["CLIENT"]] += ac
        stats[row["CLIENT"]]["connections"][row["DATABASE"]] += ac
        stats[row["WAREHOUSE"]]["connections"][row["CLIENT"]] += ac
        stats[row["WAREHOUSE"]]["connections"][row["DATABASE"]] += ac

    return dict(stats)


def _log_scale(value: float, min_val: float, max_val: float,
               out_min: float, out_max: float) -> float:
    """Map value to out_min..out_max on a log scale."""
    if max_val <= min_val or value <= 0:
        return out_min
    log_min = math.log1p(min_val)
    log_max = math.log1p(max_val)
    log_val = math.log1p(value)
    t = (log_val - log_min) / (log_max - log_min)
    return out_min + t * (out_max - out_min)


def _build_tooltip(node_name: str, node_type: str, stats: dict,
                   org_name: str, account: str) -> str:
    """Build rich HTML tooltip for a node."""
    total = stats.get("total", 0)
    reads = stats.get("read", 0)
    writes = stats.get("write", 0)
    conns = stats.get("connections", {})

    # Top 3 connections by volume
    top = sorted(conns.items(), key=lambda x: -x[1])[:3]
    top_lines = "".join(
        f"  {name}: {count:,}\n" for name, count in top
    )

    return (
        f"{node_type}: {node_name}\n"
        f"Organization: {org_name}\n"
        f"Account: {account}\n"
        f"───────────────────\n"
        f"Total Access: {total:,}\n"
        f"  Read:  {reads:,}\n"
        f"  Write: {writes:,}\n"
        f"───────────────────\n"
        f"Top Connections:\n"
        f"{top_lines}"
    ).rstrip()


# ---------------------------------------------------------------------------
# Cluster definitions — group databases by naming convention
# ---------------------------------------------------------------------------

_CLUSTER_RULES = [
    ("Raw Layer",   lambda name: name.startswith("RAW_")),
    ("Clean Layer", lambda name: name.startswith("CLEANED_") or name == "INTEGRATED_DB"),
    ("Gold Layer",  lambda name: name.endswith("_MART_DB")),
]


def _assign_cluster(db_name: str) -> str:
    """Return cluster label for a database, or empty string if unclustered."""
    for label, predicate in _CLUSTER_RULES:
        if predicate(db_name):
            return label
    return ""


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_network_html(df: pd.DataFrame, _node_images: Dict[str, str],
                       session, fullscreen: bool = False) -> str:
    """Build network visualization HTML."""
    if df.empty:
        return "<p style='color: inherit;'>No rows available to render.</p>"

    current_account = get_current_account(session)

    # Detect theme server-side to set correct initial colors (avoids flash on load)
    is_dark, text_color = get_theme_colors()
    try:
        theme_bg = st.get_option("theme.backgroundColor")
        bg_color = (theme_bg if isinstance(theme_bg, str) and theme_bg.startswith("#")
                    else ("#0e1117" if is_dark else "#ffffff"))
    except Exception:
        bg_color = "#0e1117" if is_dark else "#ffffff"
    font_color = text_color

    # ── Step 1: Aggregate edges ──────────────────────────────────────────
    agg_df = _aggregate_edges(df)
    node_stats = _compute_node_stats(agg_df)

    # ── Compute size bounds ──────────────────────────────────────────────
    all_totals = [s["total"] for s in node_stats.values()] or [1]
    global_min, global_max = min(all_totals), max(all_totals)

    # ── Build the network ────────────────────────────────────────────────
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
              "size": 0
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
            "stabilization": {{
              "enabled": true,
              "iterations": 300
            }},
            "adaptiveTimestep": true,
            "minVelocity": 0.75
          }}
        }}
        """
    net.set_options(options)

    # Shared node styling
    transparent = {
        "background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)",
        "highlight": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"},
    }
    shape_props = {"useBorderWithImage": True, "borderType": "circle"}
    added_nodes: set = set()

    # Track which databases belong to which clusters
    cluster_members: Dict[str, list] = defaultdict(list)

    for _, row in agg_df.iterrows():
        database = row["DATABASE"]
        warehouse = row["WAREHOUSE"]
        ac = int(row["ACCESS_COUNT"])
        direction = row["DIRECTION"]
        org_name = row["ORGANIZATION_NAME"]
        client = row["CLIENT"]

        # ── Add nodes (deduplicated) ─────────────────────────────────────
        if database not in added_nodes:
            s = node_stats.get(database, {})
            db_size = _log_scale(s.get("total", 0), global_min, global_max, 120, 300)
            tooltip = _build_tooltip(database, "Database", s, org_name, current_account)

            cluster_label = _assign_cluster(database)
            if cluster_label:
                cluster_members[cluster_label].append(database)

            net.add_node(
                database, label=database, title=tooltip,
                size=int(db_size), color=transparent, shape="image",
                shapeProperties=shape_props, borderWidth=0,
                image=_node_images["database"],
            )
            added_nodes.add(database)

        if warehouse not in added_nodes:
            s = node_stats.get(warehouse, {})
            wh_size = _log_scale(s.get("total", 0), global_min, global_max, 120, 300)
            tooltip = _build_tooltip(warehouse, "Warehouse", s, org_name, current_account)
            net.add_node(
                warehouse, label=warehouse, title=tooltip,
                size=int(wh_size), color=transparent, shape="image",
                shapeProperties=shape_props, borderWidth=0,
                image=_node_images["warehouse"],
            )
            added_nodes.add(warehouse)

        if client not in added_nodes:
            s = node_stats.get(client, {})
            cl_size = _log_scale(s.get("total", 0), global_min, global_max, 80, 200)
            tooltip = _build_tooltip(client, "Client", s, org_name, current_account)
            client_icon = generate_client_icon_uri(client)
            net.add_node(
                client, label=client, title=tooltip,
                size=int(cl_size), color=transparent, shape="image",
                shapeProperties=shape_props, borderWidth=0,
                image=client_icon,
            )
            added_nodes.add(client)

        # ── Add edges (already aggregated, no duplicates) ────────────────
        edge_color = AMBER if direction in ("write", "DML", "DDL") else SNOWFLAKE_BLUE
        edge_title = f"Access Count: {ac:,}"

        if direction in ("write", "DML", "DDL"):
            net.add_edge(client, warehouse, value=ac, color=edge_color,
                         arrowStrikethrough=False, title=edge_title)
            net.add_edge(warehouse, database, value=ac, color=edge_color,
                         arrowStrikethrough=False, title=edge_title)
        else:
            net.add_edge(database, warehouse, value=ac, color=edge_color,
                         arrowStrikethrough=False, title=edge_title)
            net.add_edge(warehouse, client, value=ac, color=edge_color,
                         arrowStrikethrough=False, title=edge_title)

    html = net.generate_html(notebook=False)

    # ── Build cluster mapping for JS ─────────────────────────────────────
    import json
    cluster_map_json = json.dumps(
        {label: members for label, members in cluster_members.items() if members}
    )

    # ── CSS to inject into <head> ────────────────────────────────────────
    head_css = """
    <link href="https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap" rel="stylesheet">
    <style>
      :root {
        --pyvis-bg: #ffffff;
        --pyvis-fg: #14171a;
      }
      @media (prefers-color-scheme: dark) {
        :root {
          --pyvis-bg: #0e1117;
          --pyvis-fg: #fafafa;
        }
      }
    </style>
    <script>
    (function() {
      try {
        var pdoc = window.parent.document;
        var app = pdoc.querySelector('.stApp');
        if (app) {
          var bg = window.parent.getComputedStyle(app).backgroundColor;
          var fg = window.parent.getComputedStyle(app).color;
          if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)')
            document.documentElement.style.setProperty('--pyvis-bg', bg);
          if (fg && fg !== 'transparent' && fg !== 'rgba(0, 0, 0, 0)')
            document.documentElement.style.setProperty('--pyvis-fg', fg);
        }
      } catch(e) {}
    })();
    </script>
    <style>
      html, body {
        margin: 0; padding: 0;
        background-color: var(--pyvis-bg) !important;
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
      }
      #mynetwork {
        border: 0 !important;
        background-color: var(--pyvis-bg) !important;
      }
      #loadingBar {
        background: transparent !important;
        color: var(--pyvis-fg) !important;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
      }
      #loadingBar .outerBorder {
        background: transparent !important;
        border: 2px solid rgba(128,128,128,0.5) !important;
        box-shadow: none !important;
      }
      #loadingBar .border {
        background: transparent !important;
        border: 1px solid rgba(128,128,128,0.5) !important;
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
      div.vis-tooltip {
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
        font-size: 12px !important;
        background-color: var(--pyvis-bg) !important;
        color: var(--pyvis-fg) !important;
        border: 1px solid rgba(128,128,128,0.3) !important;
        border-radius: 4px !important;
        padding: 8px 10px !important;
        box-shadow: 0 2px 8px rgba(0,0,0,0.15) !important;
        line-height: 1.5 !important;
        white-space: pre-line !important;
        max-width: 300px !important;
      }
      #graph-legend {
        position: absolute;
        bottom: 16px;
        left: 16px;
        background-color: var(--pyvis-bg);
        color: var(--pyvis-fg);
        border: 1px solid rgba(128,128,128,0.3);
        border-radius: 6px;
        padding: 10px 14px;
        font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 12px;
        line-height: 1.6;
        z-index: 100;
        opacity: 0.92;
      }
      #graph-legend .legend-title {
        font-weight: 700;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px;
        opacity: 0.7;
      }
      #graph-legend .legend-section {
        margin-top: 6px;
      }
      #graph-legend .legend-item {
        display: flex;
        align-items: center;
        gap: 8px;
      }
      #graph-legend .legend-line {
        width: 24px;
        height: 3px;
        border-radius: 2px;
      }
      #graph-legend .legend-btn {
        margin-top: 6px;
        padding: 3px 8px;
        font-size: 10px;
        font-family: 'Lato', sans-serif;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        cursor: pointer;
        border: 1px solid rgba(128,128,128,0.4);
        border-radius: 4px;
        background: transparent;
        color: var(--pyvis-fg);
        opacity: 0.8;
      }
      #graph-legend .legend-btn:hover {
        opacity: 1;
        background: rgba(128,128,128,0.15);
      }
    </style>
    """

    # ── Legend HTML ───────────────────────────────────────────────────────
    legend_html = f"""
    <div id="graph-legend">
      <div class="legend-title">Edge Direction</div>
      <div class="legend-item">
        <span class="legend-line" style="background-color: {SNOWFLAKE_BLUE};"></span>
        <span>Read</span>
      </div>
      <div class="legend-item">
        <span class="legend-line" style="background-color: {AMBER};"></span>
        <span>Write</span>
      </div>
      <div class="legend-section">
        <div class="legend-title">Interaction</div>
        <div style="font-size:11px; opacity:0.7;">Click node to highlight</div>
        <div style="font-size:11px; opacity:0.7;">Double-click to reset</div>
      </div>
      <button class="legend-btn" id="resetClustersBtn" style="display:none;"
              onclick="resetClusters()">Expand All</button>
      <button class="legend-btn" id="clusterBtn"
              onclick="applyClusters()">Cluster Databases</button>
    </div>
    """

    # ── JavaScript: theme observer + physics + clustering + click-to-filter
    theme_js = f"""
    <script>
    (function() {{
      var DARK_FG = '#fafafa';
      var LIGHT_FG = '#000000';
      var lastBg = null;
      var lastFg = null;
      var clusterMap = {cluster_map_json};
      var clustersActive = false;

      // ── Theme detection helpers ──────────────────────────────────────
      function getParentBackground() {{
        try {{
          var pdoc = window.parent.document;
          var app = pdoc.querySelector('.stApp');
          if (app) {{
            var bg = window.parent.getComputedStyle(app).backgroundColor;
            if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') return bg;
          }}
          var body = pdoc.body;
          if (body) {{
            var bg = window.parent.getComputedStyle(body).backgroundColor;
            if (bg && bg !== 'transparent' && bg !== 'rgba(0, 0, 0, 0)') return bg;
          }}
        }} catch (e) {{}}
        return 'rgb(255, 255, 255)';
      }}

      function getParentTextColor() {{
        try {{
          var pdoc = window.parent.document;
          var el = pdoc.querySelector('.network-title') || pdoc.querySelector('.stApp');
          if (el) {{
            var color = window.parent.getComputedStyle(el).color;
            if (color && color !== 'transparent' && color !== 'rgba(0, 0, 0, 0)') return color;
          }}
        }} catch (e) {{}}
        return null;
      }}

      function getLuminance(rgbStr) {{
        var m = rgbStr.match(/\\d+/g);
        if (m && m.length >= 3) {{
          return (0.299*parseInt(m[0]) + 0.587*parseInt(m[1]) + 0.114*parseInt(m[2])) / 255;
        }}
        return 1;
      }}

      function applyTheme() {{
        var bgColor = getParentBackground();
        var fgColor = getParentTextColor();
        if (!fgColor) {{
          fgColor = getLuminance(bgColor) < 0.5 ? DARK_FG : LIGHT_FG;
        }}
        if (bgColor === lastBg && fgColor === lastFg) return;
        lastBg = bgColor;
        lastFg = fgColor;

        document.documentElement.style.setProperty('--pyvis-bg', bgColor);
        document.documentElement.style.setProperty('--pyvis-fg', fgColor);
        document.body.style.backgroundColor = bgColor;
        document.body.style.color = fgColor;
        var container = document.getElementById('mynetwork');
        if (container) container.style.backgroundColor = bgColor;

        var loadingText = document.getElementById('text');
        var loadingPct = document.getElementById('percentage');
        if (loadingText) loadingText.style.color = fgColor;
        if (loadingPct) loadingPct.style.color = fgColor;

        if (typeof network !== 'undefined' && network) {{
          if (network.setOptions) {{
            network.setOptions({{
              nodes: {{ font: {{ color: fgColor }} }},
              edges: {{ font: {{ color: fgColor, strokeColor: fgColor }} }}
            }});
          }}
          try {{
            var nodes = network.body.data.nodes;
            if (nodes && nodes.getIds) {{
              var ids = nodes.getIds();
              var updates = [];
              for (var i = 0; i < ids.length; i++) {{
                updates.push({{ id: ids[i], font: {{ color: fgColor }} }});
              }}
              nodes.update(updates);
            }}
          }} catch (e) {{}}
          try {{
            var edges = network.body.data.edges;
            if (edges && edges.getIds) {{
              var ids = edges.getIds();
              var updates = [];
              for (var i = 0; i < ids.length; i++) {{
                updates.push({{ id: ids[i], font: {{ color: fgColor, strokeColor: fgColor }} }});
              }}
              edges.update(updates);
            }}
          }} catch (e) {{}}
          network.redraw();
        }}
      }}

      // ── MutationObserver instead of setInterval ──────────────────────
      applyTheme();
      window.addEventListener('load', function() {{
        applyTheme();
        try {{
          var target = window.parent.document.querySelector('.stApp');
          if (target) {{
            var observer = new MutationObserver(function() {{ applyTheme(); }});
            observer.observe(target, {{
              attributes: true,
              attributeFilter: ['class', 'style', 'data-theme']
            }});
          }}
        }} catch (e) {{}}
        // Fallback: also check once after a short delay for slow theme loads
        setTimeout(applyTheme, 500);
        setTimeout(applyTheme, 2000);
      }});

      // ── Disable physics after stabilization ──────────────────────────
      function setupPhysicsAndInteraction() {{
        if (typeof network === 'undefined' || !network) {{
          setTimeout(setupPhysicsAndInteraction, 100);
          return;
        }}

        network.once('stabilizationIterationsDone', function() {{
          network.setOptions({{ physics: {{ enabled: false }} }});
        }});

        // ── Click-to-highlight ───────────────────────────────────────
        network.on('click', function(params) {{
          if (params.nodes.length === 1) {{
            var nodeId = params.nodes[0];

            // If it's a cluster, open it
            if (network.isCluster(nodeId)) {{
              network.openCluster(nodeId);
              return;
            }}

            // Get connected nodes and edges
            var connectedNodes = network.getConnectedNodes(nodeId);
            var connectedEdges = network.getConnectedEdges(nodeId);
            var highlightNodes = [nodeId].concat(connectedNodes);

            // Dim all nodes not connected
            var allNodes = network.body.data.nodes;
            var allEdges = network.body.data.edges;
            var nodeUpdates = [];
            var edgeUpdates = [];

            var nodeIds = allNodes.getIds();
            for (var i = 0; i < nodeIds.length; i++) {{
              var nid = nodeIds[i];
              if (highlightNodes.indexOf(nid) === -1) {{
                nodeUpdates.push({{ id: nid, opacity: 0.15 }});
              }} else {{
                nodeUpdates.push({{ id: nid, opacity: 1.0 }});
              }}
            }}

            var edgeIds = allEdges.getIds();
            for (var j = 0; j < edgeIds.length; j++) {{
              var eid = edgeIds[j];
              if (connectedEdges.indexOf(eid) === -1) {{
                edgeUpdates.push({{ id: eid, hidden: true }});
              }} else {{
                edgeUpdates.push({{ id: eid, hidden: false }});
              }}
            }}

            allNodes.update(nodeUpdates);
            allEdges.update(edgeUpdates);
          }}
        }});

        // ── Double-click to reset ────────────────────────────────────
        network.on('doubleClick', function() {{
          var allNodes = network.body.data.nodes;
          var allEdges = network.body.data.edges;

          var nodeIds = allNodes.getIds();
          var nodeUpdates = [];
          for (var i = 0; i < nodeIds.length; i++) {{
            nodeUpdates.push({{ id: nodeIds[i], opacity: 1.0 }});
          }}
          allNodes.update(nodeUpdates);

          var edgeIds = allEdges.getIds();
          var edgeUpdates = [];
          for (var j = 0; j < edgeIds.length; j++) {{
            edgeUpdates.push({{ id: edgeIds[j], hidden: false }});
          }}
          allEdges.update(edgeUpdates);
        }});
      }}
      setupPhysicsAndInteraction();

      // ── Clustering ─────────────────────────────────────────────────
      window.applyClusters = function() {{
        if (typeof network === 'undefined' || !network) return;
        var labels = Object.keys(clusterMap);
        for (var i = 0; i < labels.length; i++) {{
          var label = labels[i];
          var members = clusterMap[label];
          (function(lbl, mems) {{
            network.cluster({{
              joinCondition: function(nodeOptions) {{
                return mems.indexOf(nodeOptions.id) !== -1;
              }},
              clusterNodeProperties: {{
                id: 'cluster:' + lbl,
                label: lbl + ' (' + mems.length + ')',
                shape: 'box',
                font: {{ size: 80, color: lastFg || '#fafafa', face: 'Lato, sans-serif', bold: true }},
                color: {{
                  background: 'rgba(41, 181, 232, 0.15)',
                  border: '#29B5E8',
                  highlight: {{ background: 'rgba(41, 181, 232, 0.3)', border: '#29B5E8' }}
                }},
                borderWidth: 2,
                margin: 20,
              }}
            }});
          }})(label, members);
        }}
        clustersActive = true;
        document.getElementById('clusterBtn').style.display = 'none';
        document.getElementById('resetClustersBtn').style.display = 'block';
        network.setOptions({{ physics: {{ enabled: true }} }});
        network.once('stabilizationIterationsDone', function() {{
          network.setOptions({{ physics: {{ enabled: false }} }});
        }});
        network.stabilize(150);
      }};

      window.resetClusters = function() {{
        if (typeof network === 'undefined' || !network) return;
        var labels = Object.keys(clusterMap);
        for (var i = 0; i < labels.length; i++) {{
          var clusterId = 'cluster:' + labels[i];
          if (network.isCluster(clusterId)) {{
            network.openCluster(clusterId);
          }}
        }}
        clustersActive = false;
        document.getElementById('resetClustersBtn').style.display = 'none';
        document.getElementById('clusterBtn').style.display = 'block';
        network.setOptions({{ physics: {{ enabled: true }} }});
        network.once('stabilizationIterationsDone', function() {{
          network.setOptions({{ physics: {{ enabled: false }} }});
        }});
        network.stabilize(150);
      }};
    }})();
    </script>
    """

    # ── Inject CSS and JS ────────────────────────────────────────────────
    if "</head>" in html:
        html = html.replace("</head>", head_css + "\n</head>")
    if "</body>" in html:
        html = html.replace("</body>", legend_html + theme_js + "\n</body>")
    else:
        html = html + head_css + theme_js

    return html
