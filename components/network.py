"""Network graph component using Streamlit Components v2 + vis-network.js."""

import json
import math
import os
from collections import defaultdict
from functools import lru_cache
from typing import Dict

import pandas as pd
import streamlit as st

from components.client_mappings import generate_client_icon_uri
from components.data import get_current_account
from components.theme import AMBER, SNOWFLAKE_BLUE


# ---------------------------------------------------------------------------
# Helpers (unchanged from v1 implementation)
# ---------------------------------------------------------------------------

def _aggregate_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate duplicate edges by summing ACCESS_COUNT."""
    group_cols = ["CLIENT", "WAREHOUSE", "DATABASE", "DIRECTION"]
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
    """Build rich text tooltip for a node."""
    total = stats.get("total", 0)
    reads = stats.get("read", 0)
    writes = stats.get("write", 0)
    conns = stats.get("connections", {})

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
# Cluster definitions
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
# vis-network.js loader
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_vis_js() -> str:
    """Read vis-network.min.js from the installed pyvis package."""
    import pyvis
    vis_path = os.path.join(
        os.path.dirname(pyvis.__file__),
        "lib", "vis-9.1.2", "vis-network.min.js",
    )
    with open(vis_path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# CSS — uses native Streamlit CSS variables for theme support
# ---------------------------------------------------------------------------

_COMPONENT_CSS = f"""
@import url('https://fonts.googleapis.com/css2?family=Lato:wght@300;400;700;900&display=swap');

:host, .vis-network-container {{
    width: 100%;
    font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
}}

#vis-canvas {{
    width: 100%;
    height: 680px;
    border: none;
    position: relative;
}}

#vis-canvas.fullscreen {{
    height: 100vh;
}}

div.vis-tooltip {{
    font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif !important;
    font-size: 12px !important;
    background-color: var(--st-background-color, #0e1117) !important;
    color: var(--st-text-color, #fafafa) !important;
    border: 1px solid rgba(128,128,128,0.3) !important;
    border-radius: 4px !important;
    padding: 8px 10px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15) !important;
    line-height: 1.5 !important;
    white-space: pre-line !important;
    max-width: 300px !important;
}}

#graph-legend {{
    position: absolute;
    bottom: 16px;
    left: 16px;
    background-color: var(--st-background-color, #0e1117);
    color: var(--st-text-color, #fafafa);
    border: 1px solid rgba(128,128,128,0.3);
    border-radius: 6px;
    padding: 10px 14px;
    font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 12px;
    line-height: 1.6;
    z-index: 100;
    opacity: 0.92;
}}

#graph-legend .legend-title {{
    font-weight: 700;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 4px;
    opacity: 0.7;
}}

#graph-legend .legend-section {{
    margin-top: 6px;
}}

#graph-legend .legend-item {{
    display: flex;
    align-items: center;
    gap: 8px;
}}

#graph-legend .legend-line {{
    width: 24px;
    height: 3px;
    border-radius: 2px;
}}

#graph-legend .legend-btn {{
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
    color: var(--st-text-color, #fafafa);
    opacity: 0.8;
}}

#graph-legend .legend-btn:hover {{
    opacity: 1;
    background: rgba(128,128,128,0.15);
}}
"""


# ---------------------------------------------------------------------------
# HTML — container + legend
# ---------------------------------------------------------------------------

_COMPONENT_HTML = f"""
<div class="vis-network-container">
    <div id="vis-canvas"></div>
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
        <button class="legend-btn" id="resetClustersBtn" style="display:none;">Expand All</button>
        <button class="legend-btn" id="clusterBtn">Cluster Databases</button>
    </div>
</div>
"""


# ---------------------------------------------------------------------------
# JS module — vis.js bootstrap, network creation, interactions
# ---------------------------------------------------------------------------

def _build_js() -> str:
    """Build the complete JS module string: vis.js UMD + our component code."""
    vis_js_code = _load_vis_js()

    # We load vis.js by evaluating it in a way that sets window.vis.
    # The UMD wrapper in vis-network.min.js checks for `this` context.
    # We use Function() constructor to execute it with `this` = window.
    component_js = """
// --- Inject vis-network.js as a global (UMD) ---
if (!window.vis || !window.vis.Network) {
    const VIS_JS_CODE = VIS_JS_PLACEHOLDER;
    new Function(VIS_JS_CODE).call(window);
}

export default function(component) {
    const { data, setTriggerValue, setStateValue, parentElement } = component;
    if (!data || !data.nodes) return;

    const container = parentElement.querySelector('#vis-canvas');
    if (!container) return;

    // Apply fullscreen class if requested
    if (data.fullscreen) {
        container.classList.add('fullscreen');
    }

    // Clean up previous network instance if re-rendering
    if (container._visNetwork) {
        container._visNetwork.destroy();
        container._visNetwork = null;
    }

    // Get text color from Streamlit CSS variable
    const computedStyle = getComputedStyle(document.documentElement);
    const textColor = computedStyle.getPropertyValue('--st-text-color').trim() || '#fafafa';

    // Build vis.js DataSets
    const nodes = new vis.DataSet(data.nodes);
    const edges = new vis.DataSet(data.edges);

    const options = {
        nodes: {
            font: {
                size: 100,
                color: textColor,
                face: 'Lato, -apple-system, BlinkMacSystemFont, sans-serif',
            },
        },
        edges: {
            font: { size: 0 },
            scaling: { label: { enabled: false } },
        },
        physics: {
            enabled: true,
            barnesHut: {
                gravitationalConstant: -80000,
                springLength: 200,
                springConstant: 0.00007,
            },
            stabilization: {
                enabled: true,
                iterations: 300,
            },
            adaptiveTimestep: true,
            minVelocity: 0.75,
        },
        interaction: {
            tooltipDelay: 200,
            hover: true,
        },
    };

    const network = new vis.Network(container, { nodes, edges }, options);
    container._visNetwork = network;

    // --- Disable physics after stabilization ---
    network.once('stabilizationIterationsDone', function() {
        network.setOptions({ physics: { enabled: false } });
    });

    // --- Click-to-highlight ---
    network.on('click', function(params) {
        if (params.nodes.length === 1) {
            const nodeId = params.nodes[0];

            // If it's a cluster, open it
            if (network.isCluster(nodeId)) {
                network.openCluster(nodeId);
                return;
            }

            // Use path triples [client, warehouse, database] to highlight
            // only the exact routes that involve the clicked node.
            const paths = data.paths || [];
            const highlightNodes = [nodeId];

            for (let p = 0; p < paths.length; p++) {
                const client = paths[p][0];
                const wh = paths[p][1];
                const db = paths[p][2];
                if (client === nodeId || wh === nodeId || db === nodeId) {
                    if (highlightNodes.indexOf(client) === -1) highlightNodes.push(client);
                    if (highlightNodes.indexOf(wh) === -1) highlightNodes.push(wh);
                    if (highlightNodes.indexOf(db) === -1) highlightNodes.push(db);
                }
            }

            // Collect edges where both endpoints are in highlightNodes
            const allEdgeData = edges.get();
            const connectedEdges = [];
            for (let i = 0; i < allEdgeData.length; i++) {
                const e = allEdgeData[i];
                if (highlightNodes.indexOf(e.from) !== -1 &&
                    highlightNodes.indexOf(e.to) !== -1) {
                    connectedEdges.push(e.id);
                }
            }

            const allNodes = network.body.data.nodes;
            const allEdges = network.body.data.edges;
            const nodeUpdates = [];
            const edgeUpdates = [];

            const nodeIds = allNodes.getIds();
            for (let i = 0; i < nodeIds.length; i++) {
                const nid = nodeIds[i];
                nodeUpdates.push({
                    id: nid,
                    opacity: highlightNodes.indexOf(nid) === -1 ? 0.15 : 1.0,
                });
            }

            const edgeIds = allEdges.getIds();
            for (let j = 0; j < edgeIds.length; j++) {
                const eid = edgeIds[j];
                edgeUpdates.push({
                    id: eid,
                    hidden: connectedEdges.indexOf(eid) === -1,
                });
            }

            allNodes.update(nodeUpdates);
            allEdges.update(edgeUpdates);

            // Send selected node back to Python
            setTriggerValue('selected_node', nodeId);
        }
    });

    // --- Double-click to reset ---
    network.on('doubleClick', function() {
        const allNodes = network.body.data.nodes;
        const allEdges = network.body.data.edges;

        const nodeIds = allNodes.getIds();
        const nodeUpdates = [];
        for (let i = 0; i < nodeIds.length; i++) {
            nodeUpdates.push({ id: nodeIds[i], opacity: 1.0 });
        }
        allNodes.update(nodeUpdates);

        const edgeIds = allEdges.getIds();
        const edgeUpdates = [];
        for (let j = 0; j < edgeIds.length; j++) {
            edgeUpdates.push({ id: edgeIds[j], hidden: false });
        }
        allEdges.update(edgeUpdates);
    });

    // --- Clustering ---
    const clusterMap = data.clusters || {};
    let clustersActive = false;

    const clusterBtn = parentElement.querySelector('#clusterBtn');
    const resetBtn = parentElement.querySelector('#resetClustersBtn');

    function applyClusters() {
        const labels = Object.keys(clusterMap);
        for (let i = 0; i < labels.length; i++) {
            const label = labels[i];
            const members = clusterMap[label];
            (function(lbl, mems) {
                network.cluster({
                    joinCondition: function(nodeOptions) {
                        return mems.indexOf(nodeOptions.id) !== -1;
                    },
                    clusterNodeProperties: {
                        id: 'cluster:' + lbl,
                        label: lbl + ' (' + mems.length + ')',
                        shape: 'box',
                        font: {
                            size: 80,
                            color: textColor,
                            face: 'Lato, sans-serif',
                            bold: true,
                        },
                        color: {
                            background: 'rgba(41, 181, 232, 0.15)',
                            border: '#29B5E8',
                            highlight: {
                                background: 'rgba(41, 181, 232, 0.3)',
                                border: '#29B5E8',
                            },
                        },
                        borderWidth: 2,
                        margin: 20,
                    },
                });
            })(label, members);
        }
        clustersActive = true;
        if (clusterBtn) clusterBtn.style.display = 'none';
        if (resetBtn) resetBtn.style.display = 'block';
        network.setOptions({ physics: { enabled: true } });
        network.once('stabilizationIterationsDone', function() {
            network.setOptions({ physics: { enabled: false } });
        });
        network.stabilize(150);
    }

    function resetClusters() {
        const labels = Object.keys(clusterMap);
        for (let i = 0; i < labels.length; i++) {
            const clusterId = 'cluster:' + labels[i];
            if (network.isCluster(clusterId)) {
                network.openCluster(clusterId);
            }
        }
        clustersActive = false;
        if (resetBtn) resetBtn.style.display = 'none';
        if (clusterBtn) clusterBtn.style.display = 'block';
        network.setOptions({ physics: { enabled: true } });
        network.once('stabilizationIterationsDone', function() {
            network.setOptions({ physics: { enabled: false } });
        });
        network.stabilize(150);
    }

    if (clusterBtn) clusterBtn.onclick = applyClusters;
    if (resetBtn) resetBtn.onclick = resetClusters;

    // --- Cleanup ---
    return function() {
        if (container._visNetwork) {
            container._visNetwork.destroy();
            container._visNetwork = null;
        }
    };
}
"""

    # Inject vis.js code as a JSON string literal into the placeholder
    escaped_vis = json.dumps(vis_js_code)
    final_js = component_js.replace("VIS_JS_PLACEHOLDER", escaped_vis)
    return final_js


# ---------------------------------------------------------------------------
# Component registration (called once)
# ---------------------------------------------------------------------------

@st.cache_resource(show_spinner=False)
def _get_component():
    """Register the v2 component once and return the mount callable."""
    return st.components.v2.component(
        "data_lake_network",
        html=_COMPONENT_HTML,
        css=_COMPONENT_CSS,
        js=_build_js(),
        isolate_styles=False,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def render_network(df: pd.DataFrame, _node_images: Dict[str, str],
                   session, fullscreen: bool = False):
    """Build and render the network graph as a v2 component.

    Returns the BidiComponentResult with .selected_node available.
    """
    if df.empty:
        st.info("No rows available to render.")
        return None

    current_account = get_current_account(session)

    # Aggregate edges and compute stats
    agg_df = _aggregate_edges(df)
    node_stats = _compute_node_stats(agg_df)

    all_totals = [s["total"] for s in node_stats.values()] or [1]
    global_min, global_max = min(all_totals), max(all_totals)

    # Build node and edge lists as JSON-serializable dicts
    nodes = []
    edges = []
    added_nodes: set = set()
    cluster_members: Dict[str, list] = defaultdict(list)

    transparent = {
        "background": "rgba(0,0,0,0)",
        "border": "rgba(0,0,0,0)",
        "highlight": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"},
    }
    shape_props = {"useBorderWithImage": True, "borderType": "circle"}

    for _, row in agg_df.iterrows():
        database = row["DATABASE"]
        warehouse = row["WAREHOUSE"]
        ac = int(row["ACCESS_COUNT"])
        direction = row["DIRECTION"]
        org_name = row["ORGANIZATION_NAME"]
        client = row["CLIENT"]

        # Add nodes (deduplicated)
        if database not in added_nodes:
            s = node_stats.get(database, {})
            db_size = _log_scale(s.get("total", 0), global_min, global_max, 120, 300)
            tooltip = _build_tooltip(database, "Database", s, org_name, current_account)

            cluster_label = _assign_cluster(database)
            if cluster_label:
                cluster_members[cluster_label].append(database)

            nodes.append({
                "id": database, "label": database, "title": tooltip,
                "size": int(db_size), "color": transparent, "shape": "image",
                "shapeProperties": shape_props, "borderWidth": 0,
                "image": _node_images["database"],
            })
            added_nodes.add(database)

        if warehouse not in added_nodes:
            s = node_stats.get(warehouse, {})
            wh_size = _log_scale(s.get("total", 0), global_min, global_max, 120, 300)
            tooltip = _build_tooltip(warehouse, "Warehouse", s, org_name, current_account)
            nodes.append({
                "id": warehouse, "label": warehouse, "title": tooltip,
                "size": int(wh_size), "color": transparent, "shape": "image",
                "shapeProperties": shape_props, "borderWidth": 0,
                "image": _node_images["warehouse"],
            })
            added_nodes.add(warehouse)

        if client not in added_nodes:
            s = node_stats.get(client, {})
            cl_size = _log_scale(s.get("total", 0), global_min, global_max, 80, 200)
            tooltip = _build_tooltip(client, "Client", s, org_name, current_account)
            client_icon = generate_client_icon_uri(client)
            nodes.append({
                "id": client, "label": client, "title": tooltip,
                "size": int(cl_size), "color": transparent, "shape": "image",
                "shapeProperties": shape_props, "borderWidth": 0,
                "image": client_icon,
            })
            added_nodes.add(client)

        # Add edges
        edge_color = AMBER if direction in ("write", "DML", "DDL") else SNOWFLAKE_BLUE
        edge_title = f"Access Count: {ac:,}"

        if direction in ("write", "DML", "DDL"):
            edges.append({
                "from": client, "to": warehouse, "value": ac,
                "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
            })
            edges.append({
                "from": warehouse, "to": database, "value": ac,
                "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
            })
        else:
            edges.append({
                "from": database, "to": warehouse, "value": ac,
                "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
            })
            edges.append({
                "from": warehouse, "to": client, "value": ac,
                "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
            })

    # Build path triples for precise click-to-highlight
    paths = []
    for _, row in agg_df.iterrows():
        paths.append([row["CLIENT"], row["WAREHOUSE"], row["DATABASE"]])

    clusters = {label: members for label, members in cluster_members.items() if members}

    component_data = {
        "nodes": nodes,
        "edges": edges,
        "paths": paths,
        "clusters": clusters,
        "fullscreen": fullscreen,
    }

    mount = _get_component()
    height = "stretch" if fullscreen else 720

    result = mount(
        data=component_data,
        on_selected_node_change=lambda: None,
        height=height,
        key="network_graph",
    )

    return result
