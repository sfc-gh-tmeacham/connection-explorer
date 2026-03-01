"""Network graph component using Streamlit Components v2 and vis-network.js.

Builds an interactive force-directed graph where clients, warehouses, and
databases are nodes and access events are weighted, directed edges.  Uses
vis.js ``DataSet`` operations for click-to-isolate / restore and physics-based
layout.  Custom tooltips are driven by vis.js hover events because the
built-in tooltip mechanism is unavailable inside Components v2.
"""

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
from components.theme import AMBER, READ_GREEN, SNOWFLAKE_BLUE


# ---------------------------------------------------------------------------
# Helpers (unchanged from v1 implementation)
# ---------------------------------------------------------------------------

def _aggregate_edges(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate duplicate edges by summing ACCESS_COUNT.

    Groups rows by (CLIENT, WAREHOUSE, DATABASE, DIRECTION) and sums their
    access counts so each unique route appears only once.

    Args:
        df: Raw access DataFrame with at least CLIENT, WAREHOUSE, DATABASE,
            DIRECTION, ACCESS_COUNT, ORGANIZATION_NAME, and ACCOUNT_NAME
            columns.

    Returns:
        Aggregated DataFrame sorted by ACCESS_COUNT descending.
    """
    group_cols = ["CLIENT", "WAREHOUSE", "DATABASE", "DIRECTION"]
    agg = (
        df.groupby(group_cols, as_index=False)
        .agg(ACCESS_COUNT=("ACCESS_COUNT", "sum"),
             ORGANIZATION_NAME=("ORGANIZATION_NAME", "first"),
             ACCOUNT_NAME=("ACCOUNT_NAME", "first"))
    )
    return agg.sort_values("ACCESS_COUNT", ascending=False)


def _compute_node_stats(df: pd.DataFrame) -> Dict[str, dict]:
    """Compute per-node stats: total access, read/write breakdown, top connections.

    Iterates over every row and accumulates totals for each database,
    warehouse, and client node.  Also tracks which other nodes each node
    is connected to (and by how much) so tooltips can show "Top
    Connections".

    Args:
        df: Aggregated access DataFrame (output of ``_aggregate_edges``).

    Returns:
        Dict mapping node name to a stats dict with keys ``total``,
        ``read``, ``write``, and ``connections`` (a dict of neighbor
        name to cumulative access count).
    """
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
    """Map *value* from ``[min_val, max_val]`` to ``[out_min, out_max]`` on a log scale.

    Uses ``math.log1p`` so that zero values map cleanly to *out_min*.

    Args:
        value: The input value to transform.
        min_val: Lower bound of the input range.
        max_val: Upper bound of the input range.
        out_min: Lower bound of the output range.
        out_max: Upper bound of the output range.

    Returns:
        The log-scaled output value clamped to ``[out_min, out_max]``.
    """
    if max_val <= min_val or value <= 0:
        return out_min
    log_min = math.log1p(min_val)
    log_max = math.log1p(max_val)
    log_val = math.log1p(value)
    t = (log_val - log_min) / (log_max - log_min)
    return out_min + t * (out_max - out_min)


def _build_tooltip(node_name: str, node_type: str, stats: dict,
                   org_name: str, account: str) -> str:
    """Build a rich plain-text tooltip string for a graph node.

    The tooltip includes organization / account context, total and
    read/write access counts, and the top three connected nodes by
    traffic volume.

    Args:
        node_name: Display name of the node (e.g. ``"ANALYTICS_DB"``).
        node_type: Human-readable category — ``"Database"``,
            ``"Warehouse"``, or ``"Client"``.
        stats: Per-node stats dict as returned by ``_compute_node_stats``.
        org_name: Snowflake organization name for context.
        account: Snowflake account name for context.

    Returns:
        Multi-line tooltip string suitable for display in a custom
        tooltip ``<div>``.
    """
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
    """Return the cluster label for a database, or empty string if unclustered.

    Matches *db_name* against ``_CLUSTER_RULES`` in order and returns the
    label of the first matching rule.  Databases that match no rule are
    left unclustered.

    Args:
        db_name: The database name to classify.

    Returns:
        Cluster label string (e.g. ``"Raw Layer"``) or ``""`` if no rule
        matches.
    """
    for label, predicate in _CLUSTER_RULES:
        if predicate(db_name):
            return label
    return ""


# ---------------------------------------------------------------------------
# vis-network.js loader
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_vis_js() -> str:
    """Read the vis-network.min.js source from the installed pyvis package.

    The result is cached so the file is read at most once per process
    lifetime.

    Returns:
        The full contents of ``vis-network.min.js`` as a string.
    """
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
    top: 16px;
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

#custom-tooltip {{
    display: none;
    position: absolute;
    z-index: 200;
    pointer-events: none;
    font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 12px;
    background-color: var(--st-background-color, #0e1117);
    color: var(--st-text-color, #fafafa);
    border: 1px solid rgba(128,128,128,0.3);
    border-radius: 4px;
    padding: 8px 10px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.15);
    line-height: 1.5;
    white-space: pre-line;
    max-width: 320px;
}}
"""


# ---------------------------------------------------------------------------
# HTML — container + legend
# ---------------------------------------------------------------------------

_COMPONENT_HTML = f"""
<div class="vis-network-container">
    <div id="vis-canvas"></div>
    <div id="custom-tooltip"></div>
    <div id="graph-legend">
        <div class="legend-title">Edge Direction</div>
        <div class="legend-item">
            <span class="legend-line" style="background-color: {READ_GREEN};"></span>
            <span>Read</span>
        </div>
        <div class="legend-item">
            <span class="legend-line" style="background-color: {AMBER};"></span>
            <span>Write</span>
        </div>
        <div class="legend-section">
            <div class="legend-title">Interaction</div>
            <div style="font-size:11px; opacity:0.7;">Click node to isolate</div>
            <div style="font-size:11px; opacity:0.7;">Click canvas to restore</div>
        </div>
        <button class="legend-btn" id="resetClustersBtn" style="display:none;">Expand All</button>
    </div>
</div>
"""


# ---------------------------------------------------------------------------
# JS module — vis.js bootstrap, network creation, interactions
# ---------------------------------------------------------------------------

def _build_js() -> str:
    """Build the complete JavaScript module for the vis.js network component.

    Concatenates the vis-network UMD bundle with custom component code
    that handles network creation, click-to-isolate / restore, custom
    tooltips, and database clustering.  The vis.js source is injected as
    a JSON string literal so it can be evaluated at runtime via the
    ``Function`` constructor.

    Returns:
        A single JavaScript module string ready to be passed to
        ``st.components.v2.component(js=...)``.
    """
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
            tooltipDelay: 0,
            hover: true,
            tooltip: false,
        },
    };

    const network = new vis.Network(container, { nodes, edges }, options);
    container._visNetwork = network;

    // --- Disable physics after initial stabilization ---
    network.once('stabilizationIterationsDone', function() {
        network.setOptions({ physics: { enabled: false } });
    });

    // --- Helper: re-enable physics briefly so nodes reorganize, then freeze ---
    function restabilize() {
        network.setOptions({ physics: { enabled: true } });
        network.once('stabilizationIterationsDone', function() {
            network.setOptions({ physics: { enabled: false } });
            network.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' } });
        });
        network.stabilize();
    }

    // --- Store original data for restore ---
    const originalNodes = data.nodes.slice();
    const originalEdges = data.edges.slice();
    let isolated = false;

    // --- Click: isolate subgraph or restore full network ---
    network.on('click', function(params) {
        if (params.nodes.length === 1) {
            const nodeId = params.nodes[0];

            // If it's a cluster, open it
            if (network.isCluster(nodeId)) {
                network.openCluster(nodeId);
                return;
            }

            // Use path triples [client, warehouse, database] to find
            // only the exact routes that involve the clicked node.
            const paths = data.paths || [];
            const keepNodes = [nodeId];
            const keepPathIndices = [];  // indices of matching paths

            for (let p = 0; p < paths.length; p++) {
                const client = paths[p][0];
                const wh = paths[p][1];
                const db = paths[p][2];
                if (client === nodeId || wh === nodeId || db === nodeId) {
                    if (keepNodes.indexOf(client) === -1) keepNodes.push(client);
                    if (keepNodes.indexOf(wh) === -1) keepNodes.push(wh);
                    if (keepNodes.indexOf(db) === -1) keepNodes.push(db);
                    keepPathIndices.push(p);
                }
            }

            // Find edges whose pathIndex matches a kept path
            const keepEdgeIds = [];
            for (let i = 0; i < originalEdges.length; i++) {
                const e = originalEdges[i];
                if (e.pathIndex !== undefined && keepPathIndices.indexOf(e.pathIndex) !== -1) {
                    keepEdgeIds.push(e.id);
                }
            }

            // Remove nodes not in the subgraph
            const removeNodeIds = [];
            const currentNodeIds = nodes.getIds();
            for (let i = 0; i < currentNodeIds.length; i++) {
                if (keepNodes.indexOf(currentNodeIds[i]) === -1) {
                    removeNodeIds.push(currentNodeIds[i]);
                }
            }
            if (removeNodeIds.length > 0) nodes.remove(removeNodeIds);

            // Remove edges not in the subgraph
            const removeEdgeIds = [];
            const currentEdgeIds = edges.getIds();
            for (let i = 0; i < currentEdgeIds.length; i++) {
                if (keepEdgeIds.indexOf(currentEdgeIds[i]) === -1) {
                    removeEdgeIds.push(currentEdgeIds[i]);
                }
            }
            if (removeEdgeIds.length > 0) edges.remove(removeEdgeIds);

            isolated = true;

            // Let physics re-layout the isolated subgraph
            restabilize();

            // Send selected node back to Python
            setTriggerValue('selected_node', nodeId);

        } else if (params.nodes.length === 0 && params.edges.length === 0 && isolated) {
            // Clicked empty canvas — restore full network
            nodes.clear();
            nodes.add(originalNodes);
            edges.clear();
            edges.add(originalEdges);
            isolated = false;

            // Re-run physics so restored network spreads out and fits
            restabilize();

            // Tell Python the isolation was cleared
            setTriggerValue('selected_node', null);
        }
    });

    // --- Custom tooltip (vis.js built-in doesn't work in v2 components) ---
    const tooltip = parentElement.querySelector('#custom-tooltip');
    const canvasEl = container.querySelector('canvas');

    function showTooltip(text, event) {
        if (!tooltip || !text) return;
        tooltip.textContent = text;
        tooltip.style.display = 'block';
        // Position relative to the vis-network-container
        const containerRect = parentElement.querySelector('.vis-network-container').getBoundingClientRect();
        var x = event.event.clientX - containerRect.left + 12;
        var y = event.event.clientY - containerRect.top + 12;
        // Keep tooltip within container bounds
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }

    function hideTooltip() {
        if (tooltip) tooltip.style.display = 'none';
    }

    network.on('hoverNode', function(params) {
        var nodeData = nodes.get(params.node);
        if (nodeData && nodeData.title) {
            showTooltip(nodeData.title, params);
        }
    });

    network.on('blurNode', function() {
        hideTooltip();
    });

    network.on('hoverEdge', function(params) {
        var edgeData = edges.get(params.edge);
        if (edgeData && edgeData.title) {
            showTooltip(edgeData.title, params);
        }
    });

    network.on('blurEdge', function() {
        hideTooltip();
    });

    // --- Clustering ---
    const clusterMap = data.clusters || {};
    let clustersActive = false;

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
        network.setOptions({ physics: { enabled: true } });
        network.once('stabilizationIterationsDone', function() {
            network.setOptions({ physics: { enabled: false } });
        });
        network.stabilize(150);
    }

    if (resetBtn) resetBtn.onclick = resetClusters;

    // Auto-cluster on load when checkbox is checked
    if (data.cluster_databases) {
        applyClusters();
    }

    // --- Auto-isolate on load if a node was previously selected ---
    if (data.isolated_node) {
        const nodeId = data.isolated_node;
        const paths = data.paths || [];
        const keepNodes = [nodeId];
        const keepPathIndices = [];

        for (let p = 0; p < paths.length; p++) {
            const client = paths[p][0];
            const wh = paths[p][1];
            const db = paths[p][2];
            if (client === nodeId || wh === nodeId || db === nodeId) {
                if (keepNodes.indexOf(client) === -1) keepNodes.push(client);
                if (keepNodes.indexOf(wh) === -1) keepNodes.push(wh);
                if (keepNodes.indexOf(db) === -1) keepNodes.push(db);
                keepPathIndices.push(p);
            }
        }

        const keepEdgeIds = [];
        for (let i = 0; i < originalEdges.length; i++) {
            const e = originalEdges[i];
            if (e.pathIndex !== undefined && keepPathIndices.indexOf(e.pathIndex) !== -1) {
                keepEdgeIds.push(e.id);
            }
        }

        const removeNodeIds = [];
        const currentNodeIds = nodes.getIds();
        for (let i = 0; i < currentNodeIds.length; i++) {
            if (keepNodes.indexOf(currentNodeIds[i]) === -1) {
                removeNodeIds.push(currentNodeIds[i]);
            }
        }
        if (removeNodeIds.length > 0) nodes.remove(removeNodeIds);

        const removeEdgeIds = [];
        const currentEdgeIds = edges.getIds();
        for (let i = 0; i < currentEdgeIds.length; i++) {
            if (keepEdgeIds.indexOf(currentEdgeIds[i]) === -1) {
                removeEdgeIds.push(currentEdgeIds[i]);
            }
        }
        if (removeEdgeIds.length > 0) edges.remove(removeEdgeIds);

        isolated = true;
        restabilize();
    }

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
    """Register the Streamlit Components v2 component and return its mount callable.

    The component is registered once per Streamlit server process via
    ``@st.cache_resource``.  Subsequent calls return the cached mount
    function.

    Returns:
        A callable that mounts the vis.js network component into the
        Streamlit page and returns a ``BidiComponentResult``.
    """
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
                   session, fullscreen: bool = False,
                   hide_warehouses: bool = False,
                   cluster_databases: bool = False):
    """Build and render the interactive network graph as a v2 component.

    Aggregates the access DataFrame, computes per-node statistics, then
    constructs JSON-serializable node / edge / path lists that are passed
    to the vis.js front-end.  Each edge carries a ``pathIndex`` that links
    it to its exact ``[client, warehouse, database]`` path triple so the
    JS click-to-isolate handler can filter edges unambiguously.

    Args:
        df: Filtered access DataFrame.  If empty, an info message is
            shown and ``None`` is returned.
        _node_images: Dict mapping node type (``"database"``,
            ``"warehouse"``) to base-64 data-URI strings used as vis.js
            node images.
        session: Active Snowflake session (or ``None`` in sample-data
            mode) used to resolve the current account name.
        fullscreen: If ``True``, the graph container expands to fill the
            viewport height.
        hide_warehouses: If ``True``, warehouse nodes are omitted and
            edges connect clients directly to databases.
        cluster_databases: If ``True``, database nodes are automatically
            grouped into vis.js clusters on load based on naming rules.

    Returns:
        The ``BidiComponentResult`` from the mounted component (with a
        ``selected_node`` trigger value), or ``None`` if *df* is empty.
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
    paths = []
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

        if not hide_warehouses and warehouse not in added_nodes:
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
        edge_color = AMBER if direction in ("write", "DML", "DDL") else READ_GREEN
        dir_label = direction.upper() if direction else "READ"
        edge_title = f"{client} → {warehouse} → {database}\nDirection: {dir_label}\nAccess Count: {ac:,}"
        path_idx = len(paths)

        if hide_warehouses:
            paths.append([client, client, database])
            edge_title_direct = f"{client} → {database}\nDirection: {dir_label}\nWarehouse: {warehouse}\nAccess Count: {ac:,}"
            # Direct edge: client ↔ database (skip warehouse)
            if direction in ("write", "DML", "DDL"):
                edges.append({
                    "id": f"e{len(edges)}", "from": client, "to": database,
                    "value": ac, "color": edge_color, "arrows": "to",
                    "arrowStrikethrough": False, "title": edge_title_direct,
                    "pathIndex": path_idx,
                })
            else:
                edges.append({
                    "id": f"e{len(edges)}", "from": database, "to": client,
                    "value": ac, "color": edge_color, "arrows": "to",
                    "arrowStrikethrough": False, "title": edge_title_direct,
                    "pathIndex": path_idx,
                })
        elif direction in ("write", "DML", "DDL"):
            paths.append([client, warehouse, database])
            edges.append({
                "id": f"e{len(edges)}", "from": client, "to": warehouse,
                "value": ac, "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
                "pathIndex": path_idx,
            })
            edges.append({
                "id": f"e{len(edges)}", "from": warehouse, "to": database,
                "value": ac, "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
                "pathIndex": path_idx,
            })
        else:
            paths.append([client, warehouse, database])
            edges.append({
                "id": f"e{len(edges)}", "from": database, "to": warehouse,
                "value": ac, "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
                "pathIndex": path_idx,
            })
            edges.append({
                "id": f"e{len(edges)}", "from": warehouse, "to": client,
                "value": ac, "color": edge_color, "arrows": "to",
                "arrowStrikethrough": False, "title": edge_title,
                "pathIndex": path_idx,
            })

    clusters = {label: members for label, members in cluster_members.items() if members}

    isolated_node = st.session_state.get("isolated_node", None)
    # Clear isolation if the node no longer exists in the current graph
    if isolated_node and isolated_node not in added_nodes:
        isolated_node = None
        st.session_state["isolated_node"] = None

    component_data = {
        "nodes": nodes,
        "edges": edges,
        "paths": paths,
        "clusters": clusters,
        "cluster_databases": cluster_databases,
        "isolated_node": isolated_node,
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

    # Persist the isolated node for checkbox toggles / reruns
    if result and hasattr(result, 'selected_node'):
        st.session_state["isolated_node"] = result.selected_node

    return result
