"""Network graph component using Streamlit Components v2 and vis-network.js.

Builds an interactive force-directed graph where clients, warehouses, and
databases are nodes and access events are weighted, directed edges.  Uses
vis.js ``DataSet`` operations for click-to-filter and physics-based
layout.  Custom tooltips are driven by vis.js hover events because the
built-in tooltip mechanism is unavailable inside Components v2.
"""

import json
import logging
import math
import os
from collections import defaultdict
from functools import lru_cache
from typing import Dict

logger = logging.getLogger(__name__)

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
    height: calc(100vh - 220px);
    border: none;
    position: relative;
}}

#vis-canvas.fullscreen {{
    height: 100vh;
}}

#loading-overlay {{
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    background: var(--st-background-color, #0e1117);
    z-index: 200;
    transition: opacity 0.4s ease;
}}

#loading-text {{
    font-family: 'Lato', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 14px;
    color: var(--st-text-color, #fafafa);
    opacity: 0.7;
    margin-bottom: 12px;
}}

#loading-bar-track {{
    width: 200px;
    height: 4px;
    background: rgba(128,128,128,0.2);
    border-radius: 2px;
    overflow: hidden;
}}

#loading-bar-fill {{
    width: 0%;
    height: 100%;
    background: #29B5E8;
    border-radius: 2px;
    transition: width 0.15s ease;
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
    <div id="loading-overlay">
        <div id="loading-text">Building network…</div>
        <div id="loading-bar-track"><div id="loading-bar-fill"></div></div>
    </div>
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
            <div style="font-size:11px; opacity:0.7;">Click node to filter · Click canvas to clear</div>
        </div>
        <button class="legend-btn" id="resetClustersBtn" style="display:none;">Expand All</button>
        <button class="legend-btn" id="downloadPngBtn">&#128247; Save PNG</button>
    </div>
</div>
"""


# ---------------------------------------------------------------------------
# JS module — vis.js bootstrap, network creation, interactions
# ---------------------------------------------------------------------------

def _build_js() -> str:
    """Build the complete JavaScript module for the vis.js network component.

    Concatenates the vis-network UMD bundle with custom component code
    that handles network creation, click-to-filter, custom
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

    // Use the pre-built nodeTypeMap passed as top-level component data.
    // This avoids any issues with vis.js DataSet or Streamlit serialization
    // stripping custom properties from individual node objects.
    const nodeTypeMap = data.nodeTypeMap || {};

    const options = {
        nodes: {
            font: {
                size: 100,
                color: textColor,
                face: 'Lato, -apple-system, BlinkMacSystemFont, sans-serif',
                strokeWidth: 4,
                strokeColor: 'rgba(0,0,0,0.65)',
            },
        },
        edges: {
            font: { size: 0 },
            scaling: { label: { enabled: false } },
        },
        physics: {
            enabled: true,
            barnesHut: {
                gravitationalConstant: -120000,
                springLength: 400,
                springConstant: 0.001,
                damping: 0.3,
                avoidOverlap: 0.5,
            },
            stabilization: {
                enabled: true,
                iterations: 1000,
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

    // --- Loading progress bar ---
    var loadingOverlay = container.parentElement.querySelector('#loading-overlay');
    var loadingBarFill = container.parentElement.querySelector('#loading-bar-fill');

    network.on('stabilizationProgress', function(params) {
        var pct = Math.round((params.iterations / params.total) * 100);
        if (loadingBarFill) loadingBarFill.style.width = pct + '%';
    });

    // --- Disable physics after initial stabilization ---
    network.once('stabilizationIterationsDone', function() {
        if (loadingBarFill) loadingBarFill.style.width = '100%';
        if (loadingOverlay) {
            loadingOverlay.style.opacity = '0';
            setTimeout(function() { loadingOverlay.style.display = 'none'; }, 400);
        }
        network.setOptions({ physics: { enabled: false } });
    });

    // --- Helper: re-enable physics briefly so nodes reorganize, then freeze ---
    var stabilizing = false;
    function restabilize() {
        stabilizing = true;
        if (loadingOverlay) {
            loadingOverlay.style.display = 'flex';
            loadingOverlay.style.opacity = '1';
        }
        if (loadingBarFill) loadingBarFill.style.width = '0%';

        network.setOptions({ physics: { enabled: true } });
        network.once('stabilizationIterationsDone', function() {
            if (loadingBarFill) loadingBarFill.style.width = '100%';
            if (loadingOverlay) {
                loadingOverlay.style.opacity = '0';
                setTimeout(function() { loadingOverlay.style.display = 'none'; }, 400);
            }
            network.setOptions({ physics: { enabled: false } });
            network.fit({ animation: { duration: 400, easingFunction: 'easeInOutQuad' } });
            stabilizing = false;
        });
        network.stabilize();
    }

    // --- Click: send node info to Python for filtering ---
    network.on('click', function(params) {
        if (stabilizing) return;
        if (params.nodes.length === 1) {
            const nodeId = params.nodes[0];

            // If it's a cluster, open it
            if (network.isCluster(nodeId)) {
                network.openCluster(nodeId);
                return;
            }

            // Send node type + id to Python so it can add to sidebar filters
            // _ts ensures each click produces a unique trigger value.
            const nodeType = nodeTypeMap[nodeId] || 'unknown';
            setTriggerValue('selected_node', { nodeId: nodeId, nodeType: nodeType, _ts: Date.now() });
        } else if (params.nodes.length === 0 && params.edges.length === 0) {
            // Clicked empty canvas — tell Python to clear all filters
            setTriggerValue('selected_node', { action: 'clear_all', _ts: Date.now() });
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

    // --- Hover: glow effect + fade unrelated nodes/edges ---
    let hoverActive = false;
    let hoveredNodeId = null;

    function getConnectedSet(nodeId) {
        const paths = data.paths || [];
        const connectedNodes = [nodeId];
        const connectedPathIndices = [];
        for (let p = 0; p < paths.length; p++) {
            const client = paths[p][0];
            const wh = paths[p][1];
            const db = paths[p][2];
            if (client === nodeId || wh === nodeId || db === nodeId) {
                if (connectedNodes.indexOf(client) === -1) connectedNodes.push(client);
                if (connectedNodes.indexOf(wh) === -1) connectedNodes.push(wh);
                if (connectedNodes.indexOf(db) === -1) connectedNodes.push(db);
                connectedPathIndices.push(p);
            }
        }
        const connectedEdgeIds = [];
        const allEdges = edges.get();
        for (let i = 0; i < allEdges.length; i++) {
            if (allEdges[i].pathIndex !== undefined &&
                connectedPathIndices.indexOf(allEdges[i].pathIndex) !== -1) {
                connectedEdgeIds.push(allEdges[i].id);
            }
        }
        return { connectedNodes: connectedNodes, connectedEdgeIds: connectedEdgeIds };
    }

    // Store original edge colors so we can restore them after hover fade
    const edgeOriginalColors = {};
    (function() {
        const allEdgeData = edges.get();
        for (let i = 0; i < allEdgeData.length; i++) {
            edgeOriginalColors[allEdgeData[i].id] = allEdgeData[i].color;
        }
    })();

    function applyHoverFade(nodeId) {
        const result = getConnectedSet(nodeId);
        const keepNodes = result.connectedNodes;
        const keepEdges = result.connectedEdgeIds;

        // Fade unrelated nodes
        const nodeUpdates = [];
        const allNodes = nodes.get();
        for (let i = 0; i < allNodes.length; i++) {
            var n = allNodes[i];
            if (keepNodes.indexOf(n.id) === -1) {
                nodeUpdates.push({ id: n.id, opacity: 0.12 });
            } else if (n.id === nodeId) {
                    nodeUpdates.push({ id: n.id, opacity: 1.0 });
                } else {
                nodeUpdates.push({ id: n.id, opacity: 1.0 });
            }
        }
        if (nodeUpdates.length > 0) nodes.update(nodeUpdates);

        // Fade unrelated edges (preserve original color string)
        const edgeUpdates = [];
        const allEdgeData = edges.get();
        for (let i = 0; i < allEdgeData.length; i++) {
            var e = allEdgeData[i];
            var origColor = edgeOriginalColors[e.id] || e.color;
            var colorStr = (typeof origColor === 'string') ? origColor : (origColor.color || '#848484');
            if (keepEdges.indexOf(e.id) === -1) {
                edgeUpdates.push({ id: e.id, color: { color: colorStr, opacity: 0.08 } });
            } else {
                edgeUpdates.push({ id: e.id, color: { color: colorStr, opacity: 1.0 } });
            }
        }
        if (edgeUpdates.length > 0) edges.update(edgeUpdates);

        hoverActive = true;
        hoveredNodeId = nodeId;
        network.redraw();
    }

    function clearHoverFade() {
        if (!hoverActive) return;
        hoveredNodeId = null;
        hoverActive = false;

        // Restore all nodes to full opacity
        const nodeUpdates = [];
        const allNodes = nodes.get();
        for (let i = 0; i < allNodes.length; i++) {
            nodeUpdates.push({ id: allNodes[i].id, opacity: 1.0 });
        }
        if (nodeUpdates.length > 0) nodes.update(nodeUpdates);

        // Restore all edges to their original color
        const edgeUpdates = [];
        const allEdgeData = edges.get();
        for (let i = 0; i < allEdgeData.length; i++) {
            var orig = edgeOriginalColors[allEdgeData[i].id];
            edgeUpdates.push({ id: allEdgeData[i].id, color: orig !== undefined ? orig : allEdgeData[i].color });
        }
        if (edgeUpdates.length > 0) edges.update(edgeUpdates);
    }

    // --- Canvas glow around hovered node, shape-matched ---
    // Cache for decoded node types so we only decode base64 once per node.
    var _nodeTypeCache = {};
    function detectNodeType(nodeId) {
        if (_nodeTypeCache[nodeId]) return _nodeTypeCache[nodeId];
        // Try the top-level map first
        if (nodeTypeMap[nodeId]) { _nodeTypeCache[nodeId] = nodeTypeMap[nodeId]; return nodeTypeMap[nodeId]; }
        // Fall back: decode the base64 SVG image and look for shape markers
        var nd = nodes.get(nodeId);
        if (nd && nd.image && nd.image.indexOf('base64,') !== -1) {
            try {
                var svg = atob(nd.image.split('base64,')[1]);
                if (svg.indexOf('<polygon') !== -1) { _nodeTypeCache[nodeId] = 'warehouse'; return 'warehouse'; }
                if (svg.indexOf('<rect') !== -1) { _nodeTypeCache[nodeId] = 'database'; return 'database'; }
            } catch(e) {}
        }
        _nodeTypeCache[nodeId] = 'client';
        return 'client';
    }

    network.on('afterDrawing', function(ctx) {
        if (!hoveredNodeId) return;
        var pos = network.getPositions([hoveredNodeId])[hoveredNodeId];
        if (!pos) return;
        var nodeData = nodes.get(hoveredNodeId);
        var nodeSize = (nodeData && nodeData.size ? nodeData.size : 30);
        var ntype = detectNodeType(hoveredNodeId);
        var r = nodeSize + 10;

        ctx.save();
        ctx.beginPath();

        if (ntype === 'warehouse') {
            // Diamond
            ctx.moveTo(pos.x, pos.y - r);
            ctx.lineTo(pos.x + r, pos.y);
            ctx.lineTo(pos.x, pos.y + r);
            ctx.lineTo(pos.x - r, pos.y);
            ctx.closePath();
        } else if (ntype === 'database') {
            // Rounded rectangle
            var hw = r;
            var hh = r;
            var cr = r * 0.15;
            var x0 = pos.x - hw, y0 = pos.y - hh;
            var w = hw * 2, h = hh * 2;
            ctx.moveTo(x0 + cr, y0);
            ctx.lineTo(x0 + w - cr, y0);
            ctx.quadraticCurveTo(x0 + w, y0, x0 + w, y0 + cr);
            ctx.lineTo(x0 + w, y0 + h - cr);
            ctx.quadraticCurveTo(x0 + w, y0 + h, x0 + w - cr, y0 + h);
            ctx.lineTo(x0 + cr, y0 + h);
            ctx.quadraticCurveTo(x0, y0 + h, x0, y0 + h - cr);
            ctx.lineTo(x0, y0 + cr);
            ctx.quadraticCurveTo(x0, y0, x0 + cr, y0);
            ctx.closePath();
        } else {
            // Circle
            ctx.arc(pos.x, pos.y, r, 0, 2 * Math.PI);
        }

        ctx.strokeStyle = 'rgba(41,181,232,0.7)';
        ctx.lineWidth = 3;
        ctx.shadowColor = 'rgba(41,181,232,0.8)';
        ctx.shadowBlur = 25;
        ctx.stroke();
        ctx.shadowBlur = 45;
        ctx.shadowColor = 'rgba(41,181,232,0.4)';
        ctx.stroke();
        ctx.restore();
    });

    network.on('hoverNode', function(params) {
        var nodeData = nodes.get(params.node);
        if (nodeData && nodeData.title) {
            showTooltip(nodeData.title, params);
        }
        applyHoverFade(params.node);
    });

    network.on('blurNode', function() {
        hideTooltip();
        clearHoverFade();
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

    // --- Download PNG button ---
    const dlBtn = parentElement.querySelector('#downloadPngBtn');
    if (dlBtn) {
        dlBtn.onclick = function() {
            // Strategy: temporarily enlarge the vis.js container so the
            // canvas renders at high resolution natively, capture that
            // frame, draw labels on top, then restore.
            const container = parentElement.querySelector('#vis-canvas');
            if (!container) return;

            // Save original dimensions
            const origCW = container.clientWidth;
            const origCH = container.clientHeight;
            const origW = container.style.width;
            const origH = container.style.height;
            const origPos = container.style.position;
            const origOverflow = container.parentElement.style.overflow;
            const origParentH = container.parentElement.style.height;

            // Target: 4096 CSS-px on the long side
            const aspect = origCW / (origCH || 1);
            var bigW, bigH;
            if (aspect >= 1) {
                bigW = 4096; bigH = Math.round(4096 / aspect);
            } else {
                bigH = 4096; bigW = Math.round(4096 * aspect);
            }

            // Hide the resize from the user
            container.parentElement.style.overflow = 'hidden';
            container.parentElement.style.height = (origCH || 600) + 'px';
            container.style.position = 'absolute';
            container.style.width = bigW + 'px';
            container.style.height = bigH + 'px';

            // Bump font stroke so it stays visible at the enlarged size
            var scaleFactor = bigW / (origCW || 800);
            network.setOptions({ nodes: { font: { strokeWidth: Math.round(4 * scaleFactor) } } });

            // Resize vis.js canvas, then wait for layout + paint
            network.setSize(bigW + 'px', bigH + 'px');
            network.fit();

            // Use setTimeout to let the browser finish layout, then
            // register the capture listener and trigger a fresh redraw.
            setTimeout(function() {
                var exported = false;
                network.once('afterDrawing', function(canvasCtx) {
                    if (exported) return;
                    exported = true;

                    try {
                        const srcCanvas = canvasCtx.canvas;
                        const pixW = srcCanvas.width;
                        const pixH = srcCanvas.height;

                        const exportCanvas = document.createElement('canvas');
                        exportCanvas.width = pixW;
                        exportCanvas.height = pixH;
                        const ctx = exportCanvas.getContext('2d');
                        ctx.drawImage(srcCanvas, 0, 0);

                        // Trigger download
                        var link = document.createElement('a');
                        link.download = 'data-lake-network.png';
                        link.href = exportCanvas.toDataURL('image/png');
                        link.click();
                    } catch(e) {
                        console.error('PNG export failed:', e);
                    }

                    // Restore font stroke and container size
                    network.setOptions({ nodes: { font: { strokeWidth: 4 } } });
                    container.style.width = origW;
                    container.style.height = origH;
                    container.style.position = origPos;
                    container.parentElement.style.overflow = origOverflow;
                    container.parentElement.style.height = origParentH;
                    network.setSize((origCW || 800) + 'px', (origCH || 600) + 'px');
                    network.redraw();
                    network.fit();
                });

                // Now trigger the redraw that the listener will capture
                network.redraw();
            }, 300);
        };
    }

    // Auto-cluster on load when checkbox is checked
    if (data.cluster_databases) {
        applyClusters();
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
    JS click-to-filter handler can identify node types.

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
        logger.warning("render_network called with empty DataFrame")
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
        "hover": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"},
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
            db_size = _log_scale(s.get("total", 0), global_min, global_max, 80, 200)
            tooltip = _build_tooltip(database, "Database", s, org_name, current_account)

            cluster_label = _assign_cluster(database)
            if cluster_label:
                cluster_members[cluster_label].append(database)

            nodes.append({
                "id": database, "label": database, "title": tooltip,
                "size": int(db_size), "color": transparent, "shape": "image",
                "shapeProperties": shape_props, "borderWidth": 0,
                "image": _node_images["database"],
                "nodeType": "database",
            })
            added_nodes.add(database)

        if not hide_warehouses and warehouse not in added_nodes:
            s = node_stats.get(warehouse, {})
            wh_size = _log_scale(s.get("total", 0), global_min, global_max, 80, 200)
            tooltip = _build_tooltip(warehouse, "Warehouse", s, org_name, current_account)
            nodes.append({
                "id": warehouse, "label": warehouse, "title": tooltip,
                "size": int(wh_size), "color": transparent, "shape": "image",
                "shapeProperties": shape_props, "borderWidth": 0,
                "image": _node_images["warehouse"],
                "nodeType": "warehouse",
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
                "nodeType": "client",
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

    # Build a plain {id: type} map so the JS glow handler can look up node
    # types without relying on vis.js DataSet preserving custom properties.
    node_type_map = {n["id"]: n["nodeType"] for n in nodes if "nodeType" in n}

    component_data = {
        "nodes": nodes,
        "edges": edges,
        "paths": paths,
        "clusters": clusters,
        "cluster_databases": cluster_databases,
        "fullscreen": fullscreen,
        "nodeTypeMap": node_type_map,
    }

    # --- Click-to-filter: trigger value + callback ---
    # The JS sends {nodeId, nodeType, _ts} or {action:'clear_all', _ts}
    # via setTriggerValue('selected_node', ...).  The on_change callback
    # fires *before* the page re-runs (before widgets are instantiated),
    # so it can safely write to widget keys in st.session_state.

    _NODETYPE_TO_FILTER = {
        "database": "persist_filter_database",
        "warehouse": "persist_filter_warehouse",
        "client": "persist_filter_client",
    }

    def _on_node_click():
        state = st.session_state.get("network_graph")
        if not state:
            return
        info = getattr(state, 'selected_node', None) or (
            state.get('selected_node') if hasattr(state, 'get') else None
        )
        if not info or not isinstance(info, dict):
            return
        # Deduplicate: only process if _ts is newer than last processed
        ts = info.get("_ts", 0)
        last_ts = st.session_state.get("_last_click_ts", 0)
        if ts <= last_ts:
            return
        st.session_state["_last_click_ts"] = ts

        if info.get("action") == "clear_all":
            for fk in _NODETYPE_TO_FILTER.values():
                st.session_state[fk] = []
                wk = fk.replace("persist_", "widget_")
                st.session_state[wk] = []
        else:
            node_type = info.get("nodeType", "")
            node_id = info.get("nodeId", "")
            filter_key = _NODETYPE_TO_FILTER.get(node_type)
            if filter_key and node_id:
                current = st.session_state.get(filter_key, [])
                if node_id not in current:
                    new_val = current + [node_id]
                else:
                    new_val = current
                st.session_state[filter_key] = new_val
                wk = filter_key.replace("persist_", "widget_")
                st.session_state[wk] = new_val

    mount = _get_component()
    height = "stretch"

    result = mount(
        data=component_data,
        on_selected_node_change=_on_node_click,
        height=height,
        key="network_graph",
    )

    return result
