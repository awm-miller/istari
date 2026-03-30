from __future__ import annotations

import html
import json


def render_html(data: dict) -> str:
    nodes_json = json.dumps(data["nodes"], ensure_ascii=False).replace("</", "<\\/")
    edges_json = json.dumps(data["edges"], ensure_ascii=False).replace("</", "<\\/")
    title = html.escape(str(data.get("seed_name") or "Istari"))
    node_count = len(data.get("nodes") or [])
    edge_count = len(data.get("edges") or [])

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{
  --bg: #0c0e14;
  --surface: #141820;
  --border: #1e2430;
  --text: #d0d4dc;
  --text-dim: #6b7385;
  --text-bright: #f0f2f5;
  --red: #e55561;
  --amber: #d4a017;
  --blue: #58a6ff;
  --green: #3fb950;
  --purple: #b382f0;
}}
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
  overflow: hidden;
  height: 100vh;
}}
.topbar {{
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 12px 20px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  z-index: 20;
  position: relative;
  flex-wrap: wrap;
}}
.topbar h1 {{
  font-size: 16px;
  font-weight: 700;
  color: var(--text-bright);
  white-space: nowrap;
}}
.topbar .stats {{
  font-size: 12px;
  color: var(--text-dim);
  white-space: nowrap;
}}
.search-box {{
  display: flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
}}
.search-box input {{
  width: 240px;
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid var(--border);
  background: var(--bg);
  color: var(--text);
  font-size: 13px;
  outline: none;
}}
.search-box input:focus {{ border-color: var(--blue); }}
.search-box .clear-btn {{
  background: none;
  border: none;
  color: var(--text-dim);
  cursor: pointer;
  font-size: 16px;
  padding: 2px 6px;
}}
.filters {{
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
}}
.toggle {{
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--text-dim);
  cursor: pointer;
  user-select: none;
  white-space: nowrap;
}}
.toggle input {{
  accent-color: var(--blue);
  cursor: pointer;
}}
.toggle input:checked + span {{ color: var(--blue); }}
.legend {{
  position: absolute;
  top: 58px;
  right: 12px;
  z-index: 15;
  display: flex;
  flex-direction: column;
  gap: 5px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 10px 14px;
  font-size: 11px;
}}
.legend .row {{ display: flex; align-items: center; gap: 6px; }}
.legend .dot {{
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}}
.legend .icon-chip {{
  width: 18px;
  height: 18px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  border: 1px solid rgba(255,255,255,0.18);
}}
.legend .icon-chip svg {{ width: 12px; height: 12px; overflow: visible; }}
.legend .icon-chip path {{
  fill: none;
  stroke: currentColor;
  stroke-width: 1.8;
  stroke-linecap: round;
  stroke-linejoin: round;
}}
.score-panel {{
  position: absolute;
  top: 212px;
  right: 12px;
  width: 320px;
  max-height: calc(100vh - 230px);
  overflow: auto;
  z-index: 15;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 12px 14px;
  box-shadow: 0 10px 24px rgba(0,0,0,0.35);
}}
.score-panel h2 {{
  font-size: 13px;
  font-weight: 700;
  color: var(--text-bright);
  margin-bottom: 6px;
}}
.score-panel p {{
  font-size: 11px;
  color: var(--text-dim);
  line-height: 1.45;
  margin-bottom: 10px;
}}
.score-list {{
  display: flex;
  flex-direction: column;
  gap: 8px;
}}
.score-item {{
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 8px;
  padding: 9px 10px;
  background: rgba(255,255,255,0.03);
}}
.score-item-title {{
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
  font-size: 12px;
  margin-bottom: 4px;
}}
.score-item-title strong {{
  color: var(--text-bright);
  font-weight: 600;
}}
.score-item-title span {{
  color: var(--blue);
  font-weight: 700;
  white-space: nowrap;
}}
.score-item-meta {{
  font-size: 11px;
  color: var(--text-dim);
}}
.score-empty {{
  font-size: 12px;
  color: var(--text-dim);
  padding: 4px 0 2px;
}}
#graph {{
  width: 100vw;
  height: calc(100vh - 58px);
  overflow: hidden;
}}
.tooltip {{
  position: fixed;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 16px;
  font-size: 12px;
  line-height: 1.6;
  pointer-events: none;
  z-index: 100;
  max-width: 380px;
  box-shadow: 0 8px 24px rgba(0,0,0,0.6);
  display: none;
}}
.tooltip strong {{ color: var(--text-bright); }}
.tooltip em {{ color: var(--green); font-style: normal; }}
.tooltip .dim {{ color: var(--text-dim); }}
.context-menu {{
  position: fixed;
  min-width: 220px;
  max-width: 280px;
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 10px;
  box-shadow: 0 12px 28px rgba(0,0,0,0.55);
  padding: 8px;
  z-index: 120;
  display: none;
}}
.context-menu-title {{
  font-size: 12px;
  font-weight: 700;
  color: var(--text-bright);
  margin-bottom: 4px;
}}
.context-menu-subtitle {{
  font-size: 11px;
  color: var(--text-dim);
  margin-bottom: 8px;
  text-transform: capitalize;
}}
.context-menu-item {{
  width: 100%;
  border: 0;
  background: rgba(255,255,255,0.04);
  color: var(--text);
  border-radius: 8px;
  padding: 10px 12px;
  text-align: left;
  cursor: pointer;
  font-size: 12px;
}}
.context-menu-item:hover {{
  background: rgba(88,166,255,0.15);
  color: var(--text-bright);
}}
.context-menu-empty {{
  font-size: 12px;
  color: var(--text-dim);
  padding: 6px 4px 2px;
}}
.focus-btn {{
  cursor: pointer;
}}
svg text {{ font-family: "Segoe UI", system-ui, -apple-system, sans-serif; }}
</style>
</head>
<body>
<div class="topbar">
  <h1>{title}</h1>
  <div class="stats" id="stats">{node_count} nodes, {edge_count} edges</div>
  <div class="filters">
    <label class="toggle">
      <input id="show-identities" type="checkbox" checked />
      <span>identities</span>
    </label>
    <label class="toggle">
      <input id="show-companies" type="checkbox" checked />
      <span>companies</span>
    </label>
    <label class="toggle">
      <input id="show-charities" type="checkbox" checked />
      <span>charities</span>
    </label>
    <label class="toggle">
      <input id="show-people" type="checkbox" checked />
      <span>people</span>
    </label>
    <label class="toggle">
      <input id="show-addresses" type="checkbox" checked />
      <span>addresses</span>
    </label>
    <label class="toggle">
      <input id="indirect-only" type="checkbox" />
      <span>indirect only</span>
    </label>
  </div>
  <div class="search-box">
    <input id="search" type="search" placeholder="Search and focus..." autocomplete="off" />
    <button class="clear-btn" id="clear-search">&times;</button>
  </div>
</div>
<div id="graph"></div>
<div class="legend" id="legend"></div>
<div class="score-panel" id="score-panel"></div>
<div class="tooltip" id="tooltip"></div>
<div class="context-menu" id="context-menu"></div>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script>
const allNodes = {nodes_json};
const allEdges = {edges_json}.filter(e => e.kind !== "shared_org" && e.kind !== "cross_seed");

const viewerState = {{
  focusedNodeIds: new Set(),
  searchQuery: "",
  hiddenNodeTypes: new Set(),
  indirectOnly: false,
}};

const container = document.getElementById("graph");
const tooltipEl = document.getElementById("tooltip");
const legendEl = document.getElementById("legend");
const scorePanelEl = document.getElementById("score-panel");
const statsEl = document.getElementById("stats");
const contextMenuEl = document.getElementById("context-menu");
const searchInput = document.getElementById("search");
const clearBtn = document.getElementById("clear-search");
const showIdentitiesInput = document.getElementById("show-identities");
const showCompaniesInput = document.getElementById("show-companies");
const showCharitiesInput = document.getElementById("show-charities");
const showPeopleInput = document.getElementById("show-people");
const showAddressesInput = document.getElementById("show-addresses");
const indirectOnlyInput = document.getElementById("indirect-only");

const W = container.clientWidth;
const H = container.clientHeight;
const LANE_Y = {{ 1: 140, 2: 360, 3: 620, 4: 840 }};
const nodeById = new Map(allNodes.map(n => [n.id, n]));
const visibleEdges = [];

const edgesByNodeId = new Map();
allEdges.forEach(edge => {{
  if (!edgesByNodeId.has(edge.source)) edgesByNodeId.set(edge.source, []);
  if (!edgesByNodeId.has(edge.target)) edgesByNodeId.set(edge.target, []);
  edgesByNodeId.get(edge.source).push(edge);
  edgesByNodeId.get(edge.target).push(edge);
}});
const directEdgePairs = new Set(
  allEdges.map(edge => {{
    const [a, b] = [edge.source, edge.target].sort();
    return `${{a}}||${{b}}`;
  }})
);
const orgLinkIds = new Map();
allEdges.filter(edge => edge.kind === "org_link").forEach(edge => {{
  if (!orgLinkIds.has(edge.source)) orgLinkIds.set(edge.source, new Set());
  if (!orgLinkIds.has(edge.target)) orgLinkIds.set(edge.target, new Set());
  orgLinkIds.get(edge.source).add(edge.target);
  orgLinkIds.get(edge.target).add(edge.source);
}});
const orgAddressIds = new Map();
const addressOrgIds = new Map();
allEdges.filter(edge => edge.kind === "address_link").forEach(edge => {{
  const sourceNode = nodeById.get(edge.source);
  const targetNode = nodeById.get(edge.target);
  const orgId = sourceNode?.kind === "organisation" ? edge.source : targetNode?.kind === "organisation" ? edge.target : null;
  const addressId = sourceNode?.kind === "address" ? edge.source : targetNode?.kind === "address" ? edge.target : null;
  if (!orgId || !addressId) return;
  if (!orgAddressIds.has(orgId)) orgAddressIds.set(orgId, new Set());
  orgAddressIds.get(orgId).add(addressId);
  if (!addressOrgIds.has(addressId)) addressOrgIds.set(addressId, new Set());
  addressOrgIds.get(addressId).add(orgId);
}});
const indirectIdentityIdsByOrg = new Map();
allNodes.filter(node => node.lane === 1).forEach(identity => {{
  const directOrgs = new Set();
  (edgesByNodeId.get(identity.id) || []).forEach(edge => {{
    if (edge.kind !== "role") return;
    const otherId = edge.source === identity.id ? edge.target : edge.source;
    if (nodeById.get(otherId)?.kind === "organisation") directOrgs.add(otherId);
  }});
  if (!directOrgs.size) return;
  const reachableOrgs = new Set();
  directOrgs.forEach(orgId => {{
    (orgLinkIds.get(orgId) || new Set()).forEach(id => reachableOrgs.add(id));
    (orgAddressIds.get(orgId) || new Set()).forEach(addressId => {{
      (addressOrgIds.get(addressId) || new Set()).forEach(id => reachableOrgs.add(id));
    }});
  }});
  directOrgs.forEach(id => reachableOrgs.delete(id));
  reachableOrgs.forEach(orgId => {{
    if (!indirectIdentityIdsByOrg.has(orgId)) indirectIdentityIdsByOrg.set(orgId, new Set());
    indirectIdentityIdsByOrg.get(orgId).add(identity.id);
  }});
}});

const svg = d3.select(container).append("svg")
  .attr("width", "100%")
  .attr("height", "100%")
  .style("display", "block");
const gRoot = svg.append("g");
const zoom = d3.zoom().scaleExtent([0.05, 6]).on("zoom", (event) => gRoot.attr("transform", event.transform));
svg.call(zoom);
svg.on("dblclick.zoom", null);

function iconPath(kind) {{
  if (kind === "identity") return "M12 12a3 3 0 1 0 0-6a3 3 0 0 0 0 6Zm-5.5 7a5.5 5.5 0 0 1 11 0";
  if (kind === "address") return "M12 21s-5-4.35-5-8.5a5 5 0 1 1 10 0C17 16.65 12 21 12 21Zm0-7a1.8 1.8 0 1 0 0-3.6A1.8 1.8 0 0 0 12 14Z";
  if (kind === "search") return "M11 18a7 7 0 1 1 4.95-2.05M16 16l4 4";
  if (kind === "charity") return "M12 20s-6-3.9-6-8.2A3.8 3.8 0 0 1 12 9a3.8 3.8 0 0 1 6 2.8C18 16.1 12 20 12 20Z";
  if (kind === "company") return "M4 20h16M6 20V9l4-3v14M14 20V5h4v15M8 11h.01M8 14h.01M8 17h.01M16 9h.01M16 12h.01M16 15h.01";
  if (kind === "organisation") return "M12 5v3M7 18h10M8 18l1.5-6h5L16 18M6 10h12";
  return "M12 12a3 3 0 1 0 0-6a3 3 0 0 0 0 6Zm-5.5 7a5.5 5.5 0 0 1 11 0";
}}

function iconSpec(kind) {{
  if (kind === "identity") return {{ fill: "var(--amber)", color: "#0f172a", path: iconPath("identity") }};
  if (kind === "address") return {{ fill: "var(--purple)", color: "#ffffff", path: iconPath("address") }};
  if (kind === "charity") return {{ fill: "var(--green)", color: "#ffffff", path: iconPath("charity") }};
  if (kind === "company") return {{ fill: "#0ea5e9", color: "#ffffff", path: iconPath("company") }};
  if (kind === "organisation") return {{ fill: "#475569", color: "#ffffff", path: iconPath("organisation") }};
  return {{ fill: "var(--blue)", color: "#ffffff", path: iconPath("person") }};
}}

function iconSvgMarkup(spec) {{
  return `<span class="icon-chip" style="background:${{spec.fill}};color:${{spec.color}}">
    <svg viewBox="0 0 24 24" aria-hidden="true"><path d="${{spec.path}}"></path></svg>
  </span>`;
}}

function escapeHtml(value) {{
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}}

function renderLegend() {{
  legendEl.innerHTML = [
    `<div class="row">${{iconSvgMarkup(iconSpec("identity"))}} Identity</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("charity"))}} Charity</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("company"))}} Company</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("organisation"))}} Other organisation</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("address"))}} Address</div>`,
    `<div class="row">${{iconSvgMarkup(iconSpec("person"))}} Person</div>`,
    `<div class="row"><span class="dot" style="background:#ff2222;border:2px solid #ff2222"></span> Sanctioned (OFAC)</div>`,
  ].join("");
}}
renderLegend();

function nodeTypeKey(node) {{
  if (node.kind === "seed" || node.lane === 1) return "identity";
  if (node.kind === "address") return "address";
  if (node.kind === "person") return "person";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "charity") return "charity";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "company") return "company";
  return "organisation";
}}

function isFilterableType(typeKey) {{
  return typeKey === "identity" || typeKey === "company" || typeKey === "charity" || typeKey === "address" || typeKey === "person";
}}

function nodeMatchesQuery(node, query) {{
  if (!query) return false;
  const q = query.toLowerCase();
  if ((node.label || "").toLowerCase().includes(q)) return true;
  return (node.aliases || []).some(alias => (alias || "").toLowerCase().includes(q));
}}

function nodeRankScore(node) {{
  const score = Number(node?.score || 0);
  return Number.isFinite(score) ? score : 0;
}}

function getMatchedNodeIds(query) {{
  if (!query) return new Set();
  return new Set(
    allNodes
      .filter(node => node.kind !== "seed" && nodeMatchesQuery(node, query))
      .map(node => node.id)
  );
}}

function collectConnectedSubgraph(rootIds) {{
  const reachableIds = new Set();
  const distances = new Map();
  const parents = new Map();
  const queue = [];

  rootIds.forEach(rootId => {{
    const rootNode = nodeById.get(rootId);
    if (!rootNode || rootNode.kind === "seed") return;
    reachableIds.add(rootId);
    distances.set(rootId, 0);
    queue.push(rootId);
  }});

  while (queue.length) {{
    const currentId = queue.shift();
    const currentDistance = distances.get(currentId) ?? 0;
    (edgesByNodeId.get(currentId) || []).forEach(edge => {{
      const nextId = edge.source === currentId ? edge.target : edge.source;
      const nextNode = nodeById.get(nextId);
      if (!nextNode || nextNode.kind === "seed") return;
      if (distances.has(nextId)) return;
      distances.set(nextId, currentDistance + 1);
      parents.set(nextId, currentId);
      reachableIds.add(nextId);
      queue.push(nextId);
    }});
  }}

  return {{ reachableIds, distances, parents }};
}}

function projectIndirectNodeIds(rootIds, subgraph) {{
  const indirectIds = new Set(
    [...subgraph.reachableIds].filter(id => (subgraph.distances.get(id) ?? 0) >= 2)
  );
  const projected = new Set(rootIds);
  if (!indirectIds.size) return projected;
  indirectIds.forEach(id => projected.add(id));
  return projected;
}}

function hiddenConnectionStepLine(edge) {{
  if (edge.tooltip) return edge.tooltip;
  const source = nodeById.get(edge.source);
  const target = nodeById.get(edge.target);
  return `${{source?.label || edge.source}} is linked to ${{target?.label || edge.target}}`;
}}

function hiddenNodeTypeLabel(node) {{
  if (!node) return "node";
  if (node.kind === "seed" || node.lane === 1) return "identity";
  if (node.kind === "address") return "address";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "charity") return "charity";
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "company") return "company";
  if (node.kind === "organisation") return "organisation";
  return "person";
}}

function hiddenConnectionTooltipLines(sourceId, targetId, hiddenNodeIds, pathEdges = []) {{
  const source = nodeById.get(sourceId);
  const target = nodeById.get(targetId);
  const hiddenNodes = hiddenNodeIds.map(id => nodeById.get(id)).filter(Boolean);
  const viaText = hiddenNodes.length === 1 ? "1 hidden node" : `${{hiddenNodes.length}} hidden nodes`;
  const lines = [
    `<strong>${{source?.label || sourceId}}</strong> connects to <strong>${{target?.label || targetId}}</strong> through ${{viaText}}.`,
  ];
  if (hiddenNodes.length) {{
    lines.push(
      `Hidden path: ${{hiddenNodes.map(node => `${{node.label}} <span class=\\"dim\\">(${{hiddenNodeTypeLabel(node)}})</span>`).join(" <span class=\\"dim\\">→</span> ")}}`
    );
  }}
  if (pathEdges.length) {{
    lines.push("<strong>How the connection works:</strong>");
    pathEdges.forEach(edge => lines.push(hiddenConnectionStepLine(edge)));
  }}
  return lines;
}}

function edgePairKey(a, b) {{
  return a < b ? `${{a}}||${{b}}` : `${{b}}||${{a}}`;
}}

function isBridgeStartNode(node) {{
  return !!node && node.kind === "organisation";
}}

function isBridgeTargetNode(node) {{
  return !!node && node.lane === 1;
}}

function findBridgeConnections(startId) {{
  const startNode = nodeById.get(startId);
  if (!isBridgeStartNode(startNode)) return [];
  const connections = new Map();
  const hiddenQueue = [];
  const visited = new Set([startId]);

  (edgesByNodeId.get(startId) || []).forEach(edge => {{
    const nextId = edge.source === startId ? edge.target : edge.source;
    if (visited.has(nextId)) return;
    visited.add(nextId);
    const nextNode = nodeById.get(nextId);
    if (nextNode && isBridgeTargetNode(nextNode)) {{
      if (!directEdgePairs.has(edgePairKey(startId, nextId))) {{
        connections.set(nextId, {{
          source: startId,
          target: nextId,
          kind: "hidden_connection",
          hops: 1,
          hiddenNodeIds: [nextId],
          pathEdges: [edge],
          tooltip_lines: hiddenConnectionTooltipLines(startId, nextId, [nextId], [edge]),
        }});
      }}
      return;
    }}
    if (!isBridgeStartNode(nextNode)) return;
    hiddenQueue.push({{ id: nextId, hops: 1, hiddenNodeIds: [nextId], pathEdges: [edge] }});
  }});

  while (hiddenQueue.length) {{
    const current = hiddenQueue.shift();
    (edgesByNodeId.get(current.id) || []).forEach(edge => {{
      const nextId = edge.source === current.id ? edge.target : edge.source;
      if (visited.has(nextId)) return;
      visited.add(nextId);
      const nextNode = nodeById.get(nextId);
      if (nextNode && isBridgeTargetNode(nextNode)) {{
        const existing = connections.get(nextId);
        if (!existing || current.hops + 1 < existing.hops) {{
          if (!directEdgePairs.has(edgePairKey(startId, nextId))) {{
            connections.set(nextId, {{
              source: startId,
              target: nextId,
              kind: "hidden_connection",
              hops: current.hops + 1,
              hiddenNodeIds: [...current.hiddenNodeIds, nextId],
              pathEdges: [...current.pathEdges, edge],
              tooltip_lines: hiddenConnectionTooltipLines(
                startId,
                nextId,
                current.hiddenNodeIds,
                [...current.pathEdges, edge],
              ),
            }});
          }}
        }}
        return;
      }}
      if (!isBridgeStartNode(nextNode)) return;
      hiddenQueue.push({{
        id: nextId,
        hops: current.hops + 1,
        hiddenNodeIds: [...current.hiddenNodeIds, nextId],
        pathEdges: [...current.pathEdges, edge],
      }});
    }});
  }}

  return [...connections.values()];
}}

function deriveVisibleBridgeEdges(visibleIds) {{
  const hiddenConnections = new Map();
  [...visibleIds].forEach(startId => {{
    const startNode = nodeById.get(startId);
    if (!isBridgeStartNode(startNode)) return;
    findBridgeConnections(startId).forEach(connection => {{
      if (!visibleIds.has(connection.target)) return;
      const pairKey = edgePairKey(connection.source, connection.target);
      const existing = hiddenConnections.get(pairKey);
      if (!existing || connection.hops < existing.hops) {{
        hiddenConnections.set(pairKey, connection);
      }}
    }});
  }});
  return [...hiddenConnections.values()];
}}

function deriveHiddenConnectionEdges(rootIds, visibleIds, subgraph) {{
  const hiddenConnections = new Map();
  [...visibleIds].forEach(targetId => {{
    if (rootIds.has(targetId)) return;
    let currentId = targetId;
    const hiddenNodeIds = [];
    while (subgraph.parents.has(currentId)) {{
      const parentId = subgraph.parents.get(currentId);
      if (visibleIds.has(parentId)) {{
        const pairKey = edgePairKey(parentId, targetId);
        if (!hiddenConnections.has(pairKey) && hiddenNodeIds.length) {{
          const orderedHiddenNodes = [...hiddenNodeIds].reverse();
          hiddenConnections.set(pairKey, {{
            source: parentId,
            target: targetId,
            kind: "hidden_connection",
            hiddenNodeIds: orderedHiddenNodes,
            tooltip_lines: hiddenConnectionTooltipLines(parentId, targetId, orderedHiddenNodes),
          }});
        }}
        break;
      }}
      hiddenNodeIds.push(parentId);
      currentId = parentId;
    }}
  }});
  return [...hiddenConnections.values()];
}}

function syncHiddenTypeState() {{
  viewerState.hiddenNodeTypes.clear();
  if (!showIdentitiesInput.checked) viewerState.hiddenNodeTypes.add("identity");
  if (!showCompaniesInput.checked) viewerState.hiddenNodeTypes.add("company");
  if (!showCharitiesInput.checked) viewerState.hiddenNodeTypes.add("charity");
  if (!showPeopleInput.checked) viewerState.hiddenNodeTypes.add("person");
  if (!showAddressesInput.checked) viewerState.hiddenNodeTypes.add("address");
  viewerState.indirectOnly = indirectOnlyInput.checked;
}}

function applyTypeFilters(visibleIds, rootIds, options = {{}}) {{
  if (!visibleIds.size) return new Set();

  const filteredIds = new Set(
    [...visibleIds].filter(id => {{
      const node = nodeById.get(id);
      if (!node || node.kind === "seed") return false;
      const typeKey = nodeTypeKey(node);
      if (!isFilterableType(typeKey)) return true;
      return !viewerState.hiddenNodeTypes.has(typeKey);
    }})
  );

  // When browsing the full graph without a focused/search root, keep the
  // remaining visible node types even if hiding their connectors makes them
  // appear disconnected.
  if (!rootIds.size) return filteredIds;

  if (viewerState.indirectOnly) return filteredIds;

  let changed = true;
  while (changed) {{
    changed = false;
    const degree = new Map();
    filteredIds.forEach(id => degree.set(id, 0));
    allEdges.forEach(edge => {{
      if (!filteredIds.has(edge.source) || !filteredIds.has(edge.target)) return;
      degree.set(edge.source, (degree.get(edge.source) || 0) + 1);
      degree.set(edge.target, (degree.get(edge.target) || 0) + 1);
    }});
    [...filteredIds].forEach(id => {{
      if (rootIds.has(id)) return;
      if (options.keepDisconnectedIdentities && (nodeById.get(id)?.lane === 1)) return;
      if ((degree.get(id) || 0) > 0) return;
      filteredIds.delete(id);
      changed = true;
    }});
  }}

  return filteredIds;
}}

function buildSearchProjection(matchedIds) {{
  const visibleIds = new Set();
  matchedIds.forEach(id => visibleIds.add(id));

  function walkLane(nodeId, visited, directionFn) {{
    if (visited.has(nodeId)) return;
    visited.add(nodeId);
    const node = nodeById.get(nodeId);
    if (!node || node.kind === "seed") return;
    visibleIds.add(nodeId);
    const nodeLane = node.lane ?? 0;
    (edgesByNodeId.get(nodeId) || []).forEach(edge => {{
      const otherId = edge.source === nodeId ? edge.target : edge.source;
      const otherNode = nodeById.get(otherId);
      if (!otherNode || otherNode.kind === "seed") return;
      const otherLane = otherNode.lane ?? 0;
      if (directionFn(otherLane, nodeLane)) walkLane(otherId, visited, directionFn);
    }});
  }}

  const peopleOnlySearch = matchedIds.size > 0 && [...matchedIds].every(id => nodeById.get(id)?.lane === 4);
  const upstreamVisited = new Set();
  const focusOrgIds = new Set();

  if (peopleOnlySearch) {{
    matchedIds.forEach(id => {{
      (edgesByNodeId.get(id) || []).forEach(edge => {{
        const otherId = edge.source === id ? edge.target : edge.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind !== "organisation") return;
        focusOrgIds.add(otherId);
        visibleIds.add(otherId);
        (edgesByNodeId.get(otherId) || []).forEach(orgEdge => {{
          if (orgEdge.kind !== "role") return;
          const nextId = orgEdge.source === otherId ? orgEdge.target : orgEdge.source;
          if (nodeById.get(nextId)?.lane === 1) visibleIds.add(nextId);
        }});
      }});
    }});
  }} else {{
    matchedIds.forEach(id => walkLane(id, upstreamVisited, (other, self) => other < self));
  }}

  const bridgeStartIds = peopleOnlySearch ? [...focusOrgIds] : [...matchedIds];
  bridgeStartIds.forEach(startId => {{
    findBridgeConnections(startId).forEach(connection => {{
      const node = nodeById.get(connection.target);
      if (!node) return;
      if (peopleOnlySearch && node.lane === 4) return;
      visibleIds.add(node.id);
    }});
  }});

  const downstreamVisited = new Set();
  matchedIds.forEach(id => walkLane(id, downstreamVisited, (other, self) => other > self));
  [...visibleIds]
    .map(id => nodeById.get(id))
    .filter(node => node?.kind === "organisation")
    .forEach(node => {{
      if (!peopleOnlySearch) {{
        walkLane(node.id, downstreamVisited, (other, self) => other > self);
        return;
      }}
      (edgesByNodeId.get(node.id) || []).forEach(edge => {{
        const otherId = edge.source === node.id ? edge.target : edge.source;
        if (nodeById.get(otherId)?.kind === "address") visibleIds.add(otherId);
      }});
    }});

  const filteredVisibleIds = applyTypeFilters(matchedIds.size ? visibleIds : new Set(), matchedIds, {{
    keepDisconnectedIdentities: true,
  }});
  const edgeIds = allEdges.filter(edge => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target));
  return {{
    rootIds: matchedIds,
    visibleIds: filteredVisibleIds,
    edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
  }};
}}

function buildIndirectOrgProjection() {{
  const qualifyingOrgIds = new Set();
  indirectIdentityIdsByOrg.forEach((identityIds, orgId) => {{
    if (identityIds.size >= 2) qualifyingOrgIds.add(orgId);
  }});

  const visibleIds = new Set(qualifyingOrgIds);
  qualifyingOrgIds.forEach(orgId => {{
    (edgesByNodeId.get(orgId) || []).forEach(edge => {{
      if (edge.kind !== "role") return;
      const otherId = edge.source === orgId ? edge.target : edge.source;
      if (nodeById.get(otherId)?.lane === 1) visibleIds.add(otherId);
    }});
    (indirectIdentityIdsByOrg.get(orgId) || new Set()).forEach(identityId => visibleIds.add(identityId));
  }});

  const filteredVisibleIds = applyTypeFilters(visibleIds, qualifyingOrgIds, {{
    keepDisconnectedIdentities: true,
  }});
  const edgeIds = allEdges.filter(edge => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target));
  return {{
    rootIds: qualifyingOrgIds,
    visibleIds: filteredVisibleIds,
    edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
  }};
}}

function projectVisibleGraph() {{
  syncHiddenTypeState();
  const matchedIds = getMatchedNodeIds(viewerState.searchQuery);
  const rootIds = matchedIds.size ? matchedIds : new Set(viewerState.focusedNodeIds);

  if (matchedIds.size) {{
    return buildSearchProjection(matchedIds);
  }}

  if (viewerState.indirectOnly) {{
    return buildIndirectOrgProjection();
  }}

  if (!rootIds.size) {{
    const visibleIds = applyTypeFilters(
      new Set(allNodes.filter(node => node.kind !== "seed").map(node => node.id)),
      new Set()
    );
    const edgeIds = allEdges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
    return {{ rootIds, visibleIds, edgeIds }};
  }}

  const subgraph = collectConnectedSubgraph(rootIds);
  const visibleIds = applyTypeFilters(new Set(subgraph.reachableIds), rootIds);
  const edgeIds = allEdges.filter(edge => visibleIds.has(edge.source) && visibleIds.has(edge.target));
  return {{ rootIds, visibleIds, edgeIds }};
}}

const measureCtx = document.createElement("canvas").getContext("2d");
function textWidth(text, size) {{
  measureCtx.font = `${{size}}px "Segoe UI", system-ui, sans-serif`;
  return measureCtx.measureText(text).width;
}}

function fontSize(node) {{
  if (node.kind === "seed_alias") return 12;
  return 10.5;
}}

function pillHeight(node) {{
  return fontSize(node) + 12;
}}

function badgeSpec(node) {{
  if (node.kind === "seed_alias") return iconSpec("identity");
  if (node.kind === "address") return iconSpec("address");
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "charity") return iconSpec("charity");
  if (node.kind === "organisation" && (node.registry_type || "").toLowerCase() === "company") return iconSpec("company");
  if (node.kind === "organisation") return iconSpec("organisation");
  return iconSpec("person");
}}

function badgeWidth(node) {{
  return badgeSpec(node) ? 18 : 0;
}}

function badgeHeight(node) {{
  return Math.max(14, pillHeight(node) - 6);
}}

function badgeTextInset(node) {{
  return badgeSpec(node) ? 34 : 16;
}}

function focusButtonWidth(node) {{
  return node.kind === "seed" ? 0 : 24;
}}

function pillWidth(node) {{
  return badgeWidth(node) + textWidth(node.label || "", fontSize(node)) + 32 + focusButtonWidth(node);
}}

function nodeColor(node) {{
  if (node.sanctioned) return "#ff2222";
  if (node.kind === "seed_alias") return "var(--amber)";
  if (node.kind === "organisation") return "var(--green)";
  if (node.kind === "address") return "var(--purple)";
  return "var(--blue)";
}}

function edgeStroke(edge) {{
  if (edge.kind === "hidden_connection") return "#94a3b8";
  if (edge.kind === "alias") return "var(--amber)";
  if (edge.kind === "org_link") return "var(--green)";
  if (edge.kind === "address_link") return "var(--purple)";
  const roleType = (edge.role_type || "").toLowerCase();
  if (roleType.includes("trustee")) return "var(--blue)";
  if (roleType.includes("director")) return "var(--purple)";
  if (roleType.includes("secretary")) return "#0ea5e9";
  return "#2a3040";
}}

function showTooltip(event, lines) {{
  tooltipEl.innerHTML = lines.join("<br>");
  tooltipEl.style.display = "block";
  positionTooltip(event);
}}

function positionTooltip(event) {{
  const pad = 14;
  let x = event.clientX + pad;
  let y = event.clientY - 10;
  const rect = tooltipEl.getBoundingClientRect();
  if (x + rect.width > window.innerWidth - 10) x = event.clientX - rect.width - pad;
  if (y + rect.height > window.innerHeight - 10) y = window.innerHeight - rect.height - 10;
  tooltipEl.style.left = x + "px";
  tooltipEl.style.top = y + "px";
}}

function hideTooltip() {{
  tooltipEl.style.display = "none";
}}

function registryActionForNode(node) {{
  const registryType = String(node?.registry_type || "").toLowerCase();
  const registryNumber = String(node?.registry_number || "").trim();
  if (!registryType || !registryNumber) return null;
  if (registryType === "company") {{
    return {{
      label: "Open Companies House page",
      url: `https://find-and-update.company-information.service.gov.uk/company/${{encodeURIComponent(registryNumber)}}`,
    }};
  }}
  if (registryType === "charity") {{
    return {{
      label: "Open Charity Commission page",
      url: `https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/${{encodeURIComponent(registryNumber)}}`,
    }};
  }}
  return null;
}}

function evidenceActionUrl(evidence) {{
  const documentUrl = String(evidence?.document_url || "").trim();
  if (!documentUrl) return "";
  const pageNumber = Number(evidence?.page_number || 0);
  if (!pageNumber || documentUrl.includes("#") || !/\\.pdf($|[?#])/i.test(documentUrl)) return documentUrl;
  return `${{documentUrl}}#page=${{pageNumber}}`;
}}

function evidenceActionsForNode(node) {{
  const seen = new Set();
  const actions = [];
  (edgesByNodeId.get(node.id) || []).forEach(edge => {{
    const evidence = edge?.evidence;
    const url = evidenceActionUrl(evidence);
    if (!url) return;
    const title = String(evidence?.title || edge.tooltip || "Evidence").trim();
    const pageHint = String(evidence?.page_hint || "").trim();
    const key = `${{url}}||${{title}}||${{pageHint}}`;
    if (seen.has(key)) return;
    seen.add(key);
    actions.push({{
      label: pageHint ? `Open evidence: ${{title}} (${{pageHint}})` : `Open evidence: ${{title}}`,
      url,
    }});
  }});
  return actions.slice(0, 6);
}}

function closeContextMenu() {{
  contextMenuEl.style.display = "none";
  contextMenuEl.innerHTML = "";
  contextMenuEl._actions = [];
}}

function openContextMenu(event, node) {{
  event.preventDefault();
  event.stopPropagation();
  hideTooltip();

  const actions = [];
  const registryAction = registryActionForNode(node);
  if (registryAction) actions.push(registryAction);
  evidenceActionsForNode(node).forEach(action => actions.push(action));

  contextMenuEl._actions = actions;
  contextMenuEl.innerHTML = [
    `<div class="context-menu-title">${{escapeHtml(node.label || "Node")}}</div>`,
    `<div class="context-menu-subtitle">${{escapeHtml(nodeTypeKey(node))}}</div>`,
    actions.length
      ? actions
          .map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${{index}}">${{escapeHtml(action.label)}}</button>`)
          .join("")
      : `<div class="context-menu-empty">No external links are available for this node yet.</div>`,
  ].join("");
  contextMenuEl.style.display = "block";

  const rect = contextMenuEl.getBoundingClientRect();
  const maxLeft = window.innerWidth - rect.width - 10;
  const maxTop = window.innerHeight - rect.height - 10;
  contextMenuEl.style.left = Math.max(10, Math.min(event.clientX, maxLeft)) + "px";
  contextMenuEl.style.top = Math.max(10, Math.min(event.clientY, maxTop)) + "px";
}}

function layoutRow(nodes, yTop, xMin, xMax) {{
  if (!nodes.length) return 0;
  const spacing = 16;
  const rowGap = 18;
  const pad = 18;
  const usableMin = xMin + pad;
  const usableMax = xMax - pad;
  const maxRowW = Math.max(120, usableMax - usableMin);

  const rows = [];
  let currentRow = [];
  let currentWidth = 0;
  nodes.forEach(node => {{
    const width = pillWidth(node);
    const nextWidth = currentRow.length ? currentWidth + spacing + width : width;
    if (currentRow.length && nextWidth > maxRowW) {{
      rows.push(currentRow);
      currentRow = [node];
      currentWidth = width;
    }} else {{
      currentRow.push(node);
      currentWidth = nextWidth;
    }}
  }});
  if (currentRow.length) rows.push(currentRow);

  const rowStep = Math.max(...rows.flat().map(pillHeight)) + rowGap;
  rows.forEach((row, index) => {{
    const rowW = row.reduce((sum, node) => sum + pillWidth(node), 0) + spacing * (row.length - 1);
    let cx = usableMin + Math.max(0, (maxRowW - rowW) / 2);
    const rowY = yTop + index * rowStep;
    row.forEach(node => {{
      const width = pillWidth(node);
      node.x = cx + width / 2;
      node.y = rowY;
      cx += width + spacing;
    }});
  }});
  return rows.length * rowStep;
}}

function avgNeighborX(node) {{
  const xs = [];
  (edgesByNodeId.get(node.id) || []).forEach(edge => {{
    if (!visibleEdges.includes(edge)) return;
    const otherId = edge.source === node.id ? edge.target : edge.source;
    const other = nodeById.get(otherId);
    if (other && other._visible && other.x != null && other.lane !== node.lane) xs.push(other.x);
  }});
  if (!xs.length) return W / 2;
  return xs.reduce((sum, value) => sum + value, 0) / xs.length;
}}

function sortByNeighborX(nodes) {{
  return nodes.sort((left, right) => avgNeighborX(left) - avgNeighborX(right));
}}

function positionNodes() {{
  const visible = allNodes.filter(node => node._visible);
  let curY = 70;
  [1, 2, 3, 4].forEach(lane => {{
    const laneNodes = visible.filter(node => node.lane === lane);
    if (lane === 1 || lane === 4) {{
      laneNodes.sort((left, right) => {{
        const scoreDiff = nodeRankScore(right) - nodeRankScore(left);
        if (scoreDiff !== 0) return scoreDiff;
        return avgNeighborX(left) - avgNeighborX(right);
      }});
    }} else if (lane > 1) {{
      sortByNeighborX(laneNodes);
    }}
    LANE_Y[lane] = curY;
    const height = layoutRow(laneNodes, curY, 0, W);
    curY += Math.max(height, 30) + 50;
  }});
}}

function updatePositions() {{
  edgeGroup.selectAll("line")
    .attr("x1", edge => nodeById.get(edge.source)?.x ?? 0)
    .attr("y1", edge => nodeById.get(edge.source)?.y ?? 0)
    .attr("x2", edge => nodeById.get(edge.target)?.x ?? 0)
    .attr("y2", edge => nodeById.get(edge.target)?.y ?? 0);
  pills.attr("transform", node => `translate(${{node.x - pillWidth(node) / 2}},${{node.y - pillHeight(node) / 2}})`);
}}

function isNodeDisplayed(node) {{
  return !!node && !!node._visible;
}}

function renderEdges() {{
  const groups = edgeGroup.selectAll("g.edge-group")
    .data(visibleEdges, edge => `${{edge.kind}}:${{edge.source}}:${{edge.target}}:${{edge.tooltip || ""}}:${{(edge.hiddenNodeIds || []).join("|")}}`)
    .join(
      enter => {{
        const group = enter.append("g").attr("class", "edge-group");
        group.append("line").attr("class", "role-edge-hit")
          .attr("stroke", "transparent")
          .attr("stroke-width", 12)
          .style("pointer-events", "stroke");
        group.append("line").attr("class", "role-edge")
          .style("pointer-events", "none");
        return group;
      }},
      update => update,
      exit => exit.remove()
    );

  groups.select("line.role-edge")
    .attr("stroke", edgeStroke)
    .attr("stroke-width", edge => edge.kind === "hidden_connection" ? 1.8 : edge.kind === "alias" ? 2.5 : 1.4 + (edge.weight || 0) * 1.5)
    .attr("stroke-opacity", edge => edge.kind === "hidden_connection" ? 0.7 : edge.kind === "alias" ? 0.8 : edge.kind === "address_link" ? 0.75 : 0.45)
    .attr("stroke-dasharray", edge => edge.kind === "hidden_connection" ? "5 4" : null)
    .style("pointer-events", "none");

  groups.select("line.role-edge-hit")
    .on("mouseover", (event, edge) => showTooltip(event, edge.tooltip_lines || [edge.tooltip || "link"]))
    .on("mousemove", positionTooltip)
    .on("mouseout", hideTooltip);
}}

function syncVisibility() {{
  pills
    .attr("display", node => isNodeDisplayed(node) ? null : "none")
    .attr("opacity", node => isNodeDisplayed(node) ? 1 : 0);
  renderEdges();
  updatePositions();
}}

function zoomToVisible() {{
  const visibleNodes = allNodes.filter(isNodeDisplayed);
  if (!visibleNodes.length) return;
  const xs = visibleNodes.map(node => node.x);
  const ys = visibleNodes.map(node => node.y);
  const bounds = {{
    x0: Math.min(...xs) - 60,
    x1: Math.max(...xs) + 60,
    y0: Math.min(...ys) - 40,
    y1: Math.max(...ys) + 40,
  }};
  const bw = Math.max(1, bounds.x1 - bounds.x0);
  const bh = Math.max(1, bounds.y1 - bounds.y0);
  const scale = Math.min(W / bw, H / bh, 1.5) * 0.85;
  const tx = (W - bw * scale) / 2 - bounds.x0 * scale;
  const ty = (H - bh * scale) / 2 - bounds.y0 * scale;
  svg.call(zoom.transform, d3.zoomIdentity.translate(tx, ty).scale(scale));
}}

const edgeGroup = gRoot.append("g");
const nodeGroup = gRoot.append("g");
const pills = nodeGroup.selectAll("g.pill").data(allNodes).join("g")
  .attr("class", "pill")
  .style("cursor", node => node.kind === "seed" ? "default" : "pointer");

pills.append("rect")
  .attr("rx", node => pillHeight(node) / 2)
  .attr("ry", node => pillHeight(node) / 2)
  .attr("width", pillWidth)
  .attr("height", pillHeight)
  .attr("fill", nodeColor)
  .attr("fill-opacity", node => node.sanctioned ? 0.35 : 0.18)
  .attr("stroke", nodeColor)
  .attr("stroke-width", node => node.sanctioned ? 2.5 : 1.2)
  .attr("stroke-opacity", node => node.sanctioned ? 1.0 : 0.7);

pills.append("text")
  .text(node => node.label)
  .attr("font-size", fontSize)
  .attr("font-weight", node => node.kind === "seed_alias" ? 600 : 400)
  .attr("fill", node => node.kind === "seed_alias" ? "var(--text-bright)" : "var(--text)")
  .attr("text-anchor", "start")
  .attr("dominant-baseline", "central")
  .attr("x", badgeTextInset)
  .attr("y", node => pillHeight(node) / 2)
  .style("pointer-events", "none");

const badgeGroups = pills.append("g")
  .style("display", node => badgeSpec(node) ? null : "none");

badgeGroups.append("rect")
  .attr("rx", node => badgeHeight(node) / 2)
  .attr("ry", node => badgeHeight(node) / 2)
  .attr("x", 8)
  .attr("y", node => (pillHeight(node) - badgeHeight(node)) / 2)
  .attr("width", badgeWidth)
  .attr("height", badgeHeight)
  .attr("fill", node => badgeSpec(node)?.fill || "transparent")
  .attr("stroke", "rgba(255,255,255,0.18)")
  .attr("stroke-width", 0.8);

badgeGroups.append("path")
  .attr("d", node => badgeSpec(node)?.path || "")
  .attr("transform", node => {{
    const size = 12;
    const x = 11;
    const y = (pillHeight(node) - size) / 2;
    return `translate(${{x}},${{y}}) scale(0.5)`;
  }})
  .attr("fill", "none")
  .attr("stroke", node => badgeSpec(node)?.color || "transparent")
  .attr("stroke-width", 1.8)
  .attr("stroke-linecap", "round")
  .attr("stroke-linejoin", "round")
  .style("pointer-events", "none");

const focusButtons = pills.append("g")
  .attr("class", "focus-button-group")
  .style("display", node => node.kind === "seed" ? "none" : null);

focusButtons.append("circle")
  .attr("class", "focus-btn")
  .attr("cx", node => pillWidth(node) - 14)
  .attr("cy", node => pillHeight(node) / 2)
  .attr("r", 8)
  .attr("fill", "rgba(255,255,255,0.08)")
  .attr("stroke", "rgba(255,255,255,0.28)")
  .attr("stroke-width", 1);

focusButtons.append("path")
  .attr("class", "focus-btn")
  .attr("d", iconPath("search"))
  .attr("transform", node => `translate(${{pillWidth(node) - 20}},${{pillHeight(node) / 2 - 6}}) scale(0.5)`)
  .attr("fill", "none")
  .attr("stroke", "#ffffff")
  .attr("stroke-width", 1.8)
  .attr("stroke-linecap", "round")
  .attr("stroke-linejoin", "round");

focusButtons
  .on("mouseover", (event, node) => showTooltip(event, [`Search for ${{node.label}}`]))
  .on("mousemove", positionTooltip)
  .on("mouseout", hideTooltip)
  .on("click", (event, node) => {{
    event.stopPropagation();
    searchInput.value = node.label || "";
    viewerState.searchQuery = (node.label || "").trim();
    viewerState.focusedNodeIds.clear();
    applyViewerState();
  }});

pills
  .on("mouseover", (event, node) => showTooltip(event, node.tooltip_lines || [node.label]))
  .on("mousemove", positionTooltip)
  .on("mouseout", hideTooltip)
  .on("contextmenu", openContextMenu);

contextMenuEl.addEventListener("click", (event) => {{
  const button = event.target.closest("[data-action-index]");
  if (!button) return;
  const index = Number(button.getAttribute("data-action-index"));
  const action = Array.isArray(contextMenuEl._actions) ? contextMenuEl._actions[index] : null;
  if (!action?.url) return;
  window.open(action.url, "_blank", "noopener,noreferrer");
  closeContextMenu();
}});

document.addEventListener("click", closeContextMenu);
window.addEventListener("resize", closeContextMenu);
window.addEventListener("blur", closeContextMenu);

const drag = d3.drag()
  .filter(event => !(event.target.classList && event.target.classList.contains("focus-btn")))
  .on("start", (event, node) => {{
    node._dragging = true;
  }})
  .on("drag", (event, node) => {{
    node.x = event.x;
    node.y = event.y;
    updatePositions();
  }})
  .on("end", (event, node) => {{
    node._dragging = false;
  }});
pills.call(drag);

svg.on("dblclick.focus", () => {{
  if (!viewerState.focusedNodeIds.size) return;
  viewerState.focusedNodeIds.clear();
  applyViewerState();
}});

function updateFocusStyling(rootIds) {{
  pills.select("rect")
    .attr("stroke-width", node => {{
      if (node.sanctioned) return rootIds.has(node.id) ? 3.2 : 2.5;
      return rootIds.has(node.id) ? 2.8 : 1.2;
    }})
    .attr("stroke-opacity", node => rootIds.has(node.id) ? 1.0 : node.sanctioned ? 1.0 : 0.7)
    .attr("fill-opacity", node => rootIds.has(node.id) ? 0.28 : node.sanctioned ? 0.35 : 0.18);
}}

function renderScorePanel() {{
  const rankedNodes = allNodes
    .filter(node => node._visible && nodeRankScore(node) > 0 && (node.lane === 1 || node.lane === 4))
    .sort((left, right) => {{
      const scoreDiff = nodeRankScore(right) - nodeRankScore(left);
      if (scoreDiff !== 0) return scoreDiff;
      const orgDiff = Number(right.org_count || 0) - Number(left.org_count || 0);
      if (orgDiff !== 0) return orgDiff;
      return String(left.label || "").localeCompare(String(right.label || ""));
    }})
    .slice(0, 12);

  const body = rankedNodes.length
    ? `<div class="score-list">${{rankedNodes.map(node => `
        <div class="score-item">
          <div class="score-item-title">
            <strong>${{escapeHtml(node.label || "Unknown")}}</strong>
            <span>${{nodeRankScore(node).toFixed(2)}}</span>
          </div>
          <div class="score-item-meta">${{Number(node.org_count || 0)}} orgs, ${{Number(node.role_count || 0)}} roles</div>
        </div>
      `).join("")}}</div>`
    : `<div class="score-empty">No scored identity or person nodes are currently visible.</div>`;

  scorePanelEl.innerHTML = `
    <h2>Top ranked on screen</h2>
    <p>Score is the current graph ranking signal. It grows as a person or identity picks up more and stronger weighted links across connected organisations and roles.</p>
    ${{body}}
  `;
}}

function applyViewerState() {{
  const projection = projectVisibleGraph();
  allNodes.forEach(node => {{
    node._visible = projection.visibleIds.has(node.id);
  }});

  visibleEdges.length = 0;
  projection.edgeIds.forEach(edge => visibleEdges.push(edge));

  positionNodes();
  syncVisibility();
  updateFocusStyling(projection.rootIds);
  renderScorePanel();
  zoomToVisible();

  const shownNodes = allNodes.filter(node => node._visible).length;
  statsEl.textContent = projection.rootIds.size
    ? `showing ${{shownNodes}} nodes, ${{visibleEdges.length}} edges`
    : `{node_count} nodes, {edge_count} edges`;
}}

searchInput.addEventListener("input", () => {{
  viewerState.searchQuery = searchInput.value.trim();
  if (viewerState.searchQuery) viewerState.focusedNodeIds.clear();
  applyViewerState();
}});

clearBtn.addEventListener("click", () => {{
  searchInput.value = "";
  viewerState.searchQuery = "";
  viewerState.focusedNodeIds.clear();
  applyViewerState();
  searchInput.focus();
}});

showIdentitiesInput.addEventListener("change", applyViewerState);
showCompaniesInput.addEventListener("change", applyViewerState);
showCharitiesInput.addEventListener("change", applyViewerState);
showPeopleInput.addEventListener("change", applyViewerState);
showAddressesInput.addEventListener("change", applyViewerState);
indirectOnlyInput.addEventListener("change", applyViewerState);

applyViewerState();
</script>
</body>
</html>"""
