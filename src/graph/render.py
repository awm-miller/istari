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
<link
  rel="stylesheet"
  href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css"
  integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
  crossorigin=""
>
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
.toolbar-btn {{
  border: 1px solid var(--border);
  background: var(--bg);
  color: var(--text);
  border-radius: 6px;
  padding: 6px 10px;
  font-size: 12px;
  cursor: pointer;
}}
.toolbar-btn:hover {{
  border-color: var(--blue);
  color: var(--text-bright);
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
.legend {{
  display: flex;
  flex-direction: column;
  gap: 14px;
  font-size: 13px;
}}
.legend .row {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  padding: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
}}
.legend .legend-key {{
  display: inline-flex;
  align-items: center;
  gap: 12px;
}}
.legend .legend-toggle {{
  accent-color: var(--blue);
  cursor: pointer;
}}
.compact-legend {{
  position: fixed;
  top: 74px;
  right: 12px;
  z-index: 15;
  padding: 12px;
  border: 1px solid var(--border);
  border-radius: 14px;
  background: rgba(20, 24, 32, 0.92);
  box-shadow: 0 18px 40px rgba(0,0,0,0.38);
  backdrop-filter: blur(16px);
  display: none;
}}
.compact-legend.visible {{
  display: flex;
}}
.compact-legend .row {{
  justify-content: flex-start;
}}
.legend .dot {{
  width: 12px;
  height: 12px;
  border-radius: 50%;
  flex-shrink: 0;
}}
.legend .icon-chip {{
  width: 20px;
  height: 20px;
  border-radius: 999px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  border: 1px solid rgba(255,255,255,0.18);
}}
.legend .icon-chip svg {{ width: 13px; height: 13px; overflow: visible; }}
.legend .icon-chip path {{
  fill: none;
  stroke: currentColor;
  stroke-width: 1.8;
  stroke-linecap: round;
  stroke-linejoin: round;
}}
.score-panel {{
  width: 100%;
  background: transparent;
  border: 0;
  border-radius: 0;
  padding: 0;
  box-shadow: none;
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
.modal-backdrop {{
  position: fixed;
  inset: 0;
  background: rgba(8, 10, 16, 0.75);
  z-index: 140;
  display: none;
  align-items: center;
  justify-content: center;
  padding: 24px;
}}
.modal-backdrop.open {{
  display: flex;
}}
.modal-card {{
  width: min(920px, calc(100vw - 48px));
  height: min(700px, calc(100vh - 48px));
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 14px;
  box-shadow: 0 18px 50px rgba(0,0,0,0.5);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}}
.modal-header {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 14px 16px 12px;
  border-bottom: 1px solid var(--border);
}}
.modal-header h2 {{
  font-size: 14px;
  color: var(--text-bright);
}}
.modal-close {{
  border: 0;
  background: rgba(255,255,255,0.06);
  color: var(--text);
  border-radius: 8px;
  padding: 8px 10px;
  cursor: pointer;
  font-size: 12px;
}}
.modal-status {{
  padding: 10px 16px;
  font-size: 12px;
  color: var(--text-dim);
  border-bottom: 1px solid rgba(255,255,255,0.05);
}}
.analysis-body {{
  padding: 16px;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 14px;
}}
.analysis-selection {{
  font-size: 12px;
  color: var(--text-dim);
}}
.analysis-text {{
  font-size: 13px;
  line-height: 1.6;
  color: var(--text);
}}
.analysis-path {{
  display: flex;
  flex-direction: column;
  gap: 8px;
}}
.analysis-path-item {{
  font-size: 12px;
  color: var(--text-dim);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 8px;
  padding: 8px 10px;
  background: rgba(255,255,255,0.03);
}}
.analysis-claims {{
  display: flex;
  flex-direction: column;
  gap: 10px;
}}
.analysis-claim {{
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 8px;
  padding: 10px;
  background: rgba(255,255,255,0.03);
}}
.analysis-claim-text {{
  font-size: 12px;
  color: var(--text);
  margin-bottom: 6px;
}}
.analysis-claim-evidence a {{
  font-size: 11px;
  color: var(--blue);
  text-decoration: none;
  margin-right: 8px;
}}
.tools-body {{
  padding: 16px;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 16px;
}}
.tools-actions {{
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}}
.tools-actions .toolbar-btn {{
  min-width: 150px;
}}
.viewer-sidebar {{
  position: fixed;
  top: 74px;
  right: 12px;
  bottom: 12px;
  width: min(360px, calc(100vw - 24px));
  z-index: 18;
  display: flex;
  flex-direction: column;
  background: rgba(20, 24, 32, 0.92);
  border: 1px solid var(--border);
  border-radius: 18px;
  box-shadow: 0 18px 50px rgba(0,0,0,0.45);
  backdrop-filter: blur(18px);
  transform: translateX(calc(100% + 24px));
  transition: transform 280ms cubic-bezier(0.22, 1, 0.36, 1), box-shadow 280ms ease;
  overflow: hidden;
}}
.viewer-sidebar.open {{
  transform: translateX(0);
  box-shadow: 0 22px 60px rgba(0,0,0,0.55);
}}
.sidebar-handle {{
  position: fixed;
  top: 50%;
  right: 12px;
  width: 46px;
  height: 72px;
  border: 1px solid var(--border);
  border-radius: 16px;
  background: rgba(20, 24, 32, 0.92);
  color: var(--text-bright);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  box-shadow: 0 14px 30px rgba(0,0,0,0.35);
  transition: right 280ms cubic-bezier(0.22, 1, 0.36, 1), color 180ms ease, background 180ms ease, transform 180ms ease;
  z-index: 19;
  transform: translateY(-50%);
}}
.sidebar-handle:hover {{
  color: var(--blue);
  transform: translateY(-50%) scale(1.02);
}}
.sidebar-handle.open {{
  right: calc(min(360px, calc(100vw - 24px)) + 24px);
}}
.sidebar-handle svg {{
  width: 20px;
  height: 20px;
  overflow: visible;
}}
.sidebar-handle path {{
  fill: none;
  stroke: currentColor;
  stroke-width: 2.2;
  stroke-linecap: round;
  stroke-linejoin: round;
  transition: transform 280ms cubic-bezier(0.22, 1, 0.36, 1);
  transform-origin: center;
}}
.sidebar-handle.open path {{
  transform: rotate(180deg);
}}
.sidebar-header {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 18px 18px 10px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
}}
.sidebar-header h2 {{
  font-size: 15px;
  color: var(--text-bright);
}}
.sidebar-tabs {{
  display: flex;
  gap: 8px;
  padding: 0 16px 12px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
}}
.sidebar-tab {{
  flex: 1 1 0;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.03);
  color: var(--text-dim);
  border-radius: 10px;
  padding: 8px 6px;
  font-size: 11px;
  cursor: pointer;
  transition: color 160ms ease, border-color 160ms ease, background 160ms ease;
}}
.sidebar-tab:hover {{
  color: var(--text-bright);
  border-color: rgba(88,166,255,0.3);
}}
.sidebar-tab.active {{
  color: var(--text-bright);
  background: rgba(88,166,255,0.12);
  border-color: rgba(88,166,255,0.45);
}}
.sidebar-body {{
  flex: 1;
  overflow: auto;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}}
.sidebar-section {{
  display: flex;
  flex-direction: column;
  gap: 12px;
}}
.sidebar-pane {{
  display: none;
  flex-direction: column;
  gap: 12px;
}}
.sidebar-pane.active {{
  display: flex;
}}
.sidebar-pane-title {{
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--text-dim);
}}
.sidebar-meta-toggle {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  font-size: 12px;
  color: var(--text-dim);
  padding: 8px 10px;
  border-radius: 10px;
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
}}
.sidebar-meta-toggle input {{
  accent-color: var(--blue);
}}
.sidebar-body::-webkit-scrollbar,
.analysis-body::-webkit-scrollbar {{
  width: 10px;
}}
.sidebar-body::-webkit-scrollbar-track,
.analysis-body::-webkit-scrollbar-track {{
  background: rgba(255,255,255,0.04);
  border-radius: 999px;
}}
.sidebar-body::-webkit-scrollbar-thumb,
.analysis-body::-webkit-scrollbar-thumb {{
  background: linear-gradient(180deg, rgba(88,166,255,0.55), rgba(179,130,240,0.55));
  border-radius: 999px;
  border: 2px solid rgba(20,24,32,0.92);
}}
.analysis-toolbar {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}}
.analysis-toolbar .toolbar-btn {{
  flex-shrink: 0;
}}
.analysis-toolbar .analysis-status {{
  flex: 1;
}}
.map-toolbar {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
}}
.map-refresh-btn {{
  min-width: 0;
  width: 36px;
  height: 36px;
  padding: 0;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}}
.map-refresh-btn svg {{
  width: 16px;
  height: 16px;
}}
.map-refresh-btn path {{
  fill: none;
  stroke: currentColor;
  stroke-width: 2;
  stroke-linecap: round;
  stroke-linejoin: round;
}}
.analysis-status {{
  font-size: 12px;
  color: var(--text-dim);
}}
#address-map {{
  flex: 1;
  min-height: 360px;
  border-radius: 12px;
  overflow: hidden;
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
  <div class="search-box">
    <input id="search" type="search" placeholder="Search and focus..." autocomplete="off" />
  </div>
</div>
<div class="legend compact-legend" id="compact-legend"></div>
<button class="sidebar-handle open" id="toggle-sidebar" type="button" aria-label="Hide tools sidebar" title="Hide tools sidebar">
  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M15 6l-6 6l6 6"></path></svg>
</button>
<aside class="viewer-sidebar open" id="viewer-sidebar">
  <div class="sidebar-header">
    <h2>Tools</h2>
  </div>
  <div class="sidebar-tabs">
    <button class="sidebar-tab active" data-tab="legend" type="button">Filter</button>
    <button class="sidebar-tab" data-tab="map" type="button">Map</button>
    <button class="sidebar-tab" data-tab="ranked" type="button">Ranked</button>
  </div>
  <div class="sidebar-body">
    <div class="sidebar-pane active" data-pane="legend">
      <div class="sidebar-section">
        <div class="legend" id="legend"></div>
        <label class="sidebar-meta-toggle" title="Show only indirect paths">
          <span>Indirect only</span>
          <input id="indirect-only" type="checkbox" />
        </label>
        <label class="sidebar-meta-toggle" title="Show low confidence overlay">
          <span>Low confidence overlay</span>
          <input id="show-low-confidence" type="checkbox" />
        </label>
      </div>
    </div>
    <div class="sidebar-pane" data-pane="map">
      <div class="sidebar-section">
        <div class="map-toolbar">
          <div class="sidebar-pane-title">Visible addresses</div>
          <button class="toolbar-btn map-refresh-btn" id="refresh-map" type="button" aria-label="Refresh map" title="Refresh map">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.13-3.36L23 10M1 14l5.36 5.36A9 9 0 0 0 20.49 15"></path></svg>
          </button>
        </div>
        <div class="analysis-status" id="map-status">Open the map tab to geocode the visible addresses.</div>
        <div id="address-map"></div>
      </div>
    </div>
    <div class="sidebar-pane" data-pane="ranked">
      <div class="score-panel" id="score-panel"></div>
    </div>
  </div>
</aside>
<div id="graph"></div>
<div class="tooltip" id="tooltip"></div>
<div class="context-menu" id="context-menu"></div>
<script src="https://cdn.jsdelivr.net/npm/d3@7/dist/d3.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
<script>
const rawMainNodes = {nodes_json};
const rawMainEdges = {edges_json}.filter(e => e.kind !== "shared_org" && e.kind !== "cross_seed");
const LOW_CONFIDENCE_DATA_URL = "graph-data-low-confidence.json";
let baseNodes = rawMainNodes.slice();
let baseEdges = rawMainEdges.slice();
let mainNodeIds = new Set(baseNodes.map(node => node.id));
let allNodes = baseNodes.slice();
let allEdges = baseEdges.slice();
let lowConfidenceLoaded = false;
let lowConfidenceLoadingPromise = null;
let lowConfidenceNodes = [];
let lowConfidenceEdges = [];
const MERGE_OVERRIDE_STORAGE_KEY = "istari-manual-merge-overrides-v1";
const MERGE_OVERRIDE_URL = "/.netlify/functions/merge-overrides";
const ANALYZE_CONNECTION_URL = "/.netlify/functions/analyze-connection";
const EVIDENCE_FILE_URL = "/.netlify/functions/evidence-file";
const LOW_CONFIDENCE_EDGE_URL = "/.netlify/functions/low-confidence-edge";

function isAddressMergeNode(node) {{
  return node?.kind === "address";
}}

function isPersonMergeNode(node) {{
  return node?.kind === "person" && node?.lane === 4;
}}

function isIdentityMergeNode(node) {{
  return node?.kind === "seed_alias" && node?.lane === 1;
}}

function canStartMergeFromNode(node) {{
  if (node?.is_low_confidence) return false;
  return isAddressMergeNode(node) || isPersonMergeNode(node);
}}

function mergeKindForPair(sourceNode, targetNode) {{
  if (isAddressMergeNode(sourceNode) && isAddressMergeNode(targetNode)) return "address";
  if (isPersonMergeNode(sourceNode) && isPersonMergeNode(targetNode)) return "person";
  if (isPersonMergeNode(sourceNode) && isIdentityMergeNode(targetNode)) return "identity";
  return "";
}}

function normalizeMergeOverrides(overrides) {{
  const normalized = {{ address: [], person: [], identity: [] }};
  if (!overrides || typeof overrides !== "object") return normalized;
  ["address", "person", "identity"].forEach(kind => {{
    normalized[kind] = Array.isArray(overrides[kind])
      ? overrides[kind]
          .filter(row => row && row.sourceId && row.targetId)
          .map(row => {{
            return {{
              sourceId: String(row.sourceId),
              targetId: String(row.targetId),
            }};
          }})
      : [];
  }});
  return normalized;
}}

function loadLocalMergeOverrides() {{
  try {{
    const raw = window.localStorage.getItem(MERGE_OVERRIDE_STORAGE_KEY);
    return normalizeMergeOverrides(raw ? JSON.parse(raw) : null);
  }} catch (_error) {{
    return normalizeMergeOverrides(null);
  }}
}}

function saveLocalMergeOverrides(overrides) {{
  try {{
    window.localStorage.setItem(MERGE_OVERRIDE_STORAGE_KEY, JSON.stringify(normalizeMergeOverrides(overrides)));
  }} catch (_error) {{
  }}
}}

function resolveMergeTarget(nodeId, redirects) {{
  let current = String(nodeId || "");
  const seen = new Set();
  while (redirects.has(current) && !seen.has(current)) {{
    seen.add(current);
    current = String(redirects.get(current) || current);
  }}
  return current;
}}

function updateMergedNodeTooltip(node) {{
  const summary = `${{Number(node.org_count || 0)}} orgs, ${{Number(node.role_count || 0)}} roles, score ${{Number(node.score || 0)}}`;
  const tooltip = Array.isArray(node.tooltip_lines) ? node.tooltip_lines.slice() : [];
  const index = tooltip.findIndex(line => String(line).includes(" orgs, ") && String(line).includes(" roles, score "));
  if (index >= 0) tooltip[index] = summary;
  else tooltip.splice(Math.min(1, tooltip.length), 0, summary);
  node.tooltip_lines = tooltip;
}}

function applyManualMergeOverrides(nodes, edges, overrides) {{
  const normalized = normalizeMergeOverrides(overrides);
  const nodeMap = new Map(nodes.map(node => [node.id, node]));
  const redirects = new Map();

  ["address", "person", "identity"].forEach(kind => {{
    normalized[kind].forEach(override => {{
      const sourceId = resolveMergeTarget(override.sourceId, redirects);
      const targetId = resolveMergeTarget(override.targetId, redirects);
      if (!sourceId || !targetId || sourceId === targetId) return;
      const sourceNode = nodeMap.get(sourceId);
      const targetNode = nodeMap.get(targetId);
      if (!sourceNode || !targetNode) return;
      if (mergeKindForPair(sourceNode, targetNode) !== kind) return;

      redirects.set(sourceId, targetId);
      const aliases = new Set([
        ...(targetNode.aliases || []),
        targetNode.label,
        ...(sourceNode.aliases || []),
        sourceNode.label,
      ].filter(Boolean));
      targetNode.aliases = [...aliases].sort();

      if (kind === "address") {{
        if (!targetNode.postcode && sourceNode.postcode) targetNode.postcode = sourceNode.postcode;
        if (!targetNode.country && sourceNode.country) targetNode.country = sourceNode.country;
        if (!targetNode.normalized_key && sourceNode.normalized_key) targetNode.normalized_key = sourceNode.normalized_key;
      }} else {{
        targetNode.identity_keys = [
          ...new Set([...(targetNode.identity_keys || []), ...(sourceNode.identity_keys || [])]),
        ].sort();
      }}

      if (kind === "person") {{
        if (String(sourceNode.label || "").length > String(targetNode.label || "").length) {{
          targetNode.label = sourceNode.label;
        }}
      }}

      const tooltip = Array.isArray(targetNode.tooltip_lines) ? targetNode.tooltip_lines.slice() : [];
      const mergeNotice = kind === "address"
        ? `Manually merged address: ${{sourceNode.label}}`
        : kind === "identity"
          ? `Manually merged into this identity: ${{sourceNode.label}}`
          : `Manually merged with: ${{sourceNode.label}}`;
      if (!tooltip.includes(mergeNotice)) tooltip.push(mergeNotice);
      targetNode.tooltip_lines = tooltip;
    }});
  }});

  if (!redirects.size) return;

  const remappedEdges = [];
  const seenEdges = new Set();
  edges.forEach(edge => {{
    const nextSource = resolveMergeTarget(edge.source, redirects);
    const nextTarget = resolveMergeTarget(edge.target, redirects);
    if (!nextSource || !nextTarget || nextSource === nextTarget) return;
    const nextEdge = {{ ...edge, source: nextSource, target: nextTarget }};
    const key = [
      nextEdge.kind,
      nextEdge.source,
      nextEdge.target,
      nextEdge.phrase || "",
      nextEdge.role_type || "",
      nextEdge.tooltip || "",
    ].join("||");
    if (seenEdges.has(key)) return;
    seenEdges.add(key);
    remappedEdges.push(nextEdge);
  }});
  edges.splice(0, edges.length, ...remappedEdges);

  const survivingNodes = nodes.filter(node => resolveMergeTarget(node.id, redirects) === node.id);
  nodes.splice(0, nodes.length, ...survivingNodes);
  const survivingNodeMap = new Map(nodes.map(node => [node.id, node]));
  nodes.forEach(node => {{
    if (!isPersonMergeNode(node) && !isIdentityMergeNode(node)) return;
    const linkedEdges = edges.filter(edge => edge.kind === "role" && (edge.source === node.id || edge.target === node.id));
    const orgIds = new Set();
    const roleKeys = new Set();
    let score = 0;
    linkedEdges.forEach(edge => {{
      const otherId = edge.source === node.id ? edge.target : edge.source;
      if (survivingNodeMap.get(otherId)?.kind === "organisation") orgIds.add(otherId);
      roleKeys.add(`${{otherId}}||${{edge.phrase || edge.role_type || ""}}`);
      score += Number(edge.weight || 0);
    }});
    node.org_count = orgIds.size;
    node.role_count = roleKeys.size;
    node.score = Number(score.toFixed(4));
    updateMergedNodeTooltip(node);
  }});
}}

const localMergeOverrides = loadLocalMergeOverrides();
applyManualMergeOverrides(baseNodes, baseEdges, localMergeOverrides);
mainNodeIds = new Set(baseNodes.map(node => node.id));

const viewerState = {{
  focusedNodeIds: new Set(),
  searchQuery: "",
  hiddenNodeTypes: new Set(),
  indirectOnly: false,
  showLowConfidence: false,
  pendingMergeNodeId: "",
  analysisNodeIds: [],
}};

const container = document.getElementById("graph");
const tooltipEl = document.getElementById("tooltip");
const legendEl = document.getElementById("legend");
const compactLegendEl = document.getElementById("compact-legend");
const scorePanelEl = document.getElementById("score-panel");
const statsEl = document.getElementById("stats");
const contextMenuEl = document.getElementById("context-menu");
const viewerSidebarEl = document.getElementById("viewer-sidebar");
const sidebarTabEls = Array.from(document.querySelectorAll(".sidebar-tab"));
const sidebarPaneEls = Array.from(document.querySelectorAll(".sidebar-pane"));
const toggleSidebarButton = document.getElementById("toggle-sidebar");
const refreshMapButton = document.getElementById("refresh-map");
const mapStatusEl = document.getElementById("map-status");
const searchInput = document.getElementById("search");
let showIdentitiesInput = null;
let showCompaniesInput = null;
let showCharitiesInput = null;
let showPeopleInput = null;
let showAddressesInput = null;
let showLowConfidenceInput = null;
let indirectOnlyInput = document.getElementById("indirect-only");
const GEOCODE_CACHE_KEY = "istari-address-geocode-cache-v1";
const lowConfidenceEdgeDetailCache = new Map();
const lowConfidenceEdgeDetailPromises = new Map();

const W = container.clientWidth;
const H = container.clientHeight;
const LANE_Y = {{ 1: 140, 2: 360, 3: 620, 4: 840 }};
const visibleEdges = [];
let nodeById = new Map();
let edgesByNodeId = new Map();
let directEdgePairs = new Set();
let orgLinkIds = new Map();
let orgAddressIds = new Map();
let addressOrgIds = new Map();
let indirectIdentityIdsByOrg = new Map();
let visibleEdgeSet = new Set();

function rebuildGraphIndexes() {{
  nodeById = new Map(allNodes.map(node => [node.id, node]));
  edgesByNodeId = new Map();
  allEdges.forEach(edge => {{
    if (!edgesByNodeId.has(edge.source)) edgesByNodeId.set(edge.source, []);
    if (!edgesByNodeId.has(edge.target)) edgesByNodeId.set(edge.target, []);
    edgesByNodeId.get(edge.source).push(edge);
    edgesByNodeId.get(edge.target).push(edge);
  }});
  directEdgePairs = new Set(
    allEdges.map(edge => {{
      const [a, b] = [edge.source, edge.target].sort();
      return `${{a}}||${{b}}`;
    }})
  );
  orgLinkIds = new Map();
  allEdges.filter(edge => edge.kind === "org_link").forEach(edge => {{
    if (!orgLinkIds.has(edge.source)) orgLinkIds.set(edge.source, new Set());
    if (!orgLinkIds.has(edge.target)) orgLinkIds.set(edge.target, new Set());
    orgLinkIds.get(edge.source).add(edge.target);
    orgLinkIds.get(edge.target).add(edge.source);
  }});
  orgAddressIds = new Map();
  addressOrgIds = new Map();
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
  indirectIdentityIdsByOrg = new Map();
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
}}

function rebuildActiveGraph() {{
  allNodes = baseNodes.slice();
  allEdges = baseEdges.slice();
  if (!viewerState.showLowConfidence || !lowConfidenceLoaded) return;
  allNodes.push(...lowConfidenceNodes);
  allEdges.push(...lowConfidenceEdges);
}}

async function ensureLowConfidenceLoaded() {{
  if (lowConfidenceLoaded) return true;
  if (lowConfidenceLoadingPromise) return lowConfidenceLoadingPromise;
  lowConfidenceLoadingPromise = fetch(LOW_CONFIDENCE_DATA_URL)
    .then(response => {{
      if (!response.ok) throw new Error(`Overlay fetch failed: ${{response.status}}`);
      return response.json();
    }})
    .then(payload => {{
      lowConfidenceNodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
      lowConfidenceEdges = Array.isArray(payload?.edges) ? payload.edges : [];
      lowConfidenceLoaded = true;
      return true;
    }})
    .catch(error => {{
      console.error(error);
      lowConfidenceNodes = [];
      lowConfidenceEdges = [];
      lowConfidenceLoaded = false;
      return false;
    }})
    .finally(() => {{
      lowConfidenceLoadingPromise = null;
    }});
  return lowConfidenceLoadingPromise;
}}

const svg = d3.select(container).append("svg")
  .attr("width", "100%")
  .attr("height", "100%")
  .style("display", "block");
const gRoot = svg.append("g");
const zoom = d3.zoom().scaleExtent([0.05, 6]).on("zoom", (event) => gRoot.attr("transform", event.transform));
svg.call(zoom);
svg.on("dblclick.zoom", null);

let addressMap = null;
let addressMarkersLayer = null;
let currentMapRequestId = 0;

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
    `<label class="row" title="Identities">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("identity"))}} Identity</span>
      <input class="legend-toggle" id="show-identities" type="checkbox" checked aria-label="Identities" />
    </label>`,
    `<label class="row" title="Charities">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("charity"))}} Charity</span>
      <input class="legend-toggle" id="show-charities" type="checkbox" checked aria-label="Charities" />
    </label>`,
    `<label class="row" title="Companies">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("company"))}} Company</span>
      <input class="legend-toggle" id="show-companies" type="checkbox" checked aria-label="Companies" />
    </label>`,
    `<div class="row" title="Other organisations">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("organisation"))}} Other organisation</span>
    </div>`,
    `<label class="row" title="Addresses">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("address"))}} Address</span>
      <input class="legend-toggle" id="show-addresses" type="checkbox" checked aria-label="Addresses" />
    </label>`,
    `<label class="row" title="People">
      <span class="legend-key">${{iconSvgMarkup(iconSpec("person"))}} Person</span>
      <input class="legend-toggle" id="show-people" type="checkbox" checked aria-label="People" />
    </label>`,
    `<div class="row" title="OFAC sanctioned">
      <span class="legend-key"><span class="dot" style="background:#ff2222;border:2px solid #ff2222"></span> Sanctioned (OFAC)</span>
    </div>`,
  ].join("");
}}
renderLegend();
showIdentitiesInput = document.getElementById("show-identities");
showCompaniesInput = document.getElementById("show-companies");
showCharitiesInput = document.getElementById("show-charities");
showPeopleInput = document.getElementById("show-people");
showAddressesInput = document.getElementById("show-addresses");
showLowConfidenceInput = document.getElementById("show-low-confidence");

function renderCompactLegend() {{
  compactLegendEl.innerHTML = [
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("identity"))}} Identity</span></div>`,
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("charity"))}} Charity</span></div>`,
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("company"))}} Company</span></div>`,
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("organisation"))}} Other organisation</span></div>`,
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("address"))}} Address</span></div>`,
    `<div class="row"><span class="legend-key">${{iconSvgMarkup(iconSpec("person"))}} Person</span></div>`,
    `<div class="row"><span class="legend-key"><span class="dot" style="background:#ff2222;border:2px solid #ff2222"></span> Sanctioned (OFAC)</span></div>`,
  ].join("");
}}
renderCompactLegend();

function edgeTooltipLines(edge) {{
  if (Array.isArray(edge?.tooltip_lines) && edge.tooltip_lines.length) return edge.tooltip_lines;
  if (edge?.tooltip) return [edge.tooltip];
  return [edge?.phrase || edge?.role_type || "link"];
}}

function applyLowConfidenceEdgeDetail(edge, detail) {{
  if (!edge || !detail || typeof detail !== "object") return;
  edge.tooltip = String(detail.tooltip || edge.tooltip || "");
  edge.tooltip_lines = Array.isArray(detail.tooltip_lines)
    ? detail.tooltip_lines.map(line => String(line || "")).filter(Boolean)
    : edge.tooltip_lines;
  edge.evidence = detail.evidence && typeof detail.evidence === "object" ? detail.evidence : null;
  edge.evidence_items = Array.isArray(detail.evidence_items) ? detail.evidence_items : [];
  edge._detailLoaded = true;
  lowConfidenceEdgeDetailCache.set(edge.id, {{
    tooltip: edge.tooltip,
    tooltip_lines: edge.tooltip_lines,
    evidence: edge.evidence,
    evidence_items: edge.evidence_items,
  }});
}}

async function ensureLowConfidenceEdgeDetail(edge) {{
  if (!edge?.is_low_confidence || edge._detailLoaded || !edge.detail_available) return edge;
  const cached = lowConfidenceEdgeDetailCache.get(edge.id);
  if (cached) {{
    applyLowConfidenceEdgeDetail(edge, cached);
    return edge;
  }}
  if (lowConfidenceEdgeDetailPromises.has(edge.id)) {{
    await lowConfidenceEdgeDetailPromises.get(edge.id);
    return edge;
  }}
  const request = fetch(`${{LOW_CONFIDENCE_EDGE_URL}}?id=${{encodeURIComponent(edge.id)}}`)
    .then(response => {{
      if (!response.ok) throw new Error(`Low-confidence edge detail fetch failed: ${{response.status}}`);
      return response.json();
    }})
    .then(detail => {{
      applyLowConfidenceEdgeDetail(edge, detail);
      return detail;
    }})
    .catch(error => {{
      console.error(error);
      return null;
    }})
    .finally(() => {{
      lowConfidenceEdgeDetailPromises.delete(edge.id);
    }});
  lowConfidenceEdgeDetailPromises.set(edge.id, request);
  await request;
  return edge;
}}

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

function edgeBetweenNodes(leftId, rightId) {{
  return (edgesByNodeId.get(leftId) || []).find(edge => (
    edge.kind !== "hidden_connection"
    && ((edge.source === leftId && edge.target === rightId) || (edge.source === rightId && edge.target === leftId))
  )) || null;
}}

function pathEdgesFromHiddenChain(sourceId, targetId, hiddenNodeIds) {{
  const nodeIds = [sourceId, ...hiddenNodeIds, targetId];
  const pathEdges = [];
  for (let index = 0; index < nodeIds.length - 1; index += 1) {{
    const edge = edgeBetweenNodes(nodeIds[index], nodeIds[index + 1]);
    if (edge) pathEdges.push(edge);
  }}
  return pathEdges;
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
          const pathEdges = pathEdgesFromHiddenChain(parentId, targetId, orderedHiddenNodes);
          hiddenConnections.set(pairKey, {{
            source: parentId,
            target: targetId,
            kind: "hidden_connection",
            hiddenNodeIds: orderedHiddenNodes,
            pathEdges,
            tooltip_lines: hiddenConnectionTooltipLines(parentId, targetId, orderedHiddenNodes, pathEdges),
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
  viewerState.showLowConfidence = !!showLowConfidenceInput.checked;
  viewerState.indirectOnly = indirectOnlyInput.checked;
}}

function applyTypeFilters(visibleIds, rootIds, options = {{}}) {{
  if (!visibleIds.size) return new Set();

  const filteredIds = new Set(
    [...visibleIds].filter(id => {{
      const node = nodeById.get(id);
      if (!node || node.kind === "seed") return false;
      if (node.is_low_confidence && !viewerState.showLowConfidence) return false;
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
  const edgeIds = allEdges.filter(
    edge =>
      filteredVisibleIds.has(edge.source)
      && filteredVisibleIds.has(edge.target)
      && (viewerState.showLowConfidence || !edge.is_low_confidence)
  );
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
  const edgeIds = allEdges.filter(
    edge =>
      filteredVisibleIds.has(edge.source)
      && filteredVisibleIds.has(edge.target)
      && (viewerState.showLowConfidence || !edge.is_low_confidence)
  );
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
    const edgeIds = allEdges.filter(
      edge =>
        visibleIds.has(edge.source)
        && visibleIds.has(edge.target)
        && (viewerState.showLowConfidence || !edge.is_low_confidence)
    );
    return {{ rootIds, visibleIds, edgeIds }};
  }}

  const subgraph = collectConnectedSubgraph(rootIds);
  const visibleIds = applyTypeFilters(new Set(subgraph.reachableIds), rootIds);
  const edgeIds = allEdges.filter(
    edge =>
      visibleIds.has(edge.source)
      && visibleIds.has(edge.target)
      && (viewerState.showLowConfidence || !edge.is_low_confidence)
  );
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
  if (node.is_low_confidence) return "var(--amber)";
  if (node.kind === "seed_alias") return "var(--amber)";
  if (node.kind === "organisation") return "var(--green)";
  if (node.kind === "address") return "var(--purple)";
  return "var(--blue)";
}}

function edgeStroke(edge) {{
  if (edge.is_low_confidence) return "var(--amber)";
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
      type: "open_url",
      label: "Open Companies House page",
      url: `https://find-and-update.company-information.service.gov.uk/company/${{encodeURIComponent(registryNumber)}}`,
    }};
  }}
  if (registryType === "charity") {{
    return {{
      type: "open_url",
      label: "Open Charity Commission page",
      url: `https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/${{encodeURIComponent(registryNumber)}}`,
    }};
  }}
  return null;
}}

function isCompaniesHouseDocumentUrl(value) {{
  try {{
    const url = new URL(String(value || "").trim(), window.location.origin);
    return /(^|\.)document-api\.company-information\.service\.gov\.uk$/i.test(url.hostname)
      && /^\/document\/.+/.test(url.pathname);
  }} catch (_error) {{
    return false;
  }}
}}

function evidenceActionUrl(evidence) {{
  const documentUrl = String(evidence?.document_url || "").trim();
  if (!documentUrl) return "";
  const pageNumber = Number(evidence?.page_number || 0);
  if (isCompaniesHouseDocumentUrl(documentUrl)) {{
    const params = new URLSearchParams({{ url: documentUrl }});
    if (pageNumber) params.set("page", String(pageNumber));
    return `${{EVIDENCE_FILE_URL}}?${{params.toString()}}`;
  }}
  if (!pageNumber || documentUrl.includes("#") || !/\\.pdf($|[?#])/i.test(documentUrl)) return documentUrl;
  return `${{documentUrl}}#page=${{pageNumber}}`;
}}

function evidenceActionsForEdge(edge) {{
  const evidenceItems = [];
  const seen = new Set();
  const pushEvidence = evidence => {{
    if (!evidence || typeof evidence !== "object") return;
    const url = String(evidence.document_url || "").trim();
    const title = String(evidence.title || "").trim();
    const page = String(evidence.page_hint || evidence.page_number || "").trim();
    const key = `${{url}}||${{title}}||${{page}}`;
    if (!url || seen.has(key)) return;
    seen.add(key);
    evidenceItems.push(evidence);
  }};
  (Array.isArray(edge?.evidence_items) ? edge.evidence_items : []).forEach(pushEvidence);
  if (edge?.evidence) pushEvidence(edge.evidence);
  (Array.isArray(edge?.pathEdges) ? edge.pathEdges : []).forEach(pathEdge => {{
    (Array.isArray(pathEdge?.evidence_items) ? pathEdge.evidence_items : []).forEach(pushEvidence);
    if (pathEdge?.evidence) pushEvidence(pathEdge.evidence);
  }});
  return evidenceItems
    .map(evidence => {{
      const url = evidenceActionUrl(evidence);
      if (!url) return null;
      const title = String(evidence?.title || edge.tooltip || "Evidence").trim();
      const pageHint = String(evidence?.page_hint || "").trim();
      return {{
        type: "open_url",
        label: pageHint ? `Open evidence: ${{title}} (${{pageHint}})` : `Open evidence: ${{title}}`,
        url,
      }};
    }})
    .filter(Boolean);
}}

function handleEdgeMouseOver(event, edge) {{
  showTooltip(event, edgeTooltipLines(edge));
  if (edge?.is_low_confidence && !edge._detailLoaded && edge.detail_available) {{
    ensureLowConfidenceEdgeDetail(edge);
  }}
}}

function mergeActionsForNode(node) {{
  const actions = [];
  const pendingNode = nodeById.get(viewerState.pendingMergeNodeId);

  if (pendingNode && pendingNode.id !== node.id) {{
    const kind = mergeKindForPair(pendingNode, node);
    if (kind) {{
      actions.push({{
        type: "merge_commit",
        kind,
        sourceId: pendingNode.id,
        targetId: node.id,
        label: kind === "identity"
          ? `Merge ${{pendingNode.label}} into this identity permanently`
          : `Merge ${{pendingNode.label}} into this ${{kind}} permanently`,
      }});
    }}
    actions.push({{
      type: "merge_clear",
      label: "Clear merge selection",
    }});
    return actions;
  }}

  if (viewerState.pendingMergeNodeId === node.id) {{
    actions.push({{
      type: "merge_clear",
      label: "Clear merge selection",
    }});
    return actions;
  }}

  if (!canStartMergeFromNode(node)) return actions;
  actions.push({{
    type: "merge_prepare",
    nodeId: node.id,
    label: "Merge",
  }});
  return actions;
}}

async function persistMergeOverride(kind, sourceId, targetId) {{
  const overrides = loadLocalMergeOverrides();
  overrides[kind].push({{ sourceId, targetId }});
  saveLocalMergeOverrides(overrides);
  try {{
    const response = await fetch(MERGE_OVERRIDE_URL, {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify({{ kind, sourceId, targetId }}),
    }});
    if (response.ok) {{
      const payload = await response.json();
      saveLocalMergeOverrides(payload?.overrides || overrides);
    }}
  }} catch (_error) {{
  }}
  window.location.reload();
}}

async function syncLocalOverridesFromServer() {{
  try {{
    const response = await fetch(MERGE_OVERRIDE_URL);
    if (!response.ok) return;
    const payload = await response.json();
    const remote = normalizeMergeOverrides(payload?.overrides || payload);
    const local = loadLocalMergeOverrides();
    const remoteCount = remote.address.length + remote.person.length + remote.identity.length;
    const localCount = local.address.length + local.person.length + local.identity.length;
    if (remoteCount > localCount) {{
      saveLocalMergeOverrides(remote);
      window.location.reload();
    }}
  }} catch (_error) {{
  }}
}}

function toggleSidebar(forceOpen) {{
  const nextOpen = typeof forceOpen === "boolean"
    ? forceOpen
    : !viewerSidebarEl.classList.contains("open");
  viewerSidebarEl.classList.toggle("open", nextOpen);
  toggleSidebarButton.classList.toggle("open", nextOpen);
  compactLegendEl.classList.toggle("visible", !nextOpen);
  toggleSidebarButton.setAttribute("aria-label", nextOpen ? "Hide tools sidebar" : "Show tools sidebar");
  toggleSidebarButton.setAttribute("title", nextOpen ? "Hide tools sidebar" : "Show tools sidebar");
}}
toggleSidebar(viewerSidebarEl.classList.contains("open"));

function setSidebarTab(tabName) {{
  sidebarTabEls.forEach(el => el.classList.toggle("active", el.dataset.tab === tabName));
  sidebarPaneEls.forEach(el => el.classList.toggle("active", el.dataset.pane === tabName));
  if (tabName === "map") {{
    window.setTimeout(() => {{
      if (addressMap) addressMap.invalidateSize();
    }}, 0);
  }}
}}
setSidebarTab("legend");

function analysisActionsForNode(node) {{
  if (node?.is_low_confidence) return [];
  const selected = viewerState.analysisNodeIds.includes(node.id);
  if (selected && viewerState.analysisNodeIds.length === 2) {{
    return [{{
      type: "analysis_run",
      label: "Explain selected connection",
    }}, {{
      type: "analysis_clear",
      label: "Clear selected analysis nodes",
    }}];
  }}
  if (selected) {{
    return [{{
      type: "analysis_remove",
      nodeId: node.id,
      label: "Remove from connection analysis",
    }}];
  }}
  if (viewerState.analysisNodeIds.length < 2) {{
    return [{{
      type: "analysis_add",
      nodeId: node.id,
      label: "Select for connection analysis",
    }}];
  }}
  return [{{
    type: "analysis_run",
    label: "Explain selected connection",
  }}, {{
    type: "analysis_clear",
    label: "Clear selected analysis nodes",
  }}];
}}

function formatAnalysisCopyText(payload) {{
  const evidenceById = new Map((Array.isArray(payload.evidence) ? payload.evidence : []).map(item => [String(item.id || ""), item]));
  const lines = [String(payload.summary || "No explanation returned.").trim()];
  const claims = Array.isArray(payload.claims) ? payload.claims : [];
  if (claims.length) {{
    lines.push("", "Claims:");
    claims.forEach((claim, index) => {{
      const refs = (Array.isArray(claim.evidence_ids) ? claim.evidence_ids : [])
        .map(id => evidenceById.get(String(id)))
        .filter(Boolean)
        .map(item => {{
          const pageLabel = item.page_hint || (item.page_number ? `page ${{item.page_number}}` : "");
          return pageLabel ? `${{item.title}} (${{pageLabel}})` : item.title;
        }});
      lines.push(`${{index + 1}}. ${{String(claim.text || "").trim()}}${{refs.length ? ` [${{refs.join("; ")}}]` : ""}}`);
    }});
  }}
  return lines.join("\\n");
}}

function renderAnalysisResult(payload) {{
  const sourceNode = nodeById.get(payload.sourceNodeId);
  const targetNode = nodeById.get(payload.targetNodeId);
  const pathItems = Array.isArray(payload.path?.edges) ? payload.path.edges : [];
  const evidenceById = new Map((Array.isArray(payload.evidence) ? payload.evidence : []).map(item => [String(item.id || ""), item]));
  const claims = Array.isArray(payload.claims) ? payload.claims : [];
  const content = [
    `<div class="analysis-selection">${{escapeHtml(sourceNode?.label || payload.sourceNodeId)}} to ${{escapeHtml(targetNode?.label || payload.targetNodeId)}}</div>`,
    `<div class="analysis-text">${{escapeHtml(payload.summary || "No explanation returned.").replaceAll("\\n", "<br>")}}</div>`,
    claims.length
      ? `<div class="analysis-claims">${{claims.map((claim, index) => {{
          const links = (Array.isArray(claim.evidence_ids) ? claim.evidence_ids : [])
            .map(id => evidenceById.get(String(id)))
            .filter(Boolean)
            .map(item => {{
              const url = evidenceActionUrl(item);
              const pageLabel = item.page_hint || (item.page_number ? `page ${{item.page_number}}` : "");
              const label = pageLabel ? `${{item.title}} (${{pageLabel}})` : item.title;
              return url ? `<a href="${{escapeHtml(url)}}" target="_blank" rel="noreferrer">${{escapeHtml(label)}}</a>` : "";
            }})
            .filter(Boolean)
            .join("");
          return `
          <div class="analysis-claim">
            <div class="analysis-claim-text">${{index + 1}}. ${{escapeHtml(claim.text || "")}}</div>
            <div class="analysis-claim-evidence">${{links}}</div>
          </div>`;
        }}).join("")}}</div>`
      : "",
    pathItems.length
      ? `<div class="analysis-path">${{pathItems.map(edge => `
          <div class="analysis-path-item">${{escapeHtml(edge.source_label || edge.source_id)}} ${{escapeHtml(edge.phrase || "is linked to")}} ${{escapeHtml(edge.target_label || edge.target_id)}}</div>
        `).join("")}}</div>`
      : "",
  ].join("");
  const popup = window.open("", "_blank", "noopener,noreferrer,width=680,height=760");
  if (!popup) return;
  popup.document.write(`<!DOCTYPE html><html><head><title>Connection analysis</title><meta charset="utf-8"><style>
    body {{ margin: 0; padding: 18px; background: #161a22; color: #d7dde7; font: 13px/1.6 "Segoe UI", system-ui, sans-serif; }}
    a {{ color: #58a6ff; text-decoration: none; }}
    .analysis-selection {{ font-size: 12px; color: #94a3b8; margin-bottom: 12px; }}
    .analysis-text {{ margin-bottom: 14px; }}
    .analysis-claims, .analysis-path {{ display: flex; flex-direction: column; gap: 10px; }}
    .analysis-claim, .analysis-path-item {{ border: 1px solid rgba(255,255,255,0.08); border-radius: 10px; padding: 10px; background: rgba(255,255,255,0.03); }}
    .analysis-claim-text {{ margin-bottom: 6px; }}
    .toolbar {{ display: flex; justify-content: flex-end; margin-bottom: 14px; }}
    button {{ border: 1px solid rgba(255,255,255,0.08); background: rgba(255,255,255,0.03); color: #d7dde7; border-radius: 8px; padding: 8px 10px; cursor: pointer; }}
  </style></head><body>
    <div class="toolbar"><button id="copy-analysis-popup" type="button">Copy</button></div>
    ${{content}}
    <script>
      const copyText = "__COPY_TEXT__";
      document.getElementById("copy-analysis-popup").addEventListener("click", async () => {{
        try {{
          await navigator.clipboard.writeText(copyText);
        }} catch (_error) {{
        }}
      }});
    <\/script>
  </body></html>`.replace("__COPY_TEXT__", JSON.stringify(formatAnalysisCopyText(payload))));
  popup.document.close();
}}

async function openAnalysisView() {{
  if (viewerState.analysisNodeIds.length !== 2) {{
    return;
  }}
  const [sourceNodeId, targetNodeId] = viewerState.analysisNodeIds;
  const response = await fetch(ANALYZE_CONNECTION_URL, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ sourceNodeId, targetNodeId }}),
  }});
  const payload = await response.json().catch(() => ({{}}));
  if (!response.ok) {{
    if (payload.summary) renderAnalysisResult({{ ...payload, sourceNodeId, targetNodeId }});
    return;
  }}
  renderAnalysisResult(payload);
}}

async function openEdgeContextMenu(event, edge) {{
  event.preventDefault();
  event.stopPropagation();
  hideTooltip();

  if (edge?.is_low_confidence && !edge._detailLoaded && edge.detail_available) {{
    await ensureLowConfidenceEdgeDetail(edge);
  }}

  const sourceNode = nodeById.get(edge.source);
  const targetNode = nodeById.get(edge.target);
  const actions = evidenceActionsForEdge(edge);
  contextMenuEl._actions = actions;
  contextMenuEl.innerHTML = [
    `<div class="context-menu-title">${{escapeHtml(sourceNode?.label || edge.source)}} to ${{escapeHtml(targetNode?.label || edge.target)}}</div>`,
    `<div class="context-menu-subtitle">${{escapeHtml(edge.phrase || edge.role_type || "link")}}</div>`,
    actions.length
      ? actions
          .map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${{index}}">${{escapeHtml(action.label)}}</button>`)
          .join("")
      : `<div class="context-menu-empty">No evidence is available for this link yet.</div>`,
  ].join("");
  contextMenuEl.style.display = "block";

  const rect = contextMenuEl.getBoundingClientRect();
  const maxLeft = window.innerWidth - rect.width - 10;
  const maxTop = window.innerHeight - rect.height - 10;
  contextMenuEl.style.left = Math.max(10, Math.min(event.clientX, maxLeft)) + "px";
  contextMenuEl.style.top = Math.max(10, Math.min(event.clientY, maxTop)) + "px";
}}

function closeContextMenu() {{
  contextMenuEl.style.display = "none";
  contextMenuEl.innerHTML = "";
  contextMenuEl._actions = [];
}}

function loadGeocodeCache() {{
  try {{
    const raw = window.localStorage.getItem(GEOCODE_CACHE_KEY);
    const parsed = raw ? JSON.parse(raw) : {{}};
    return parsed && typeof parsed === "object" ? parsed : {{}};
  }} catch (_error) {{
    return {{}};
  }}
}}

function saveGeocodeCache(cache) {{
  try {{
    window.localStorage.setItem(GEOCODE_CACHE_KEY, JSON.stringify(cache));
  }} catch (_error) {{
  }}
}}

function visibleAddressNodes() {{
  return allNodes.filter(node => node._visible && node.kind === "address");
}}

function ensureAddressMap() {{
  if (addressMap) return;
  addressMap = L.map("address-map", {{ zoomControl: true }});
  L.tileLayer("https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }}).addTo(addressMap);
  addressMarkersLayer = L.layerGroup().addTo(addressMap);
}}

function normalizeGeocodeValue(value) {{
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}}

function buildGeocodeQueries(node) {{
  const label = String(node?.label || "").trim();
  const postcode = String(node?.postcode || "").trim();
  const country = String(node?.country || "").trim();
  const labelNorm = normalizeGeocodeValue(label);
  const queries = [];
  const pushQuery = value => {{
    const cleaned = String(value || "").trim();
    if (!cleaned) return;
    const normalized = normalizeGeocodeValue(cleaned);
    if (!normalized) return;
    if (queries.some(existing => normalizeGeocodeValue(existing) === normalized)) return;
    queries.push(cleaned);
  }};
  const extras = [];
  if (postcode && !labelNorm.includes(normalizeGeocodeValue(postcode))) extras.push(postcode);
  if (country && !labelNorm.includes(normalizeGeocodeValue(country))) extras.push(country);
  pushQuery(label);
  pushQuery([label, ...extras].filter(Boolean).join(", "));
  if (postcode) pushQuery([postcode, country].filter(Boolean).join(", "));
  if (/\\bpo\\.?\\s*box\\b/i.test(label) && postcode) {{
    pushQuery(postcode);
  }}
  return queries;
}}

async function geocodeAddressNode(node) {{
  const cacheKey = String(node.normalized_key || node.label || "").trim();
  if (!cacheKey) return null;
  const cache = loadGeocodeCache();
  if (cache[cacheKey]) return cache[cacheKey];

  const queries = buildGeocodeQueries(node);
  for (const query of queries) {{
    const attempts = [
      async () => {{
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1&q=${{encodeURIComponent(query)}}`);
        if (!response.ok) return null;
        const rows = await response.json();
        if (!Array.isArray(rows) || !rows.length) return null;
        return {{
          lat: Number(rows[0].lat),
          lon: Number(rows[0].lon),
          label: String(rows[0].display_name || node.label || ""),
        }};
      }},
      async () => {{
        const response = await fetch(`https://photon.komoot.io/api/?limit=1&q=${{encodeURIComponent(query)}}`);
        if (!response.ok) return null;
        const payload = await response.json();
        const feature = Array.isArray(payload?.features) ? payload.features[0] : null;
        const coords = Array.isArray(feature?.geometry?.coordinates) ? feature.geometry.coordinates : null;
        if (!coords || coords.length < 2) return null;
        return {{
          lat: Number(coords[1]),
          lon: Number(coords[0]),
          label: String(feature?.properties?.name || feature?.properties?.street || node.label || ""),
        }};
      }},
    ];
    for (const attempt of attempts) {{
      try {{
        const resolved = await attempt();
        if (!resolved) continue;
        if (!Number.isFinite(resolved.lat) || !Number.isFinite(resolved.lon)) continue;
        cache[cacheKey] = resolved;
        saveGeocodeCache(cache);
        return resolved;
      }} catch (_error) {{
      }}
    }}
  }}
  return null;
}}

async function openMapView() {{
  closeContextMenu();
  hideTooltip();
  setSidebarTab("map");
  toggleSidebar(true);

  const addressNodes = visibleAddressNodes();
  if (!addressNodes.length) {{
    mapStatusEl.textContent = "No visible address nodes are on screen right now.";
    return;
  }}

  ensureAddressMap();
  window.setTimeout(() => {{
    if (addressMap) addressMap.invalidateSize();
  }}, 0);
  currentMapRequestId += 1;
  const requestId = currentMapRequestId;
  const limitedNodes = addressNodes.slice(0, 50);
  const markers = [];

  mapStatusEl.textContent = `Geocoding ${{limitedNodes.length}} visible addresses...`;
  for (let index = 0; index < limitedNodes.length; index += 1) {{
    if (requestId !== currentMapRequestId) return;
    const node = limitedNodes[index];
    const result = await geocodeAddressNode(node);
    if (result) markers.push({{ node, ...result }});
  }}

  if (requestId !== currentMapRequestId) return;

  addressMarkersLayer.clearLayers();
  if (!markers.length) {{
    mapStatusEl.textContent = "No visible addresses could be geocoded yet.";
    addressMap.setView([20, 0], 2);
    window.setTimeout(() => addressMap.invalidateSize(), 0);
    return;
  }}

  markers.forEach(marker => {{
    L.marker([marker.lat, marker.lon])
      .bindPopup(`<strong>${{escapeHtml(marker.node.label || "Address")}}</strong><br>${{escapeHtml(marker.label || "")}}`)
      .addTo(addressMarkersLayer);
  }});

  const bounds = L.latLngBounds(markers.map(marker => [marker.lat, marker.lon]));
  addressMap.fitBounds(bounds.pad(0.15));
  window.setTimeout(() => addressMap.invalidateSize(), 0);
  mapStatusEl.textContent = markers.length === limitedNodes.length
    ? `Mapped ${{markers.length}} visible addresses.`
    : `Mapped ${{markers.length}} of ${{limitedNodes.length}} visible addresses.`;
  if (addressNodes.length > limitedNodes.length) {{
    mapStatusEl.textContent += ` Showing the first ${{limitedNodes.length}} only.`;
  }}
}}

function openContextMenu(event, node) {{
  event.preventDefault();
  event.stopPropagation();
  hideTooltip();

  const actions = [];
  const registryAction = registryActionForNode(node);
  if (registryAction) actions.push(registryAction);
  mergeActionsForNode(node).forEach(action => actions.push(action));
  analysisActionsForNode(node).forEach(action => actions.push(action));

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
    if (!visibleEdgeSet.has(edge)) return;
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
    .attr("stroke-opacity", edge => edge.is_low_confidence ? 0.75 : edge.kind === "hidden_connection" ? 0.7 : edge.kind === "alias" ? 0.8 : edge.kind === "address_link" ? 0.75 : 0.45)
    .attr("stroke-dasharray", edge => edge.is_low_confidence ? "5 4" : edge.kind === "hidden_connection" ? "5 4" : null)
    .style("pointer-events", "none");

  groups.select("line.role-edge-hit")
    .on("mouseover", handleEdgeMouseOver)
    .on("mousemove", positionTooltip)
    .on("mouseout", hideTooltip)
    .on("contextmenu", openEdgeContextMenu);
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
let pills = null;

function renderNodeJoin() {{
  pills = nodeGroup.selectAll("g.pill")
    .data(allNodes, node => node.id)
    .join(
      enter => {{
        const pill = enter.append("g")
          .attr("class", "pill");

        pill.append("rect");
        pill.append("text").style("pointer-events", "none");

        const badgeGroups = pill.append("g")
          .attr("class", "badge-group");
        badgeGroups.append("rect");
        badgeGroups.append("path").style("pointer-events", "none");

        const focusButtons = pill.append("g")
          .attr("class", "focus-button-group");
        focusButtons.append("circle").attr("class", "focus-btn");
        focusButtons.append("path").attr("class", "focus-btn");

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

        pill
          .on("mouseover", (event, node) => showTooltip(event, node.tooltip_lines || [node.label]))
          .on("mousemove", positionTooltip)
          .on("mouseout", hideTooltip)
          .on("contextmenu", openContextMenu);

        return pill;
      }},
      update => update,
      exit => exit.remove()
    )
    .style("cursor", node => node.kind === "seed" ? "default" : "pointer");

  pills.select("rect")
    .attr("rx", node => pillHeight(node) / 2)
    .attr("ry", node => pillHeight(node) / 2)
    .attr("width", pillWidth)
    .attr("height", pillHeight)
    .attr("fill", nodeColor)
    .attr("fill-opacity", node => node.is_low_confidence ? 0.14 : node.sanctioned ? 0.35 : 0.18)
    .attr("stroke", nodeColor)
    .attr("stroke-width", node => node.sanctioned ? 2.5 : node.is_low_confidence ? 1.4 : 1.2)
    .attr("stroke-opacity", node => node.sanctioned ? 1.0 : node.is_low_confidence ? 0.95 : 0.7)
    .attr("stroke-dasharray", node => node.is_low_confidence ? "5 3" : null);

  pills.select("text")
    .text(node => node.label)
    .attr("font-size", fontSize)
    .attr("font-weight", node => node.kind === "seed_alias" ? 600 : 400)
    .attr("fill", node => node.kind === "seed_alias" ? "var(--text-bright)" : "var(--text)")
    .attr("text-anchor", "start")
    .attr("dominant-baseline", "central")
    .attr("x", badgeTextInset)
    .attr("y", node => pillHeight(node) / 2);

  const badgeGroups = pills.select("g.badge-group")
    .style("display", node => badgeSpec(node) ? null : "none");

  badgeGroups.select("rect")
    .attr("rx", node => badgeHeight(node) / 2)
    .attr("ry", node => badgeHeight(node) / 2)
    .attr("x", 8)
    .attr("y", node => (pillHeight(node) - badgeHeight(node)) / 2)
    .attr("width", badgeWidth)
    .attr("height", badgeHeight)
    .attr("fill", node => badgeSpec(node)?.fill || "transparent")
    .attr("stroke", "rgba(255,255,255,0.18)")
    .attr("stroke-width", 0.8);

  badgeGroups.select("path")
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
    .attr("stroke-linejoin", "round");

  const focusButtons = pills.select("g.focus-button-group")
    .style("display", node => node.kind === "seed" ? "none" : null);

  focusButtons.select("circle")
    .attr("cx", node => pillWidth(node) - 14)
    .attr("cy", node => pillHeight(node) / 2)
    .attr("r", 8)
    .attr("fill", "rgba(255,255,255,0.08)")
    .attr("stroke", "rgba(255,255,255,0.28)")
    .attr("stroke-width", 1);

  focusButtons.select("path")
    .attr("d", iconPath("search"))
    .attr("transform", node => `translate(${{pillWidth(node) - 20}},${{pillHeight(node) / 2 - 6}}) scale(0.5)`)
    .attr("fill", "none")
    .attr("stroke", "#ffffff")
    .attr("stroke-width", 1.8)
    .attr("stroke-linecap", "round")
    .attr("stroke-linejoin", "round");

  pills.call(drag);
}}

contextMenuEl.addEventListener("click", (event) => {{
  const button = event.target.closest("[data-action-index]");
  if (!button) return;
  const index = Number(button.getAttribute("data-action-index"));
  const action = Array.isArray(contextMenuEl._actions) ? contextMenuEl._actions[index] : null;
  if (!action) return;
  if (action.type === "open_url" && action.url) {{
    window.open(action.url, "_blank", "noopener,noreferrer");
    closeContextMenu();
    return;
  }}
  if (action.type === "merge_prepare") {{
    viewerState.pendingMergeNodeId = String(action.nodeId || "");
    closeContextMenu();
    return;
  }}
  if (action.type === "merge_clear") {{
    viewerState.pendingMergeNodeId = "";
    closeContextMenu();
    return;
  }}
  if (action.type === "merge_commit") {{
    closeContextMenu();
    const sourceNode = nodeById.get(action.sourceId);
    const targetNode = nodeById.get(action.targetId);
    const confirmed = window.confirm(
      `Permanently merge "${{sourceNode?.label || action.sourceId}}" into "${{targetNode?.label || action.targetId}}"?`
    );
    if (!confirmed) return;
    viewerState.pendingMergeNodeId = "";
    persistMergeOverride(String(action.kind || ""), String(action.sourceId || ""), String(action.targetId || "")).catch(() => {{
      window.alert("Saving the merge failed.");
    }});
    return;
  }}
  if (action.type === "analysis_add") {{
    viewerState.analysisNodeIds = [...viewerState.analysisNodeIds, String(action.nodeId || "")].slice(0, 2);
    closeContextMenu();
    return;
  }}
  if (action.type === "analysis_remove") {{
    viewerState.analysisNodeIds = viewerState.analysisNodeIds.filter(id => id !== String(action.nodeId || ""));
    closeContextMenu();
    return;
  }}
  if (action.type === "analysis_run") {{
    closeContextMenu();
    openAnalysisView().catch(() => {{
      window.alert("Connection analysis failed.");
    }});
    return;
  }}
  if (action.type === "analysis_clear") {{
    viewerState.analysisNodeIds = [];
    closeContextMenu();
  }}
}});

document.addEventListener("click", closeContextMenu);
window.addEventListener("resize", closeContextMenu);
window.addEventListener("blur", closeContextMenu);
refreshMapButton.addEventListener("click", () => {{
  openMapView().catch(() => {{
    mapStatusEl.textContent = "Address geocoding failed.";
  }});
}});
toggleSidebarButton.addEventListener("click", () => toggleSidebar());
sidebarTabEls.forEach(el => {{
  el.addEventListener("click", () => {{
    const tabName = String(el.dataset.tab || "legend");
    setSidebarTab(tabName);
    if (tabName === "map") {{
      openMapView().catch(() => {{
        mapStatusEl.textContent = "Address geocoding failed.";
      }});
    }}
  }});
}});

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

svg.on("dblclick.focus", () => {{
  if (!viewerState.focusedNodeIds.size) return;
  viewerState.focusedNodeIds.clear();
  applyViewerState();
}});

function updateFocusStyling(rootIds) {{
  pills.select("rect")
    .attr("stroke-width", node => {{
      if (node.sanctioned) return rootIds.has(node.id) ? 3.2 : 2.5;
      if (node.is_low_confidence) return rootIds.has(node.id) ? 3.0 : 1.4;
      return rootIds.has(node.id) ? 2.8 : 1.2;
    }})
    .attr("stroke-opacity", node => rootIds.has(node.id) ? 1.0 : node.sanctioned ? 1.0 : node.is_low_confidence ? 0.95 : 0.7)
    .attr("fill-opacity", node => rootIds.has(node.id) ? 0.28 : node.is_low_confidence ? 0.14 : node.sanctioned ? 0.35 : 0.18);
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
    ${{body}}
  `;
}}

function applyViewerState() {{
  syncHiddenTypeState();
  rebuildActiveGraph();
  rebuildGraphIndexes();
  renderNodeJoin();
  const projection = projectVisibleGraph();
  allNodes.forEach(node => {{
    node._visible = projection.visibleIds.has(node.id);
  }});

  visibleEdges.length = 0;
  projection.edgeIds.forEach(edge => visibleEdges.push(edge));
  visibleEdgeSet = new Set(visibleEdges);

  positionNodes();
  syncVisibility();
  updateFocusStyling(projection.rootIds);
  renderScorePanel();
  zoomToVisible();

  const shownNodes = allNodes.filter(node => node._visible).length;
  statsEl.textContent = projection.rootIds.size || viewerState.showLowConfidence
    ? `showing ${{shownNodes}} nodes, ${{visibleEdges.length}} edges`
    : `{node_count} nodes, {edge_count} edges`;
}}

searchInput.addEventListener("input", () => {{
  viewerState.searchQuery = searchInput.value.trim();
  if (viewerState.searchQuery) viewerState.focusedNodeIds.clear();
  applyViewerState();
}});

searchInput.addEventListener("search", () => {{
  viewerState.searchQuery = searchInput.value.trim();
  if (!viewerState.searchQuery) viewerState.focusedNodeIds.clear();
  applyViewerState();
}});

showIdentitiesInput.addEventListener("change", applyViewerState);
showCompaniesInput.addEventListener("change", applyViewerState);
showCharitiesInput.addEventListener("change", applyViewerState);
showPeopleInput.addEventListener("change", applyViewerState);
showAddressesInput.addEventListener("change", applyViewerState);
showLowConfidenceInput.addEventListener("change", async () => {{
  if (showLowConfidenceInput.checked) {{
    const ok = await ensureLowConfidenceLoaded();
    if (!ok) {{
      showLowConfidenceInput.checked = false;
      viewerState.showLowConfidence = false;
    }}
  }}
  applyViewerState();
}});
indirectOnlyInput.addEventListener("change", applyViewerState);

applyViewerState();
syncLocalOverridesFromServer();
</script>
</body>
</html>"""
