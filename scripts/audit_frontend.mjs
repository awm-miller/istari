import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const viewerRoot = path.join(root, "netlify_graph_viewer");
const sourceRoot = path.join(root, "src", "graph");
const failures = [];

function check(condition, message) {
  if (!condition) failures.push(message);
}

async function readJson(file) {
  return JSON.parse(await readFile(file, "utf8"));
}

function nodeIndex(nodes, context) {
  const index = new Map();
  for (const node of nodes) {
    const id = String(node?.id || "").trim();
    check(Boolean(id), `${context}: node without an id`);
    check(!index.has(id), `${context}: duplicate node id ${id}`);
    if (id) index.set(id, node);
  }
  return index;
}

function auditEdges(edges, nodes, context) {
  let labelledRelationships = 0;
  let referentLabelsPresent = 0;
  const missingReferentLabels = [];
  for (const [position, edge] of edges.entries()) {
    const source = String(edge?.source || "").trim();
    const target = String(edge?.target || "").trim();
    check(Boolean(source), `${context}: edge ${position} has no source`);
    check(Boolean(target), `${context}: edge ${position} has no target`);
    check(nodes.has(source), `${context}: edge ${position} source ${source} has no node referent`);
    check(nodes.has(target), `${context}: edge ${position} target ${target} has no node referent`);
    const tooltip = normalizedText(edge?.tooltip);
    const sourceLabel = normalizedText(nodes.get(source)?.label);
    const targetLabel = normalizedText(nodes.get(target)?.label);
    if (tooltip && sourceLabel && targetLabel) {
      labelledRelationships += 1;
      if (tooltip.includes(sourceLabel) && tooltip.includes(targetLabel)) referentLabelsPresent += 1;
      else if (missingReferentLabels.length < 5) {
        missingReferentLabels.push({
          kind: edge.kind,
          source: nodes.get(source)?.label,
          target: nodes.get(target)?.label,
          tooltip: edge.tooltip,
        });
      }
    }
  }
  return { labelledRelationships, referentLabelsPresent, missingReferentLabels };
}

function normalizedText(value) {
  return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

async function graphBundles() {
  const entries = await readdir(viewerRoot, { withFileTypes: true });
  const directories = entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name).sort();
  const bundles = [];
  for (const name of directories) {
    try {
      await readFile(path.join(viewerRoot, name, "graph-data.json"));
      bundles.push(name);
    } catch {
      // Non-graph directories are not part of the viewer release.
    }
  }
  return bundles;
}

async function auditBundle(name) {
  const directory = path.join(viewerRoot, name);
  const main = await readJson(path.join(directory, "graph-data.json"));
  const mainNodes = Array.isArray(main.nodes) ? main.nodes : [];
  const mainEdges = Array.isArray(main.edges) ? main.edges : [];
  const mainIndex = nodeIndex(mainNodes, `${name}/graph-data.json`);
  const referents = auditEdges(mainEdges, mainIndex, `${name}/graph-data.json`);

  const overlays = [
    "graph-data-low-confidence.json",
    "graph-data-low-confidence-nodes.json",
    "graph-data-open-letters.json",
  ];
  for (const overlayName of overlays) {
    const overlay = await readJson(path.join(directory, overlayName));
    const overlayNodes = Array.isArray(overlay.nodes) ? overlay.nodes : [];
    const overlayEdges = Array.isArray(overlay.edges) ? overlay.edges : [];
    const overlayIndex = nodeIndex(overlayNodes, `${name}/${overlayName}`);
    const combined = new Map([...mainIndex, ...overlayIndex]);
    auditEdges(overlayEdges, combined, `${name}/${overlayName}`);
  }
  return { name, nodes: mainNodes.length, edges: mainEdges.length, ...referents };
}

async function auditControls() {
  const [markup, app, runtime] = await Promise.all([
    readFile(path.join(sourceRoot, "viewer_markup.html"), "utf8"),
    readFile(path.join(sourceRoot, "viewer_app.js"), "utf8"),
    readFile(path.join(sourceRoot, "viewer_runtime_webgl.js"), "utf8"),
  ]);
  const controls = [
    "graph-switcher-button", "search", "mode-viewer", "mode-builder", "toggle-sidebar",
    "indirect-only", "sanctioned-only", "negative-news-only", "case-plan-submit", "case-run",
    "case-reset", "case-add-input", "case-minimum-occupancy", "case-max-addresses", "compare-clear", "details-modal-close",
    "resolution-panel", "question-selection", "question-input", "question-submit", "question-clear",
  ];
  for (const id of controls) {
    check(markup.includes(`id="${id}"`), `markup: missing intended control #${id}`);
    check(app.includes(`getElementById("${id}")`), `viewer app: #${id} is not bound`);
  }
  const rendererEvents = ["pointerdown", "pointermove", "pointerup", "click", "contextmenu", "dblclick"];
  for (const event of rendererEvents) {
    check(runtime.includes(`addEventListener("${event}"`), `renderer: missing ${event} interaction`);
  }
  assert.equal(failures.length, 0, failures.join("\n"));
}

await auditControls();
const summaries = [];
for (const bundle of await graphBundles()) summaries.push(await auditBundle(bundle));
if (failures.length) throw new Error(failures.join("\n"));
for (const summary of summaries) {
  console.log(`${summary.name}: ${summary.nodes} nodes, ${summary.edges} edges, ${summary.referentLabelsPresent}/${summary.labelledRelationships} tooltips name both referents`);
}
console.log(`Frontend audit passed for ${summaries.length} graph bundles.`);
