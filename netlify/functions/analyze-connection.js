const fs = require("fs/promises");
const path = require("path");

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

async function loadGraphData() {
  const graphPath = path.join(process.cwd(), "netlify_graph_viewer", "graph-data.json");
  const raw = await fs.readFile(graphPath, "utf8");
  return JSON.parse(raw);
}

function shortestPath(data, sourceNodeId, targetNodeId) {
  const edges = Array.isArray(data.edges) ? data.edges : [];
  const adjacency = new Map();
  for (const edge of edges) {
    if (!adjacency.has(edge.source)) adjacency.set(edge.source, []);
    if (!adjacency.has(edge.target)) adjacency.set(edge.target, []);
    adjacency.get(edge.source).push({ edge, next: edge.target });
    adjacency.get(edge.target).push({ edge, next: edge.source });
  }

  const queue = [sourceNodeId];
  const visited = new Set([sourceNodeId]);
  const prev = new Map();
  while (queue.length) {
    const current = queue.shift();
    if (current === targetNodeId) break;
    for (const step of adjacency.get(current) || []) {
      if (visited.has(step.next)) continue;
      visited.add(step.next);
      prev.set(step.next, { nodeId: current, edge: step.edge });
      queue.push(step.next);
    }
  }

  if (!visited.has(targetNodeId)) return null;
  const nodeIds = [targetNodeId];
  const pathEdges = [];
  let cursor = targetNodeId;
  while (cursor !== sourceNodeId) {
    const step = prev.get(cursor);
    if (!step) break;
    pathEdges.unshift(step.edge);
    cursor = step.nodeId;
    nodeIds.unshift(cursor);
  }
  return { nodeIds, edges: pathEdges };
}

function buildPathContext(data, sourceNodeId, targetNodeId) {
  const nodes = Array.isArray(data.nodes) ? data.nodes : [];
  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  const path = shortestPath(data, sourceNodeId, targetNodeId);
  if (!path) return null;

  const edgeSummaries = path.edges.map((edge) => {
    const source = nodeById.get(edge.source);
    const target = nodeById.get(edge.target);
    return {
      source_id: edge.source,
      source_label: source ? source.label : edge.source,
      target_id: edge.target,
      target_label: target ? target.label : edge.target,
      kind: edge.kind,
      phrase: edge.phrase || edge.role_type || "is linked to",
      source_provider: edge.source_provider || "",
      evidence: edge.evidence || null,
    };
  });
  const nodeSummaries = path.nodeIds.map((nodeId) => {
    const node = nodeById.get(nodeId) || { id: nodeId, label: nodeId, kind: "unknown" };
    return {
      id: node.id,
      label: node.label,
      kind: node.kind,
      lane: node.lane,
    };
  });
  return {
    path: {
      node_ids: path.nodeIds,
      nodes: nodeSummaries,
      edges: edgeSummaries,
    },
    evidence: edgeSummaries
      .map((edge) => edge.evidence)
      .filter(Boolean),
  };
}

function fallbackSummary(context) {
  const edges = context.path.edges || [];
  if (!edges.length) return "No connection path was found in the current graph data.";
  return edges
    .map((edge) => `${edge.source_label} ${edge.phrase} ${edge.target_label}`)
    .join(". ");
}

async function generateSummary(context, sourceNodeId, targetNodeId) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return fallbackSummary(context);
  }

  const model =
    process.env.OPENAI_SEARCH_MODEL ||
    process.env.OPENAI_RESOLUTION_MODEL ||
    "gpt-4.1-mini";
  const baseUrl = (process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/$/, "");
  const prompt = [
    "Explain the connection between two graph nodes using only the supplied graph path and evidence.",
    "Keep the answer concise and grounded. Do not invent facts.",
    `Source node id: ${sourceNodeId}`,
    `Target node id: ${targetNodeId}`,
    `Context JSON: ${JSON.stringify(context)}`,
  ].join("\n\n");

  const response = await fetch(`${baseUrl}/responses`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      input: prompt,
    }),
  });
  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI request failed: ${response.status} ${errorText}`);
  }
  const payload = await response.json();
  return String(payload.output_text || "").trim() || fallbackSummary(context);
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed." });
  }

  let payload = {};
  try {
    payload = event.body ? JSON.parse(event.body) : {};
  } catch (_error) {
    return json(400, { error: "Invalid JSON body." });
  }

  const sourceNodeId = String(payload.sourceNodeId || "");
  const targetNodeId = String(payload.targetNodeId || "");
  if (!sourceNodeId || !targetNodeId || sourceNodeId === targetNodeId) {
    return json(400, { error: "Two distinct node ids are required." });
  }

  let data;
  try {
    data = await loadGraphData();
  } catch (error) {
    return json(500, { error: `Graph data is unavailable: ${error.message}` });
  }

  const context = buildPathContext(data, sourceNodeId, targetNodeId);
  if (!context) {
    return json(404, { error: "No connection path found between those nodes." });
  }

  try {
    const summary = await generateSummary(context, sourceNodeId, targetNodeId);
    return json(200, {
      sourceNodeId,
      targetNodeId,
      summary,
      path: context.path,
      evidence: context.evidence,
    });
  } catch (error) {
    return json(500, { error: error.message, summary: fallbackSummary(context), path: context.path });
  }
};
