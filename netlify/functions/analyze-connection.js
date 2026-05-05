const fs = require("fs/promises");
const path = require("path");

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

function tryParseJson(text) {
  const raw = String(text || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_error) {
    const start = raw.indexOf("{");
    const end = raw.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try {
        return JSON.parse(raw.slice(start, end + 1));
      } catch (_error2) {
        return null;
      }
    }
    return null;
  }
}

function normalizeGraphKey(value) {
  const graph = String(value || "").trim().toLowerCase();
  if (graph === "iums") return "iums";
  if (graph === "iran") return "iran";
  if (graph === "sevenspikes") return "sevenspikes";
  if (graph === "expanded-mb-names" || graph === "expandedmbnames") return "expanded-mb-names";
  return "mb";
}

async function loadGraphDataForKey(graphKey) {
  const normalizedGraphKey = normalizeGraphKey(graphKey);
  const graphPath = path.join(process.cwd(), "netlify_graph_viewer", normalizedGraphKey, "graph-data.json");
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

function evidenceKey(evidence) {
  return [
    evidence.document_url || "",
    evidence.title || "",
    evidence.page_hint || "",
    evidence.page_number || "",
  ].join("||");
}

function buildPathContext(data, sourceNodeId, targetNodeId) {
  const nodes = Array.isArray(data.nodes) ? data.nodes : [];
  const nodeById = new Map(nodes.map((node) => [node.id, node]));
  const path = shortestPath(data, sourceNodeId, targetNodeId);
  if (!path) return null;

  const evidence = [];
  const evidenceIdByKey = new Map();
  const edgeSummaries = path.edges.map((edge) => {
    const source = nodeById.get(edge.source);
    const target = nodeById.get(edge.target);
    const evidenceIds = [];
    if (edge.evidence && typeof edge.evidence === "object") {
      const key = evidenceKey(edge.evidence);
      if (key.trim()) {
        let evidenceId = evidenceIdByKey.get(key);
        if (!evidenceId) {
          evidenceId = `e${evidence.length + 1}`;
          evidenceIdByKey.set(key, evidenceId);
          evidence.push({
            id: evidenceId,
            title: String(edge.evidence.title || "Evidence"),
            document_url: String(edge.evidence.document_url || ""),
            page_hint: String(edge.evidence.page_hint || ""),
            page_number: edge.evidence.page_number || null,
            notes: String(edge.evidence.notes || ""),
          });
        }
        evidenceIds.push(evidenceId);
      }
    }
    return {
      source_id: edge.source,
      source_label: source ? source.label : edge.source,
      target_id: edge.target,
      target_label: target ? target.label : edge.target,
      kind: edge.kind,
      phrase: edge.phrase || edge.role_type || "is linked to",
      source_provider: edge.source_provider || "",
      evidence_ids: evidenceIds,
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
    evidence,
  };
}

function fallbackSummary(context) {
  const edges = context.path.edges || [];
  if (!edges.length) return "No connection path was found in the current graph data.";
  return edges
    .map((edge) => `${edge.source_label} ${edge.phrase} ${edge.target_label}`)
    .join(". ");
}

function fallbackClaims(context) {
  return (context.path.edges || []).map((edge) => ({
    text: `${edge.source_label} ${edge.phrase} ${edge.target_label}.`,
    evidence_ids: Array.isArray(edge.evidence_ids) ? edge.evidence_ids : [],
  }));
}

async function generateAnalysis(context, sourceNodeId, targetNodeId) {
  const fallback = {
    summary: fallbackSummary(context),
    claims: fallbackClaims(context),
  };
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    return fallback;
  }

  const model =
    process.env.OPENAI_SEARCH_MODEL ||
    process.env.OPENAI_RESOLUTION_MODEL ||
    "gpt-4.1-mini";
  const baseUrl = (process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/$/, "");
  const prompt = [
    "Explain the connection between two graph nodes using only the supplied graph path and evidence.",
    "Return JSON only with this shape:",
    JSON.stringify({
      summary: "",
      claims: [{ text: "", evidence_ids: ["e1"] }],
    }),
    "Rules:",
    "- Keep the summary concise and grounded.",
    "- Each claim must be directly supported by supplied path edges.",
    "- Use only evidence ids that exist in the evidence list.",
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
  const document = tryParseJson(payload.output_text || "");
  if (!document || typeof document !== "object") {
    return fallback;
  }
  return {
    summary: String(document.summary || fallback.summary),
    claims: Array.isArray(document.claims)
      ? document.claims
          .filter((claim) => claim && claim.text)
          .map((claim) => ({
            text: String(claim.text),
            evidence_ids: Array.isArray(claim.evidence_ids)
              ? claim.evidence_ids.map((value) => String(value))
              : [],
          }))
      : fallback.claims,
  };
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

  const graphKey = normalizeGraphKey(payload.graph || event.queryStringParameters?.graph);
  const sourceNodeId = String(payload.sourceNodeId || payload.source_id || "");
  const targetNodeId = String(payload.targetNodeId || payload.target_id || "");
  if (!sourceNodeId || !targetNodeId || sourceNodeId === targetNodeId) {
    return json(400, { error: "Two distinct node ids are required." });
  }

  let data;
  try {
    data = await loadGraphDataForKey(graphKey);
  } catch (error) {
    return json(500, { error: `Graph data is unavailable: ${error.message}` });
  }

  const context = buildPathContext(data, sourceNodeId, targetNodeId);
  if (!context) {
    return json(404, { error: "No connection path found between those nodes." });
  }

  try {
    const analysis = await generateAnalysis(context, sourceNodeId, targetNodeId);
    return json(200, {
      graph: graphKey,
      sourceNodeId,
      targetNodeId,
      summary: analysis.summary,
      claims: analysis.claims,
      path: context.path,
      evidence: context.evidence,
    });
  } catch (error) {
    return json(500, {
      error: error.message,
      summary: fallbackSummary(context),
      claims: fallbackClaims(context),
      path: context.path,
      evidence: context.evidence,
    });
  }
};
