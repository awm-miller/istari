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

async function requestModelJson(prompt) {
  const openRouterKey = String(process.env.OPENROUTER_API_KEY || "");
  if (!openRouterKey) return null;
  const baseUrl = (process.env.OPENROUTER_BASE_URL || "https://openrouter.ai/api/v1").replace(/\/$/, "");
  const model = process.env.OPENROUTER_RESOLUTION_MODEL || "~deepseek/deepseek-v4-flash-latest";
  const response = await fetch(`${baseUrl}/chat/completions`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${openRouterKey}`,
      "Content-Type": "application/json",
      "HTTP-Referer": process.env.URL || "https://projectistari.netlify.app",
      "X-Title": "Istari graph analysis",
    },
    body: JSON.stringify({
      model,
      messages: [{ role: "user", content: prompt }],
      response_format: { type: "json_object" },
      provider: { data_collection: "deny", zdr: true },
    }),
  });
  if (!response.ok) throw new Error(`OpenRouter request failed: ${response.status}`);
  const payload = await response.json();
  return tryParseJson(payload?.choices?.[0]?.message?.content || "");
}

function normalizeGraphKey(value) {
  const graph = String(value || "").trim().toLowerCase();
  if (graph === "expandedmbnames") return "expanded-mb-names";
  return /^[a-z0-9][a-z0-9-]{0,79}$/.test(graph) ? graph : "mb";
}

function graphDataPath(graphKey) {
  const normalizedGraphKey = normalizeGraphKey(graphKey);
  const staticKeys = new Set(["mb", "94-park-ave", "iums", "iran", "sevenspikes", "expanded-mb-names"]);
  return staticKeys.has(normalizedGraphKey)
    ? `/${normalizedGraphKey}/graph-data.json`
    : `/generated-graphs/${normalizedGraphKey}/graph-data.json`;
}

async function loadGraphDataForKey(graphKey, event) {
  const requestOrigin = process.env.URL || process.env.DEPLOY_PRIME_URL || new URL(event.rawUrl).origin;
  const response = await fetch(new URL(graphDataPath(graphKey), requestOrigin), {
    headers: event.headers?.cookie ? { Cookie: event.headers.cookie } : {},
  });
  if (!response.ok) throw new Error(`graph request returned ${response.status}`);
  return response.json();
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
  if (!process.env.OPENROUTER_API_KEY) {
    return fallback;
  }
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

  const document = await requestModelJson(prompt);
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

function buildSubgraphContext(snapshot = {}) {
  const nodes = (Array.isArray(snapshot.nodes) ? snapshot.nodes : []).slice(0, 80).map((node) => ({
    id: String(node?.id || ""),
    label: String(node?.label || node?.id || ""),
    kind: String(node?.kind || "unknown"),
  })).filter((node) => node.id);
  const nodeIds = new Set(nodes.map((node) => node.id));
  const evidence = [];
  const evidenceIdByKey = new Map();
  const edges = (Array.isArray(snapshot.edges) ? snapshot.edges : []).slice(0, 150).map((edge, index) => {
    const evidenceIds = [];
    const items = [edge?.evidence, ...(Array.isArray(edge?.evidence_items) ? edge.evidence_items : [])].filter((item) => item && typeof item === "object");
    for (const item of items) {
      const key = evidenceKey(item);
      if (!key.trim()) continue;
      let id = evidenceIdByKey.get(key);
      if (!id) {
        id = `e${evidence.length + 1}`;
        evidenceIdByKey.set(key, id);
        evidence.push({
          id,
          title: String(item.title || "Evidence"),
          document_url: String(item.document_url || ""),
          page_hint: String(item.page_hint || ""),
          page_number: item.page_number || null,
          notes: String(item.notes || item.filing_description || ""),
        });
      }
      if (!evidenceIds.includes(id)) evidenceIds.push(id);
    }
    return {
      id: String(edge?.id || `edge-${index + 1}`),
      source_id: String(edge?.source || ""),
      source_label: String(edge?.source_label || edge?.source || ""),
      target_id: String(edge?.target || ""),
      target_label: String(edge?.target_label || edge?.target || ""),
      phrase: String(edge?.phrase || edge?.role_type || "is linked to"),
      confidence: String(edge?.confidence || ""),
      evidence_ids: evidenceIds,
    };
  }).filter((edge) => nodeIds.has(edge.source_id) && nodeIds.has(edge.target_id));
  return { nodes, edges, evidence };
}

function fallbackSubgraphAnswer(question, context) {
  const relationships = context.edges.slice(0, 12).map((edge) => `${edge.source_label} ${edge.phrase} ${edge.target_label}`);
  return {
    answer: relationships.length
      ? `The selected subgraph contains ${relationships.join("; ")}.`
      : "The selected nodes have no visible relationship in the supplied subgraph.",
    claims: context.edges.slice(0, 12).map((edge) => ({
      text: `${edge.source_label} ${edge.phrase} ${edge.target_label}.`,
      edge_ids: [edge.id],
      evidence_ids: edge.evidence_ids,
    })),
    question,
  };
}

async function generateSubgraphAnswer(question, context) {
  const fallback = fallbackSubgraphAnswer(question, context);
  if (!process.env.OPENROUTER_API_KEY) return fallback;
  const prompt = [
    "Answer a question using only the supplied visible graph subgraph.",
    "Return JSON only with this shape:",
    JSON.stringify({ answer: "", claims: [{ text: "", edge_ids: ["edge-1"], evidence_ids: ["e1"] }] }),
    "Rules:",
    "- Do not use outside knowledge.",
    "- Every claim must cite one or more supplied edge_ids.",
    "- Use only supplied evidence_ids.",
    "- Say when the visible graph is insufficient.",
    `Question: ${question}`,
    `Visible subgraph JSON: ${JSON.stringify(context)}`,
  ].join("\n\n");
  const document = await requestModelJson(prompt);
  if (!document || typeof document !== "object") return fallback;
  const edgeIds = new Set(context.edges.map((edge) => edge.id));
  const evidenceIds = new Set(context.evidence.map((item) => item.id));
  const claims = (Array.isArray(document.claims) ? document.claims : []).map((claim) => ({
    text: String(claim?.text || ""),
    edge_ids: (Array.isArray(claim?.edge_ids) ? claim.edge_ids : []).map(String).filter((id) => edgeIds.has(id)),
    evidence_ids: (Array.isArray(claim?.evidence_ids) ? claim.evidence_ids : []).map(String).filter((id) => evidenceIds.has(id)),
  })).filter((claim) => claim.text && claim.edge_ids.length);
  return { answer: String(document.answer || fallback.answer), claims: claims.length ? claims : fallback.claims, question };
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

  const question = String(payload.question || "").trim();
  if (question) {
    const context = buildSubgraphContext(payload.subgraph || {});
    if (!context.nodes.length || !context.edges.length) return json(400, { error: "A connected visible subgraph is required." });
    try {
      const analysis = await generateSubgraphAnswer(question, context);
      return json(200, { ...analysis, context });
    } catch (error) {
      return json(200, { ...fallbackSubgraphAnswer(question, context), warning: error.message, context });
    }
  }

  const graphKey = normalizeGraphKey(payload.graph || event.queryStringParameters?.graph);
  const sourceNodeId = String(payload.sourceNodeId || payload.source_id || "");
  const targetNodeId = String(payload.targetNodeId || payload.target_id || "");
  if (!sourceNodeId || !targetNodeId || sourceNodeId === targetNodeId) {
    return json(400, { error: "Two distinct node ids are required." });
  }

  let data;
  try {
    data = await loadGraphDataForKey(graphKey, event);
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
    return json(200, {
      graph: graphKey,
      sourceNodeId,
      targetNodeId,
      warning: error.message,
      summary: fallbackSummary(context),
      claims: fallbackClaims(context),
      path: context.path,
      evidence: context.evidence,
    });
  }
};

exports._private = { normalizeGraphKey, graphDataPath, shortestPath, buildPathContext, buildSubgraphContext, fallbackSubgraphAnswer, requestModelJson };
