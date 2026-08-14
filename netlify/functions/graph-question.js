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
    if (start < 0 || end <= start) return null;
    try {
      return JSON.parse(raw.slice(start, end + 1));
    } catch (_error2) {
      return null;
    }
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
      "X-Title": "Istari graph question",
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

function evidenceKey(evidence) {
  return [
    evidence.document_url || "",
    evidence.title || "",
    evidence.page_hint || "",
    evidence.page_number || "",
  ].join("||");
}

function buildSubgraphContext(snapshot = {}) {
  const nodes = (Array.isArray(snapshot.nodes) ? snapshot.nodes : [])
    .slice(0, 80)
    .map((node) => ({
      id: String(node?.id || ""),
      label: String(node?.label || node?.id || ""),
      kind: String(node?.kind || "unknown"),
    }))
    .filter((node) => node.id);
  const nodeIds = new Set(nodes.map((node) => node.id));
  const evidence = [];
  const evidenceIdByKey = new Map();
  const edges = (Array.isArray(snapshot.edges) ? snapshot.edges : [])
    .slice(0, 150)
    .map((edge, index) => {
      const evidenceIds = [];
      const items = [edge?.evidence, ...(Array.isArray(edge?.evidence_items) ? edge.evidence_items : [])]
        .filter((item) => item && typeof item === "object");
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
    })
    .filter((edge) => nodeIds.has(edge.source_id) && nodeIds.has(edge.target_id));
  return { nodes, edges, evidence };
}

function fallbackSubgraphAnswer(question, context) {
  const relationships = context.edges
    .slice(0, 12)
    .map((edge) => `${edge.source_label} ${edge.phrase} ${edge.target_label}`);
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
  const claims = (Array.isArray(document.claims) ? document.claims : [])
    .map((claim) => ({
      text: String(claim?.text || ""),
      edge_ids: (Array.isArray(claim?.edge_ids) ? claim.edge_ids : []).map(String).filter((id) => edgeIds.has(id)),
      evidence_ids: (Array.isArray(claim?.evidence_ids) ? claim.evidence_ids : []).map(String).filter((id) => evidenceIds.has(id)),
    }))
    .filter((claim) => claim.text && claim.edge_ids.length);
  return {
    answer: String(document.answer || fallback.answer),
    claims: claims.length ? claims : fallback.claims,
    question,
  };
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "POST") return json(405, { error: "Method not allowed." });

  let payload;
  try {
    payload = event.body ? JSON.parse(event.body) : {};
  } catch (_error) {
    return json(400, { error: "Invalid JSON body." });
  }

  const question = String(payload.question || "").trim();
  const context = buildSubgraphContext(payload.subgraph || {});
  if (!question) return json(400, { error: "A question is required." });
  if (!context.nodes.length || !context.edges.length) {
    return json(400, { error: "A connected visible subgraph is required." });
  }
  try {
    const answer = await generateSubgraphAnswer(question, context);
    return json(200, { ...answer, context });
  } catch (error) {
    return json(200, { ...fallbackSubgraphAnswer(question, context), warning: error.message, context });
  }
};

exports._private = { buildSubgraphContext, fallbackSubgraphAnswer, requestModelJson };
