const fs = require("fs");
const path = require("path");
const { connectLambda, getStore } = require("@netlify/blobs");

const STORE_NAME = "istari-manual-merges";
const DEFAULT_STORE_KEY = "overrides";

function normalizeGraphKey(value) {
  const graph = String(value || "").trim().toLowerCase();
  if (graph === "expandedmbnames") return "expanded-mb-names";
  return /^[a-z0-9][a-z0-9-]{0,79}$/.test(graph) ? graph : "mb";
}

function storeKeyForGraph(graphKey) {
  const normalizedGraphKey = normalizeGraphKey(graphKey);
  return normalizedGraphKey === "mb" ? DEFAULT_STORE_KEY : `${DEFAULT_STORE_KEY}:${normalizedGraphKey}`;
}

function optionalText(value) {
  return String(value || "").trim();
}

function normalizeRow(sourceId, targetId, leaderId = "", metadata = {}) {
  const source = String(sourceId || "");
  const target = String(targetId || "");
  const leader = String(leaderId || "");
  if (!source || !target || source === target) return null;
  return {
    sourceId: source,
    targetId: target,
    ...(leader ? { leaderId: leader } : {}),
    ...(optionalText(metadata.sourceLabel) ? { sourceLabel: optionalText(metadata.sourceLabel) } : {}),
    ...(optionalText(metadata.targetLabel) ? { targetLabel: optionalText(metadata.targetLabel) } : {}),
    ...(optionalText(metadata.leaderLabel) ? { leaderLabel: optionalText(metadata.leaderLabel) } : {}),
    ...(optionalText(metadata.reason) ? { reason: optionalText(metadata.reason) } : {}),
    ...(optionalText(metadata.decidedAt) ? { decidedAt: optionalText(metadata.decidedAt) } : {}),
  };
}

function normalizeHiddenRow(nodeId, label = "") {
  const node = String(nodeId || "");
  const text = String(label || "");
  if (!node) return null;
  return text ? { nodeId: node, label: text } : { nodeId: node };
}

function normalizeSeedRow(nodeId, label = "", decidedAt = "") {
  const row = normalizeHiddenRow(nodeId, label);
  if (!row) return null;
  return decidedAt ? { ...row, decidedAt: optionalText(decidedAt) } : row;
}

function upsertUnique(rows, sourceId, targetId, leaderId = "", metadata = {}) {
  const row = normalizeRow(sourceId, targetId, leaderId, metadata);
  if (!row) return;
  const existingIndex = rows.findIndex((entry) => entry.sourceId === row.sourceId && entry.targetId === row.targetId);
  if (existingIndex >= 0) {
    rows[existingIndex] = row;
    return;
  }
  rows.push(row);
}

function removeRow(rows, sourceId, targetId) {
  const source = String(sourceId || "");
  const target = String(targetId || "");
  return rows.filter((row) => !(row.sourceId === source && row.targetId === target));
}

function upsertHiddenUnique(rows, nodeId, label = "") {
  const row = normalizeHiddenRow(nodeId, label);
  if (!row) return;
  const existingIndex = rows.findIndex((entry) => entry.nodeId === row.nodeId);
  if (existingIndex >= 0) {
    rows[existingIndex] = row;
    return;
  }
  rows.push(row);
}

function upsertSeedUnique(rows, nodeId, label = "", decidedAt = "") {
  const row = normalizeSeedRow(nodeId, label, decidedAt);
  if (!row) return;
  const existingIndex = rows.findIndex((entry) => entry.nodeId === row.nodeId);
  if (existingIndex >= 0) {
    rows[existingIndex] = row;
    return;
  }
  rows.push(row);
}

function removeHiddenRow(rows, nodeId) {
  const target = String(nodeId || "");
  return rows.filter((row) => row.nodeId !== target);
}

function normalizeRejectedRow(row) {
  const normalized = normalizeRow(row?.sourceId, row?.targetId, "", row);
  if (!normalized) return null;
  return { ...normalized, kind: optionalText(row?.kind) || "name" };
}

function normalizeMergeRows(rows) {
  return (Array.isArray(rows) ? rows : [])
    .slice(0, 100)
    .map((row) => normalizeRow(row?.sourceId, row?.targetId, row?.leaderId, row))
    .filter(Boolean);
}

function normalizeAuditRow(row) {
  const action = optionalText(row?.action);
  const at = optionalText(row?.at);
  if (!action || !at) return null;
  return {
    id: optionalText(row?.id) || `${at}:${action}`,
    action,
    at,
    kind: optionalText(row?.kind),
    sourceId: optionalText(row?.sourceId),
    targetId: optionalText(row?.targetId),
    sourceLabel: optionalText(row?.sourceLabel),
    targetLabel: optionalText(row?.targetLabel),
    reason: optionalText(row?.reason),
  };
}

function appendAudit(current, entry) {
  current.audit = [...current.audit, normalizeAuditRow(entry)].filter(Boolean).slice(-200);
}

function normalizeOverrides(overrides) {
  const normalized = { address: [], name: [], organisation: [], seed: [], hidden: [], rejected: [], audit: [] };
  if (!overrides || typeof overrides !== "object") {
    return normalized;
  }

  for (const row of Array.isArray(overrides.address) ? overrides.address : []) {
    upsertUnique(normalized.address, row?.sourceId, row?.targetId, row?.leaderId, row);
  }

  for (const kind of ["name", "person", "identity"]) {
    for (const row of Array.isArray(overrides[kind]) ? overrides[kind] : []) {
      upsertUnique(normalized.name, row?.sourceId, row?.targetId, row?.leaderId, row);
    }
  }

  for (const row of Array.isArray(overrides.organisation) ? overrides.organisation : []) {
    upsertUnique(normalized.organisation, row?.sourceId, row?.targetId, row?.leaderId, row);
  }

  for (const row of Array.isArray(overrides.hidden) ? overrides.hidden : []) {
    upsertHiddenUnique(normalized.hidden, row?.nodeId, row?.label);
  }
  for (const row of Array.isArray(overrides.seed) ? overrides.seed : []) {
    upsertSeedUnique(normalized.seed, row?.nodeId, row?.label, row?.decidedAt);
  }
  for (const row of Array.isArray(overrides.rejected) ? overrides.rejected : []) {
    const normalizedRow = normalizeRejectedRow(row);
    if (normalizedRow) upsertUnique(normalized.rejected, normalizedRow.sourceId, normalizedRow.targetId, "", normalizedRow);
  }
  normalized.rejected = normalized.rejected.map((row) => ({ ...row, kind: optionalText(overrides.rejected?.find((candidate) => candidate.sourceId === row.sourceId && candidate.targetId === row.targetId)?.kind) || "name" }));
  normalized.audit = (Array.isArray(overrides.audit) ? overrides.audit : []).map(normalizeAuditRow).filter(Boolean).slice(-200);
  return normalized;
}

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

function readSiteIdFromState() {
  try {
    const statePath = path.join(process.cwd(), ".netlify", "state.json");
    const raw = fs.readFileSync(statePath, "utf8");
    const parsed = JSON.parse(raw);
    return String(parsed?.siteId || "").trim();
  } catch (_error) {
    return "";
  }
}

function fallbackStore() {
  const siteID = String(process.env.NETLIFY_SITE_ID || readSiteIdFromState() || "").trim();
  const token = String(process.env.NETLIFY_AUTH_TOKEN || process.env.NETLIFY_API_TOKEN || "").trim();
  if (!siteID || !token) {
    throw new Error("Merge overrides store is not configured.");
  }
  return getStore({
    name: STORE_NAME,
    siteID,
    token,
  });
}

function createStore(event) {
  try {
    connectLambda(event);
    return getStore(STORE_NAME);
  } catch (_error) {
    return fallbackStore();
  }
}

exports.handler = async function handler(event) {
  let payload = {};
  if (event.httpMethod === "POST") {
    try {
      payload = event.body ? JSON.parse(event.body) : {};
    } catch (_error) {
      return json(400, { error: "Invalid JSON body." });
    }
  }
  const graphKey = normalizeGraphKey(
    payload.graph
      || event.queryStringParameters?.graph
      || event.headers?.["x-istari-graph"]
      || event.headers?.["X-Istari-Graph"]
  );
  const storeKey = storeKeyForGraph(graphKey);
  const store = createStore(event);
  const current = normalizeOverrides((await store.get(storeKey, { type: "json" })) || {});

  if (event.httpMethod === "GET") {
    return json(200, { graph: graphKey, overrides: current });
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed." });
  }

  const operation = String(payload.operation || "add");
  const kind = String(payload.kind || "");
  const sourceId = String(payload.sourceId || "");
  const targetId = String(payload.targetId || "");
  const leaderId = String(payload.leaderId || "");
  const nodeId = String(payload.nodeId || payload.sourceId || "");
  const label = String(payload.label || "");
  const resolutionKind = String(payload.resolutionKind || payload.mergeKind || "name");
  const decidedAt = new Date().toISOString();
  const metadata = {
    sourceLabel: payload.sourceLabel,
    targetLabel: payload.targetLabel,
    leaderLabel: payload.leaderLabel,
    reason: payload.reason,
    decidedAt,
  };
  if (!["address", "name", "organisation", "seed", "hidden", "rejected"].includes(kind)) {
    return json(400, { error: "Unsupported override kind." });
  }
  if (!["add", "remove", "add_many"].includes(operation)) {
    return json(400, { error: "Unsupported override operation." });
  }
  if (operation === "add_many") {
    if (!["address", "name", "organisation"].includes(kind)) {
      return json(400, { error: "Batch writes require a merge kind." });
    }
    const rows = normalizeMergeRows(payload.rows);
    if (!rows.length) {
      return json(400, { error: "Batch write contains no valid merge pairs." });
    }
    rows.forEach((row, index) => {
      upsertUnique(current[kind], row.sourceId, row.targetId, row.leaderId, row);
      appendAudit(current, {
        id: `${Date.now()}:${index}:add:${kind}`,
        action: "merge",
        at: decidedAt,
        kind,
        ...row,
      });
    });
    await store.setJSON(storeKey, current);
    return json(200, { graph: graphKey, overrides: current });
  }
  if (kind === "hidden") {
    if (!nodeId) {
      return json(400, { error: "Invalid hidden node key." });
    }
    if (operation === "remove") {
      current.hidden = removeHiddenRow(current.hidden, nodeId);
    } else {
      upsertHiddenUnique(current.hidden, nodeId, label);
    }
    await store.setJSON(storeKey, current);
    return json(200, { graph: graphKey, overrides: current });
  }
  if (kind === "seed") {
    if (!nodeId) {
      return json(400, { error: "Invalid seed node key." });
    }
    if (operation === "remove") {
      current.seed = removeHiddenRow(current.seed, nodeId);
    } else {
      upsertSeedUnique(current.seed, nodeId, label, decidedAt);
    }
    appendAudit(current, {
      id: `${Date.now()}:${operation}:seed`,
      action: operation === "add" ? "promote_seed" : "undo_promote_seed",
      at: decidedAt,
      kind,
      sourceId: nodeId,
      sourceLabel: label,
    });
    await store.setJSON(storeKey, current);
    return json(200, { graph: graphKey, overrides: current });
  }
  if (kind === "rejected") {
    if (!sourceId || !targetId || sourceId === targetId || !["address", "name", "organisation"].includes(resolutionKind)) {
      return json(400, { error: "Invalid rejected pair." });
    }
    if (operation === "remove") {
      current.rejected = removeRow(current.rejected, sourceId, targetId);
    } else {
      upsertUnique(current.rejected, sourceId, targetId, "", metadata);
      const row = current.rejected.find((entry) => entry.sourceId === sourceId && entry.targetId === targetId);
      if (row) row.kind = resolutionKind;
    }
    appendAudit(current, {
      id: `${Date.now()}:${operation}:rejected`,
      action: operation === "add" ? "reject" : "undo_reject",
      at: decidedAt,
      kind: resolutionKind,
      sourceId,
      targetId,
      ...metadata,
    });
    await store.setJSON(storeKey, current);
    return json(200, { graph: graphKey, overrides: current });
  }
  if (!sourceId || !targetId || sourceId === targetId) {
    return json(400, { error: "Invalid merge pair." });
  }

  if (operation === "remove") {
    current[kind] = removeRow(current[kind], sourceId, targetId);
  } else {
    upsertUnique(current[kind], sourceId, targetId, leaderId, metadata);
  }
  appendAudit(current, {
    id: `${Date.now()}:${operation}:${kind}`,
    action: operation === "add" ? "merge" : "undo_merge",
    at: decidedAt,
    kind,
    sourceId,
    targetId,
    ...metadata,
  });
  await store.setJSON(storeKey, current);

  return json(200, { graph: graphKey, overrides: current });
};

exports._private = { normalizeGraphKey, storeKeyForGraph, normalizeOverrides, normalizeMergeRows };
