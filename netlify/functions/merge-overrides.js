const fs = require("fs");
const path = require("path");
const { connectLambda, getStore } = require("@netlify/blobs");

const STORE_NAME = "istari-manual-merges";
const DEFAULT_STORE_KEY = "overrides";

function normalizeGraphKey(value) {
  const graph = String(value || "").trim().toLowerCase();
  if (graph === "iums") return "iums";
  if (graph === "iran") return "iran";
  if (graph === "sevenspikes") return "sevenspikes";
  if (graph === "expanded-mb-names" || graph === "expandedmbnames") return "expanded-mb-names";
  return "mb";
}

function storeKeyForGraph(graphKey) {
  const normalizedGraphKey = normalizeGraphKey(graphKey);
  return normalizedGraphKey === "mb" ? DEFAULT_STORE_KEY : `${DEFAULT_STORE_KEY}:${normalizedGraphKey}`;
}

function normalizeRow(sourceId, targetId, leaderId = "") {
  const source = String(sourceId || "");
  const target = String(targetId || "");
  const leader = String(leaderId || "");
  if (!source || !target || source === target) return null;
  return leader
    ? { sourceId: source, targetId: target, leaderId: leader }
    : { sourceId: source, targetId: target };
}

function normalizeHiddenRow(nodeId, label = "") {
  const node = String(nodeId || "");
  const text = String(label || "");
  if (!node) return null;
  return text ? { nodeId: node, label: text } : { nodeId: node };
}

function upsertUnique(rows, sourceId, targetId, leaderId = "") {
  const row = normalizeRow(sourceId, targetId, leaderId);
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

function removeHiddenRow(rows, nodeId) {
  const target = String(nodeId || "");
  return rows.filter((row) => row.nodeId !== target);
}

function normalizeOverrides(overrides) {
  const normalized = { address: [], name: [], organisation: [], hidden: [] };
  if (!overrides || typeof overrides !== "object") {
    return normalized;
  }

  for (const row of Array.isArray(overrides.address) ? overrides.address : []) {
    upsertUnique(normalized.address, row?.sourceId, row?.targetId, row?.leaderId);
  }

  for (const kind of ["name", "person", "identity"]) {
    for (const row of Array.isArray(overrides[kind]) ? overrides[kind] : []) {
      upsertUnique(normalized.name, row?.sourceId, row?.targetId, row?.leaderId);
    }
  }

  for (const row of Array.isArray(overrides.organisation) ? overrides.organisation : []) {
    upsertUnique(normalized.organisation, row?.sourceId, row?.targetId, row?.leaderId);
  }

  for (const row of Array.isArray(overrides.hidden) ? overrides.hidden : []) {
    upsertHiddenUnique(normalized.hidden, row?.nodeId, row?.label);
  }
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
  if (!["address", "name", "organisation", "hidden"].includes(kind)) {
    return json(400, { error: "Unsupported override kind." });
  }
  if (!["add", "remove"].includes(operation)) {
    return json(400, { error: "Unsupported override operation." });
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
  if (!sourceId || !targetId || sourceId === targetId) {
    return json(400, { error: "Invalid merge pair." });
  }

  if (operation === "remove") {
    current[kind] = removeRow(current[kind], sourceId, targetId);
  } else {
    upsertUnique(current[kind], sourceId, targetId, leaderId);
  }
  await store.setJSON(storeKey, current);

  return json(200, { graph: graphKey, overrides: current });
};
