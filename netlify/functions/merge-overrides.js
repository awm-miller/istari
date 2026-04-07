const fs = require("fs");
const path = require("path");
const { connectLambda, getStore } = require("@netlify/blobs");

const STORE_NAME = "istari-manual-merges";
const STORE_KEY = "overrides";

function pushUnique(rows, sourceId, targetId) {
  const source = String(sourceId || "");
  const target = String(targetId || "");
  if (!source || !target || source === target) return;
  if (rows.some((row) => row.sourceId === source && row.targetId === target)) return;
  rows.push({ sourceId: source, targetId: target });
}

function normalizeOverrides(overrides) {
  const normalized = { address: [], name: [] };
  if (!overrides || typeof overrides !== "object") {
    return normalized;
  }

  for (const row of Array.isArray(overrides.address) ? overrides.address : []) {
    pushUnique(normalized.address, row?.sourceId, row?.targetId);
  }

  for (const kind of ["name", "person", "identity"]) {
    for (const row of Array.isArray(overrides[kind]) ? overrides[kind] : []) {
      pushUnique(normalized.name, row?.sourceId, row?.targetId);
    }
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
  const store = createStore(event);
  const current = normalizeOverrides((await store.get(STORE_KEY, { type: "json" })) || {});

  if (event.httpMethod === "GET") {
    return json(200, { overrides: current });
  }

  if (event.httpMethod !== "POST") {
    return json(405, { error: "Method not allowed." });
  }

  let payload = {};
  try {
    payload = event.body ? JSON.parse(event.body) : {};
  } catch (_error) {
    return json(400, { error: "Invalid JSON body." });
  }

  const kind = String(payload.kind || "");
  const sourceId = String(payload.sourceId || "");
  const targetId = String(payload.targetId || "");
  if (!["address", "name"].includes(kind)) {
    return json(400, { error: "Unsupported merge kind." });
  }
  if (!sourceId || !targetId || sourceId === targetId) {
    return json(400, { error: "Invalid merge pair." });
  }

  pushUnique(current[kind], sourceId, targetId);
  await store.setJSON(STORE_KEY, current);

  return json(200, { overrides: current });
};
