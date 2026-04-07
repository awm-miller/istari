const { getStore } = require("@netlify/blobs");

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

exports.handler = async function handler(event) {
  const store = getStore(STORE_NAME);
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
