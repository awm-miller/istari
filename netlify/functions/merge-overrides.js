const { getStore } = require("@netlify/blobs");

const STORE_NAME = "istari-manual-merges";
const STORE_KEY = "overrides";

function normalizeOverrides(overrides) {
  const normalized = { address: [], person: [] };
  if (!overrides || typeof overrides !== "object") {
    return normalized;
  }
  for (const kind of ["address", "person"]) {
    normalized[kind] = Array.isArray(overrides[kind])
      ? overrides[kind]
          .filter((row) => row && row.sourceId && row.targetId)
          .map((row) => ({
            sourceId: String(row.sourceId),
            targetId: String(row.targetId),
          }))
      : [];
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
  if (!["address", "person"].includes(kind)) {
    return json(400, { error: "Unsupported merge kind." });
  }
  if (!sourceId || !targetId || sourceId === targetId) {
    return json(400, { error: "Invalid merge pair." });
  }

  const alreadyExists = current[kind].some(
    (row) => row.sourceId === sourceId && row.targetId === targetId,
  );
  if (!alreadyExists) {
    current[kind].push({ sourceId, targetId });
    await store.setJSON(STORE_KEY, current);
  }

  return json(200, { overrides: current });
};
