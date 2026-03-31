const fs = require("fs");
const path = require("path");

let detailIndexCache = null;

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

function resolveDetailsPath() {
  const candidates = [
    path.join(process.cwd(), "output", "graph-data-low-confidence-details.json"),
    path.join(__dirname, "..", "..", "output", "graph-data-low-confidence-details.json"),
    path.join(__dirname, "output", "graph-data-low-confidence-details.json"),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return candidates[0];
}

function loadDetailIndex() {
  if (detailIndexCache) return detailIndexCache;
  const detailPath = resolveDetailsPath();
  detailIndexCache = JSON.parse(fs.readFileSync(detailPath, "utf8"));
  return detailIndexCache;
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method not allowed." });
  }

  const edgeId = String(event.queryStringParameters?.id || "").trim();
  if (!edgeId) {
    return json(400, { error: "Missing id parameter." });
  }

  try {
    const detailIndex = loadDetailIndex();
    const detail = detailIndex?.[edgeId];
    if (!detail) {
      return json(404, { error: "Low-confidence edge detail not found." });
    }
    return json(200, detail);
  } catch (error) {
    return json(500, { error: `Low-confidence edge detail is unavailable: ${error.message}` });
  }
};
