const fs = require("fs");
const path = require("path");

let groupIndexCache = null;

function json(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(body),
  };
}

function resolveGroupsPath() {
  const candidates = [
    path.join(process.cwd(), "output", "graph-data-low-confidence-groups.json"),
    path.join(__dirname, "..", "..", "output", "graph-data-low-confidence-groups.json"),
    path.join(__dirname, "output", "graph-data-low-confidence-groups.json"),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) return candidate;
  }
  return candidates[0];
}

function loadGroupIndex() {
  if (groupIndexCache) return groupIndexCache;
  const groupsPath = resolveGroupsPath();
  groupIndexCache = JSON.parse(fs.readFileSync(groupsPath, "utf8"));
  return groupIndexCache;
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "GET") {
    return json(405, { error: "Method not allowed." });
  }

  const groupId = String(event.queryStringParameters?.id || "").trim();
  if (!groupId) {
    return json(400, { error: "Missing id parameter." });
  }

  try {
    const groupIndex = loadGroupIndex();
    const detail = groupIndex?.[groupId];
    if (!detail) {
      return json(404, { error: "Low-confidence group detail not found." });
    }
    return json(200, detail);
  } catch (error) {
    return json(500, { error: `Low-confidence group detail is unavailable: ${error.message}` });
  }
};
