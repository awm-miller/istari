const ALLOWED_TARGET = /^(?:\/health|\/api\/(?:nearby-addresses\/preview|investigations(?:\/draft|\/[a-f0-9]+(?:\/(?:start|resolutions|clear))?)?|generated-graphs(?:\/[a-z0-9-]+(?:\/data)?)?)|\/generated-graphs\/[a-z0-9-]+(?:\/versions\/v\d+)?\/(?:graph-data\.json)?)$/;

function response(statusCode, body, contentType = "application/json; charset=utf-8") {
  return { statusCode, headers: { "content-type": contentType, "cache-control": "no-store" }, body: typeof body === "string" ? body : JSON.stringify(body) };
}

function targetPath(event) {
  const requested = String(event.queryStringParameters?.target || "").trim();
  const eventPath = String(event.path || "").trim();
  const rawPath = event.rawUrl ? new URL(event.rawUrl).pathname : "";
  const target = requested && requested !== "/" ? requested : [eventPath, rawPath].find((value) => value.startsWith("/api/") || value.startsWith("/generated-graphs/") || value === "/health") || "/";
  return target.startsWith("/") ? target : `/${target}`;
}

function upstreamTarget(event, target) {
  const allowed = target === "/api/investigations" ? new Set(["status", "limit"]) : new Set();
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(event.queryStringParameters || {})) {
    if (key === "target" || !allowed.has(key) || value == null) continue;
    query.set(key, String(value));
  }
  return query.size ? `${target}?${query}` : target;
}

exports.handler = async function handler(event) {
  const origin = String(process.env.ISTARI_BACKEND_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_BACKEND_PROXY_TOKEN || "");
  if (!origin || !token) return response(503, { ok: false, error: "Istari discovery service is not configured." });
  const target = targetPath(event);
  if (!ALLOWED_TARGET.test(target)) return response(400, { ok: false, error: "Unsupported discovery route." });
  const headers = { Accept: event.headers?.accept || "application/json, text/html", "X-Istari-Proxy-Token": token };
  const contentType = event.headers?.["content-type"] || event.headers?.["Content-Type"];
  if (contentType) headers["Content-Type"] = contentType;
  const body = event.body ? (event.isBase64Encoded ? Buffer.from(event.body, "base64") : event.body) : undefined;
  try {
    const upstream = await fetch(`${origin}${upstreamTarget(event, target)}`, { method: event.httpMethod || "GET", headers, body: ["GET", "HEAD"].includes(event.httpMethod) ? undefined : body });
    return response(upstream.status, await upstream.text(), upstream.headers.get("content-type") || "application/octet-stream");
  } catch (error) {
    return response(502, { ok: false, error: error.message || "Istari discovery service is unavailable." });
  }
};

exports._private = { targetPath, upstreamTarget, ALLOWED_TARGET };
