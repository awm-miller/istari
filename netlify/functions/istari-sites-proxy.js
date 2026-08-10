const ALLOWED_TARGET = /^(?:\/health|\/api\/nearby-addresses\/preview|\/api\/case-jobs(?:\/[a-f0-9]+(?:\/run)?)?|\/api\/generated-graphs(?:\/[a-z0-9-]+(?:\/data)?)?|\/generated-graphs\/[a-z0-9-]+(?:\/versions\/v\d+)?\/(?:graph-data\.json)?)$/;

function response(statusCode, body, contentType = "application/json; charset=utf-8") {
  return {
    statusCode,
    headers: {
      "content-type": contentType,
      "cache-control": "no-store",
    },
    body: typeof body === "string" ? body : JSON.stringify(body),
  };
}

function targetPath(event) {
  const requested = String(event.queryStringParameters?.target || "").trim();
  const eventPath = String(event.path || "").trim();
  const rawPath = event.rawUrl ? new URL(event.rawUrl).pathname : "";
  const target = requested && requested !== "/"
    ? requested
    : [eventPath, rawPath].find((path) => path.startsWith("/api/") || path.startsWith("/generated-graphs/") || path === "/health") || "/";
  return target.startsWith("/") ? target : `/${target}`;
}

function sanitizeUpstreamBody(body, contentType) {
  if (!String(contentType).toLowerCase().includes("text/html")) return body;
  return body.replace(
    /<script>\(function\(\)\{function c\(\)\{[\s\S]*?\/cdn-cgi\/challenge-platform\/scripts\/jsd\/main\.js[\s\S]*?<\/script>/g,
    "",
  );
}

exports.handler = async function handler(event) {
  const origin = String(process.env.ISTARI_SITES_ORIGIN || "").replace(/\/$/, "");
  const token = String(process.env.ISTARI_SITES_PROXY_TOKEN || "");
  if (!origin || !token) {
    return response(503, { ok: false, error: "Istari discovery service is not configured." });
  }

  const target = targetPath(event);
  if (!ALLOWED_TARGET.test(target)) {
    return response(400, { ok: false, error: "Unsupported discovery route." });
  }

  const headers = {
    Accept: event.headers?.accept || "application/json, text/html",
    "X-Istari-Proxy-Token": token,
  };
  const contentType = event.headers?.["content-type"] || event.headers?.["Content-Type"];
  if (contentType) headers["Content-Type"] = contentType;
  const body = event.body
    ? (event.isBase64Encoded ? Buffer.from(event.body, "base64") : event.body)
    : undefined;

  try {
    const upstream = await fetch(`${origin}${target}`, {
      method: event.httpMethod || "GET",
      headers,
      body: ["GET", "HEAD"].includes(event.httpMethod) ? undefined : body,
    });
    const upstreamBody = await upstream.text();
    const upstreamContentType = upstream.headers.get("content-type") || "application/octet-stream";
    return response(
      upstream.status,
      sanitizeUpstreamBody(upstreamBody, upstreamContentType),
      upstreamContentType,
    );
  } catch (error) {
    return response(502, { ok: false, error: error.message || "Istari discovery service is unavailable." });
  }
};

exports._private = { targetPath, sanitizeUpstreamBody, ALLOWED_TARGET };
