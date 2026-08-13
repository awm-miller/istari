const assert = require("node:assert/strict");
const test = require("node:test");
const { handler, _private } = require("../netlify/functions/istari-backend-proxy");

test("proxy allows only Istari investigation and generated graph routes", () => {
  assert.equal(_private.ALLOWED_TARGET.test("/api/investigations/draft"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/investigations/abc123/start"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/generated-graphs/32-store-street/"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/generated-graphs/32-store-street"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/admin"), false);
});

test("proxy recovers rewritten paths and preserves only job filters", () => {
  assert.equal(_private.targetPath({ path: "/api/generated-graphs", rawUrl: "https://projectistari.netlify.app/api/generated-graphs", queryStringParameters: {} }), "/api/generated-graphs");
  const event = { queryStringParameters: { target: "/api/investigations", status: "running,failed", limit: "25", ignored: "secret" } };
  assert.equal(_private.upstreamTarget(event, "/api/investigations"), "/api/investigations?status=running%2Cfailed&limit=25");
});

test("proxy adds the server token without exposing it", async () => {
  const previousOrigin = process.env.ISTARI_BACKEND_ORIGIN;
  const previousToken = process.env.ISTARI_BACKEND_PROXY_TOKEN;
  const previousFetch = global.fetch;
  process.env.ISTARI_BACKEND_ORIGIN = "https://graph.example";
  process.env.ISTARI_BACKEND_PROXY_TOKEN = "proxy-secret";
  global.fetch = async (url, options) => {
    assert.equal(url, "https://graph.example/api/generated-graphs");
    assert.equal(options.headers["X-Istari-Proxy-Token"], "proxy-secret");
    return new Response(JSON.stringify({ ok: true, graphs: [] }), { headers: { "content-type": "application/json" } });
  };
  try {
    const result = await handler({ httpMethod: "GET", headers: {}, queryStringParameters: { target: "/api/generated-graphs" } });
    assert.equal(result.statusCode, 200);
    assert.equal(result.body.includes("proxy-secret"), false);
  } finally {
    global.fetch = previousFetch;
    if (previousOrigin == null) delete process.env.ISTARI_BACKEND_ORIGIN; else process.env.ISTARI_BACKEND_ORIGIN = previousOrigin;
    if (previousToken == null) delete process.env.ISTARI_BACKEND_PROXY_TOKEN; else process.env.ISTARI_BACKEND_PROXY_TOKEN = previousToken;
  }
});
