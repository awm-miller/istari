const assert = require("node:assert/strict");
const test = require("node:test");
const { handler, _private } = require("../netlify/functions/istari-sites-proxy");

test("proxy allows only Istari case and generated graph routes", () => {
  assert.equal(_private.ALLOWED_TARGET.test("/api/case-jobs/abc123/run"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/generated-graphs/32-store-street/"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/generated-graphs/32-store-street"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/nearby-addresses/preview"), true);
  assert.equal(_private.ALLOWED_TARGET.test("/api/nearby-addresses/delete"), false);
  assert.equal(_private.ALLOWED_TARGET.test("/https://example.com"), false);
  assert.equal(_private.ALLOWED_TARGET.test("/api/admin"), false);
});

test("proxy recovers the original path when Netlify drops the rewrite query", () => {
  assert.equal(_private.targetPath({
    path: "/api/generated-graphs",
    rawUrl: "https://projectistari.netlify.app/api/generated-graphs",
    queryStringParameters: {},
  }), "/api/generated-graphs");
});

test("proxy strips only the injected Cloudflare challenge from generated HTML", () => {
  const html = '<main>graph</main><script>(function(){function c(){var a="/cdn-cgi/challenge-platform/scripts/jsd/main.js";}})();</script></body>';
  assert.equal(_private.sanitizeUpstreamBody(html, "text/html; charset=utf-8"), "<main>graph</main></body>");
  assert.equal(_private.sanitizeUpstreamBody(html, "application/json"), html);
});

test("proxy adds the server-side token without exposing it in the response", async () => {
  const previousOrigin = process.env.ISTARI_SITES_ORIGIN;
  const previousToken = process.env.ISTARI_SITES_PROXY_TOKEN;
  const previousFetch = global.fetch;
  process.env.ISTARI_SITES_ORIGIN = "https://graph.example";
  process.env.ISTARI_SITES_PROXY_TOKEN = "proxy-secret";
  global.fetch = async (url, options) => {
    assert.equal(url, "https://graph.example/api/generated-graphs");
    assert.equal(options.headers["X-Istari-Proxy-Token"], "proxy-secret");
    return new Response(JSON.stringify({ ok: true, graphs: [] }), {
      headers: { "content-type": "application/json" },
    });
  };
  try {
    const result = await handler({
      httpMethod: "GET",
      headers: {},
      queryStringParameters: { target: "/api/generated-graphs" },
    });
    assert.equal(result.statusCode, 200);
    assert.equal(result.body.includes("proxy-secret"), false);
  } finally {
    global.fetch = previousFetch;
    if (previousOrigin == null) delete process.env.ISTARI_SITES_ORIGIN;
    else process.env.ISTARI_SITES_ORIGIN = previousOrigin;
    if (previousToken == null) delete process.env.ISTARI_SITES_PROXY_TOKEN;
    else process.env.ISTARI_SITES_PROXY_TOKEN = previousToken;
  }
});
