import assert from "node:assert/strict";
import test from "node:test";
import { config, pumpJobs } from "../netlify/functions/istari-job-pump.mjs";

test("scheduled pump calls only the protected internal backend route", async () => {
  process.env.ISTARI_SITES_ORIGIN = "https://backend.example/";
  process.env.ISTARI_SITES_PROXY_TOKEN = "server-secret";
  let request;
  const result = await pumpJobs(async (url, options) => {
    request = { url, options };
    return Response.json({ ok: true, results: [] });
  });
  assert.equal(config.schedule, "* * * * *");
  assert.equal(request.url, "https://backend.example/api/internal/pump");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.headers["X-Istari-Proxy-Token"], "server-secret");
  assert.deepEqual(result, { ok: true, results: [] });
});

test("scheduled pump fails closed when credentials are absent", async () => {
  delete process.env.ISTARI_SITES_ORIGIN;
  delete process.env.ISTARI_SITES_PROXY_TOKEN;
  await assert.rejects(() => pumpJobs(), /not configured/);
});
