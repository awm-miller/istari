import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import { chromium } from "playwright-core";

const baseUrl = String(process.env.ISTARI_LIVE_URL || "https://projectistari.netlify.app").replace(/\/$/, "");
const password = String(process.env.ISTARI_PRODUCTION_PASSWORD || "");
const officerUrl = "https://find-and-update.company-information.service.gov.uk/officers/t3nXauqERv0dGTiFzHgDDRX1RJk/appointments";
const cases = [
  { key: "address-direct", query: "Every company registered at 32 Store Street, London", kinds: ["address"], cycles: 0 },
  { key: "person", query: `Everything connected to the person at ${officerUrl}`, kinds: ["person"], cycles: 1 },
  { key: "company", query: "Everything connected to Companies House company 00000006", kinds: ["company"], cycles: 1 },
  { key: "charity", query: "Everything connected to Charity Commission charity 1136945", kinds: ["charity"], cycles: 1 },
  { key: "multiple", query: "Map everything connected to company 00000006 and charity 1136945", kinds: ["company", "charity"], cycles: 1 },
  { key: "nearby", query: "Show addresses and organisations near 32 Store Street, London within 100 metres", kinds: ["address"], cycles: 1, nearby: true },
  { key: "broad-address", query: "I want to see all individuals, charities and companies with a connection to 32 Store Street, London", kinds: ["address"], cycles: 1 },
];

const browser = await chromium.launch({ executablePath: await browserPath(), headless: true });
const page = await browser.newPage();
const completed = [];
try {
  await authenticate(page);
  const jobs = [];
  for (const scenario of cases) {
    const planned = await request(page, "/api/investigations/draft", "POST", {
      query: scenario.query,
      title: `Netlify E2E ${scenario.key}`,
    });
    const draft = planned.draft;
    assert.deepEqual(draft.seeds.map((seed) => seed.kind), scenario.kinds, `${scenario.key}: seed kinds`);
    assert.equal(draft.expansionCycles, scenario.cycles, `${scenario.key}: expansion cycles`);
    assert.equal(Boolean(draft.nearby.enabled), Boolean(scenario.nearby), `${scenario.key}: nearby`);
    draft.entityCeiling = 40;
    if (scenario.nearby) draft.nearby.maxAddresses = 3;
    const created = await request(page, "/api/investigations", "POST", {
      draft,
      graphId: `netlify-e2e-${scenario.key}`,
    });
    await request(page, `/api/investigations/${created.job.id}/start`, "POST", { draft });
    jobs.push({ ...scenario, id: created.job.id });
  }

  const pending = new Map(jobs.map((job) => [job.id, job]));
  const deadline = Date.now() + 12 * 60_000;
  while (pending.size && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    for (const [id, scenario] of [...pending]) {
      const { job } = await request(page, `/api/investigations/${id}`, "GET");
      if (!["completed", "failed", "cancelled"].includes(job.status)) continue;
      pending.delete(id);
      assert.equal(job.status, "completed", `${scenario.key}: ${job.error || job.status}`);
      completed.push({ scenario, job });
      console.log(`${scenario.key}: ${job.progress.nodes} nodes, ${job.progress.edges} edges`);
    }
  }
  assert.equal(pending.size, 0, `timed out: ${[...pending.values()].map((item) => item.key).join(", ")}`);

  for (const { scenario, job } of completed) {
    const graphId = job.result.artifact.id;
    const graph = await request(page, `/generated-graphs/${graphId}/graph-data.json`, "GET");
    const ids = new Set(graph.nodes.map((node) => String(node.id)));
    assert.ok(graph.nodes.some((node) => node.kind === "seed"), `${scenario.key}: no seed node`);
    assert.ok(graph.edges.every((edge) => ids.has(String(edge.source)) && ids.has(String(edge.target))), `${scenario.key}: broken edge referent`);
  }
  console.log("Live Netlify discovery audit passed for seven investigations.");
} finally {
  for (const { job } of completed) {
    const graphId = job.result?.artifact?.id;
    if (graphId) await request(page, `/api/generated-graphs/${graphId}`, "DELETE").catch(() => {});
  }
  await browser.close();
}

async function browserPath() {
  const candidates = [
    process.env.PLAYWRIGHT_BROWSER_PATH,
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
  ].filter(Boolean);
  for (const candidate of candidates) {
    try { await access(candidate); return candidate; } catch { /* Try the next browser. */ }
  }
  throw new Error("Set PLAYWRIGHT_BROWSER_PATH or install Chrome/Edge.");
}

async function authenticate(page) {
  await page.goto(`${baseUrl}/`, { waitUntil: "domcontentloaded" });
  if (!await page.locator('input[name="password"]').count()) return;
  assert.ok(password, "ISTARI_PRODUCTION_PASSWORD is required.");
  await page.locator('input[name="password"]').fill(password);
  await Promise.all([
    page.waitForLoadState("domcontentloaded"),
    page.getByRole("button", { name: "Submit" }).click(),
  ]);
  await page.waitForSelector("#mode-viewer", { timeout: 30_000 });
}

async function request(page, path, method, body) {
  const result = await page.evaluate(async ({ path, method, body }) => {
    const response = await fetch(path, {
      method,
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: "no-store",
    });
    const value = await response.json().catch(() => ({}));
    return { ok: response.ok, status: response.status, value };
  }, { path, method, body });
  if (!result.ok || result.value.ok === false) throw new Error(`${method} ${path}: ${result.value.error || result.status}`);
  return result.value;
}
