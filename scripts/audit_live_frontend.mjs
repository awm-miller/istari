import assert from "node:assert/strict";
import { access } from "node:fs/promises";
import { chromium } from "playwright-core";

const baseUrl = String(process.env.ISTARI_LIVE_URL || "https://projectistari.netlify.app").replace(/\/$/, "");
const password = String(process.env.ISTARI_PRODUCTION_PASSWORD || "");
const headless = String(process.env.ISTARI_HEADLESS || "1").trim() !== "0";
const browserCandidates = [
  process.env.PLAYWRIGHT_BROWSER_PATH,
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
].filter(Boolean);
const staticGraphs = ["94-park-ave", "expanded-mb-names", "iran", "iums", "mb", "sevenspikes"];

function log(message) {
  console.log(`[live-audit] ${message}`);
}

async function browserPath() {
  for (const candidate of browserCandidates) {
    try {
      await access(candidate);
      return candidate;
    } catch {
      // Try the next installed browser.
    }
  }
  throw new Error("Set PLAYWRIGHT_BROWSER_PATH or install Chrome/Edge");
}

async function authenticate(page) {
  await page.goto(`${baseUrl}/`, { waitUntil: "domcontentloaded" });
  if (await page.locator('input[name="password"]').count()) {
    assert.ok(password, "ISTARI_PRODUCTION_PASSWORD is required for the protected production site");
    await page.locator('input[name="password"]').fill(password);
    await Promise.all([
      page.waitForLoadState("domcontentloaded"),
      page.getByRole("button", { name: "Submit" }).click(),
    ]);
  }
  await page.waitForSelector("#mode-viewer", { timeout: 30_000 });
}

async function graphData(page) {
  return page.evaluate(async () => {
    const response = await fetch("graph-data.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`graph-data.json returned ${response.status}`);
    return response.json();
  });
}

function validateReferents(data, graphKey) {
  const nodes = Array.isArray(data.nodes) ? data.nodes : [];
  const edges = Array.isArray(data.edges) ? data.edges : [];
  const ids = new Set(nodes.map((node) => String(node.id)));
  assert.equal(ids.size, nodes.length, `${graphKey}: node IDs are not unique`);
  for (const [index, edge] of edges.entries()) {
    assert.ok(ids.has(String(edge.source)), `${graphKey}: edge ${index} source has no node referent`);
    assert.ok(ids.has(String(edge.target)), `${graphKey}: edge ${index} target has no node referent`);
  }
  return { nodes: nodes.length, edges: edges.length };
}

async function openGraph(page, graphKey, path = `/${graphKey}/`) {
  const started = Date.now();
  await page.goto(`${baseUrl}${path}`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector(".graph-node-label", { timeout: 60_000 });
  await page.waitForFunction(() => /^showing \d+ nodes, \d+ edges$/.test(document.querySelector("#stats")?.textContent || ""));
  const data = await graphData(page);
  const counts = validateReferents(data, graphKey);
  await page.locator("#graph-switcher-button").click();
  assert.equal(
    await page.locator(`.graph-switcher-option[data-graph-key="${graphKey}"]`).getAttribute("aria-current"),
    "page",
    `${graphKey}: graph switcher did not retain the active graph`,
  );
  await page.keyboard.press("Escape");
  log(`${graphKey}: ${counts.nodes} nodes, ${counts.edges} edges, rendered in ${Date.now() - started}ms`);
  return data;
}

async function testGraphSwitcher(page) {
  await openGraph(page, "94-park-ave");
  await page.locator("#graph-switcher-button").click();
  await page.locator('.graph-switcher-option[data-graph-key="mb"]').click();
  await page.waitForURL(/\/mb\/$/, { timeout: 60_000 });
  await page.waitForSelector(".graph-node-label", { timeout: 60_000 });
  await page.locator("#graph-switcher-button").click();
  assert.equal(await page.locator('.graph-switcher-option[data-graph-key="mb"]').getAttribute("aria-current"), "page");
  log("graph switcher navigation and retained selection passed");
}

async function selectNodeLocatorAction(page, label, actionName) {
  const box = await label.boundingBox();
  assert.ok(box, `${actionName}: node has no hit area`);
  await page.mouse.click(box.x + Math.min(8, box.width / 2), box.y + (box.height / 2), { button: "right" });
  await page.getByRole("button", { name: actionName }).click();
}

async function selectNodeAction(page, labelIndex, actionName) {
  await selectNodeLocatorAction(page, page.locator(".graph-node-label").nth(labelIndex), actionName);
}

async function testViewer(page) {
  const graph = await openGraph(page, "94-park-ave");
  const firstEdge = graph.edges[0];
  const sourceLabel = page.locator(`.graph-node-label[data-node-id="${firstEdge.source}"]`).first();
  const targetLabel = page.locator(`.graph-node-label[data-node-id="${firstEdge.target}"]`).first();
  const sourceBox = await sourceLabel.boundingBox();
  const targetBox = await targetLabel.boundingBox();
  assert.ok(sourceBox && targetBox, "edge endpoints have no rendered labels");
  const edgePoint = {
    x: ((sourceBox.x + (sourceBox.width / 2)) + (targetBox.x + (targetBox.width / 2))) / 2,
    y: ((sourceBox.y + (sourceBox.height / 2)) + (targetBox.y + (targetBox.height / 2))) / 2,
  };
  await page.mouse.move(edgePoint.x, edgePoint.y);
  await page.waitForSelector("#tooltip", { state: "visible" });
  await page.mouse.move(20, 30);
  await page.waitForSelector("#tooltip", { state: "hidden" });
  await page.mouse.move(edgePoint.x, edgePoint.y);
  await page.waitForSelector("#tooltip", { state: "visible" });
  await page.mouse.click(edgePoint.x, edgePoint.y, { button: "right" });
  await page.getByRole("button", { name: "View relationship evidence" }).click();
  assert.match(await page.locator("#details-modal-body").innerText(), /Why these nodes are connected/i);
  assert.ok(await page.getByRole("link", { name: "Open exact source" }).count());
  await page.locator("#details-modal-close").click();

  const initialStats = await page.locator("#stats").innerText();
  await page.locator("#search").fill("AL-UMRAN");
  await page.waitForFunction((value) => document.querySelector("#stats")?.textContent !== value, initialStats);
  await page.locator("#search").fill("");

  await page.locator("#search").fill("AL-UMRAN");
  const stageBox = await page.locator("#graph .graph-stage").boundingBox();
  assert.ok(stageBox, "graph stage has no bounds");
  await page.mouse.click(stageBox.x + 30, stageBox.y + stageBox.height - 30, { button: "right" });
  await page.getByRole("button", { name: "Add tree..." }).click();
  await page.locator("#canvas-search-input").fill("E&IT");
  await page.locator('.canvas-search-result[data-node-id="org:5"]').click();
  await page.waitForFunction(() => document.querySelector("#stats")?.textContent.includes("1 added tree"));
  const blankLabels = await page.locator(".graph-node-label").evaluateAll((elements) =>
    elements.filter((element) => !element.querySelector(".graph-node-text")?.textContent.trim()).length,
  );
  assert.equal(blankLabels, 0, "an added tree rendered blank duplicate nodes");
  await page.locator("#compare-clear").click();
  await page.locator("#search").fill("");

  await page.locator('.sidebar-tab[data-tab="ranked"]').click();
  assert.ok(await page.locator("#score-panel [data-ranked-mode]").count() >= 5, "ranked view is empty");
  await page.locator('.sidebar-tab[data-tab="legend"]').click();
  await page.locator("#show-companies").uncheck();
  assert.equal(await page.locator("#show-companies").isChecked(), false);
  await page.locator("#show-companies").check();
  await page.locator("#show-low-confidence-nodes").check();
  await page.locator("#toggle-sidebar").click();
  assert.equal(await page.locator("#viewer-sidebar").evaluate((element) => element.classList.contains("open")), false);
  await page.locator("#toggle-sidebar").click();

  await selectNodeAction(page, 0, "Explain claims and attribution");
  assert.ok(await page.locator("#details-modal").evaluate((element) => element.classList.contains("open")));
  await page.locator("#details-modal-close").click();
  await selectNodeAction(page, 0, "Select");
  assert.equal(await page.locator("#graph-selection-count").innerText(), "1 selected");
  assert.ok(await page.locator(".graph-node-label.selected").count());
  assert.equal(await page.getByRole("button", { name: /connection analysis/i }).count(), 0);
  assert.equal(await page.locator('.sidebar-tab[data-tab="ask"]').count(), 0);
  await selectNodeAction(page, 0, "Deselect");

  await page.locator('.sidebar-tab[data-tab="resolve"]').click();
  await page.waitForSelector("#resolution-panel");
  await page.locator('.sidebar-tab[data-tab="map"]').click();
  await page.waitForSelector("#address-map.leaflet-container", { timeout: 30_000 });
  log("hover, evidence, added trees, selection, resolution, filters, and map passed");
}

async function testFunctions(page) {
  const result = await page.evaluate(async () => {
    const [mergeResponse, catalogResponse] = await Promise.all([
      fetch("/.netlify/functions/merge-overrides?graph=94-park-ave", { cache: "no-store" }),
      fetch("/api/generated-graphs", { cache: "no-store" }),
    ]);
    return {
      mergeStatus: mergeResponse.status,
      merge: await mergeResponse.json(),
      catalogStatus: catalogResponse.status,
      catalog: await catalogResponse.json(),
    };
  });
  assert.equal(result.mergeStatus, 200);
  assert.equal(result.merge.graph, "94-park-ave");
  assert.equal(result.catalogStatus, 200);
  assert.ok(Array.isArray(result.catalog.graphs));
  log(`live functions passed; generated graph catalog contains ${result.catalog.graphs.length} entries`);
  return result.catalog.graphs;
}

async function waitForBuilder(page, target, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let previous = "";
  while (Date.now() < deadline) {
    const status = (await page.locator("#builder-status").innerText()).trim();
    const tail = status.split(/\r?\n/).slice(-3).join(" | ");
    if (tail && tail !== previous) {
      log(`builder: ${tail}`);
      previous = tail;
    }
    if (target === "planned" && await page.locator("#case-plan").evaluate((element) => !element.classList.contains("hidden"))) return;
    if (target === "completed" && await page.locator("#case-open-result").evaluate((element) => !element.classList.contains("hidden"))) return;
    if (/failed|error/i.test(status)) throw new Error(`Builder ${target} failed: ${tail}`);
    await page.waitForTimeout(2_000);
  }
  throw new Error(`Builder did not reach ${target} within ${Math.round(timeoutMs / 1000)} seconds`);
}

async function testBuilder(page) {
  await page.goto(`${baseUrl}/94-park-ave/`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector(".graph-node-label", { timeout: 60_000 });
  await page.locator("#mode-builder").click();
  await page.waitForSelector("#builder-panel:not(.hidden)");
  await page.locator("#case-query").fill(
    "Investigate everything connected to Companies House company 00000006. Include former appointments.",
  );
  await page.locator("#case-plan-submit").click();
  await waitForBuilder(page, "planned", 180_000);
  assert.ok(await page.locator(".case-input").count(), "planner returned no inputs");
  assert.equal(await page.locator(".case-input-kind").first().inputValue(), "company");
  await page.locator("#case-expansion").selectOption("1");
  await page.locator("#case-entities").fill("50");
  await page.locator("#case-include-former").check();
  await page.locator("#case-plan-title").fill("Live cutover E2E company");
  await page.locator("#case-plan-id").fill("live-cutover-e2e-company");
  await page.locator("#case-run").click();
  await waitForBuilder(page, "completed", 900_000);

  const stdout = await page.locator("#builder-status").innerText();
  assert.match(stdout, /complete:/i, "Builder stdout has no completion marker");
  const resultPath = await page.locator("#case-open-result").getAttribute("href");
  assert.ok(resultPath?.startsWith("/generated-graphs/"), `unexpected result path ${resultPath}`);
  await page.locator("#case-open-result").click();
  await page.waitForURL(/\/generated-graphs\//, { timeout: 30_000 });
  await page.waitForSelector(".graph-node-label", { timeout: 60_000 });
  const graphKey = new URL(page.url()).pathname.split("/").filter(Boolean).at(-1);
  const data = await graphData(page);
  const counts = validateReferents(data, graphKey);
  assert.ok(data.nodes.some((node) => node.kind === "person"), "People discovery produced no person nodes");
  await page.locator("#graph-switcher-button").click();
  assert.equal(await page.locator(`.graph-switcher-option[data-graph-key="${graphKey}"]`).getAttribute("aria-current"), "page");
  page.once("dialog", (dialog) => dialog.accept());
  await page.locator(`.graph-switcher-row:has(.graph-switcher-option[data-graph-key="${graphKey}"]) .graph-delete-button`).click();
  await page.waitForURL((url) => !url.pathname.startsWith("/generated-graphs/"), { timeout: 30_000 });
  log(`Builder completed and opened ${graphKey}: ${counts.nodes} nodes, ${counts.edges} edges`);
}

async function testMobile(context) {
  const page = await context.newPage();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`${baseUrl}/94-park-ave/`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector(".graph-node-label", { timeout: 60_000 });
  assert.ok(await page.locator("#mode-viewer").isVisible());
  assert.ok(await page.locator("#graph-switcher-button").isVisible());
  log("mobile viewport rendering passed");
  await page.close();
}

const browser = await chromium.launch({ executablePath: await browserPath(), headless, slowMo: headless ? 0 : 100 });
const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
const page = await context.newPage();
const runtimeErrors = [];
page.on("pageerror", (error) => runtimeErrors.push(error.message));
page.on("console", (message) => {
  if (message.type() === "error" && !message.text().startsWith("Failed to load resource:")) {
    runtimeErrors.push(message.text());
  }
});
page.on("response", (response) => {
  if (response.status() >= 400) runtimeErrors.push(`${response.status()} ${response.url()}`);
  const target = decodeURIComponent(new URL(response.url()).searchParams.get("target") || "");
  if (response.request().method() === "POST" && target === "/api/investigations") {
    void response.json().then((body) => {
      const jobId = String(body?.job?.id || "");
      if (jobId) log(`backend job ${jobId}`);
    }).catch(() => {});
  }
});

try {
  await authenticate(page);
  runtimeErrors.length = 0;
  log("password gate passed");
  await testGraphSwitcher(page);
  for (const graphKey of staticGraphs) await openGraph(page, graphKey);
  await testViewer(page);
  const generated = await testFunctions(page);
  for (const graph of generated) await openGraph(page, String(graph.id), String(graph.path));
  await testMobile(context);
  await testBuilder(page);
  assert.deepEqual(runtimeErrors, [], `browser runtime errors:\n${runtimeErrors.join("\n")}`);
  log("production frontend audit passed");
} finally {
  await browser.close();
}
