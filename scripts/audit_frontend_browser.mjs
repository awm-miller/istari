import assert from "node:assert/strict";
import { createReadStream } from "node:fs";
import { access, readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright-core";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "netlify_graph_viewer");
const browserCandidates = [
  process.env.PLAYWRIGHT_BROWSER_PATH,
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
].filter(Boolean);
const contentTypes = new Map([
  [".css", "text/css"], [".html", "text/html"], [".js", "text/javascript"], [".json", "application/json"],
]);
let caseStatus = "queued";
let caseCreated = false;
let caseCleared = false;
let casePollFailures = 0;
let transientPollObserved = false;
let directPlan = null;
let approvedPlan = null;
let createdGraphId = "";
let generatedGraphDeleted = false;
let deleteConfirmation = "";
let enrichmentRequest = null;
let seedBatchRequest = null;
let seedSingleRequest = null;
let mergeOverrides = {
  address: [], name: [], organisation: [], seed: [], hidden: [],
  rejected: [{ sourceId: "label:akef mahmoud abdalla dr", targetId: "label:akef mahmoud", kind: "name", sourceLabel: "AKEF, Mahmoud Abdalla, Dr", targetLabel: "Mahmoud Akef", reason: "Reviewed as different people" }],
  audit: [{ id: "audit-1", action: "reject", at: "2026-07-27T12:00:00Z", kind: "name", sourceLabel: "AKEF, Mahmoud Abdalla, Dr", targetLabel: "Mahmoud Akef" }],
};

function sendJson(response, body, status = 200) {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

async function requestJson(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : {};
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://127.0.0.1");
  if (url.pathname === "/favicon.ico") {
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/.netlify/functions/merge-overrides") {
    if (request.method === "POST") {
      const body = await requestJson(request);
      if (body.kind === "seed") {
        if (body.operation === "add_many") {
          seedBatchRequest = body;
          mergeOverrides.seed = body.rows.map((row) => ({ ...row, decidedAt: new Date().toISOString() }));
        } else {
          seedSingleRequest = body;
          if (body.operation === "remove") {
            mergeOverrides.seed = mergeOverrides.seed.filter((row) => row.nodeId !== body.nodeId);
          } else {
            mergeOverrides.seed = [
              ...mergeOverrides.seed.filter((row) => row.nodeId !== body.nodeId),
              { nodeId: body.nodeId, label: body.label, decidedAt: new Date().toISOString() },
            ];
          }
        }
      } else {
        mergeOverrides = { address: [], name: [], organisation: [], seed: mergeOverrides.seed, hidden: [], rejected: [], audit: mergeOverrides.audit };
      }
    }
    sendJson(response, { overrides: mergeOverrides });
    return;
  }
  if (url.pathname === "/api/nearby-addresses/preview" && request.method === "POST") {
    const body = await requestJson(request);
    sendJson(response, {
      ok: true,
      centre: { address: body.address, postcode: "N4 2QH", lat: 51.562927, lon: -0.105696 },
      radius_metres: body.radius_metres,
      address_count: 2,
      company_count: 4,
      addresses: [
        { address: "7-11 St Thomas's Road, London, N4 2QH", lat: 51.562927, lon: -0.105696, distance_metres: 0, companies: [{}, {}] },
        { address: "233 Seven Sisters Road, London, N4 2DA", lat: 51.563504, lon: -0.107475, distance_metres: 139, companies: [{}, {}] },
      ],
    });
    return;
  }
  if (url.pathname === "/api/generated-graphs") {
    sendJson(response, { graphs: generatedGraphDeleted ? [] : [{ id: "generated-check", title: "Generated check", path: "/generated-graphs/generated-check/" }] });
    return;
  }
  if (url.pathname === "/api/generated-graphs/generated-check" && request.method === "DELETE") {
    generatedGraphDeleted = true;
    sendJson(response, { ok: true, deleted: "generated-check" });
    return;
  }
  if (url.pathname === "/api/generated-graphs/generated-check/enrich" && request.method === "POST") {
    enrichmentRequest = await requestJson(request);
    sendJson(response, { ok: true, job: { id: "enrich123", status: "planned", progress: {}, stdout: [] } }, 201);
    return;
  }
  if (url.pathname === "/api/investigations/enrich123/start" && request.method === "POST") {
    await requestJson(request);
    sendJson(response, { ok: true, job: { id: "enrich123", status: "running", progress: { processed: 0, total: 1, percent: 0 }, stdout: [] } }, 202);
    return;
  }
  if (url.pathname === "/api/investigations/enrich123/events" && request.method === "GET") {
    response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    response.end(`event: update\ndata: ${JSON.stringify({ ok: true, job: {
      id: "enrich123", status: "completed", stage: "completed",
      progress: { processed: 1, active: 0, queued: 0, failed: 0, total: 1, nodes: 14, edges: 14, percent: 100 },
      activity: { current: [], retrying: 0, skipped: 0 },
      result: { artifact: { path: "/generated-graphs/generated-check/" } },
      stdout: [{ message: "cleanup: found 0 possible duplicate person groups", created_at: new Date().toISOString() }],
    } })}\n\n`);
    return;
  }
  if (url.pathname === "/api/investigations/draft" && request.method === "POST") {
    await requestJson(request);
    sendJson(response, {
      ok: true,
      draft: {
        title: "Audit case",
        seeds: [{ kind: "address", value: "32 Store Street, London" }],
        expansionCycles: 1,
        expandPeople: true,
        entityCeiling: 5000,
        includeFormer: true,
        nearby: { enabled: false, radiusMetres: 250, maxAddresses: 200 },
      },
    });
    return;
  }
  if (url.pathname === "/api/investigations" && request.method === "GET") {
    const jobs = caseCreated && !caseCleared ? [{
      id: "abc123",
      status: caseStatus,
      draft: approvedPlan || directPlan || { title: "Audit case", seeds: [{ kind: "address", value: "32 Store Street, London" }] },
      progress: caseStatus === "completed"
        ? { processed: 4, active: 0, queued: 0, failed: 0, total: 4, nodes: 4, edges: 3, percent: 100 }
        : { processed: 0, active: 0, queued: 1, failed: 0, total: 1, nodes: 1, edges: 0, percent: 0 },
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
      result: caseStatus === "completed" ? { artifact: { path: "/generated-graphs/audit-case/" } } : null,
    }] : [];
    sendJson(response, { ok: true, jobs });
    return;
  }
  if (url.pathname === "/api/investigations" && request.method === "POST") {
    const body = await requestJson(request);
    directPlan = body.draft;
    createdGraphId = body.graphId;
    caseCreated = true;
    caseCleared = false;
    caseStatus = "planned";
    casePollFailures = 1;
    sendJson(response, { ok: true, job: { id: "abc123", status: "planned", plan: body.draft, stdout: [{ message: "contract: ready for approval", created_at: new Date().toISOString() }] } }, 201);
    return;
  }
  if (url.pathname === "/api/investigations/abc123/start" && request.method === "POST") {
    approvedPlan = (await requestJson(request)).draft;
    caseStatus = "completed";
    sendJson(response, { ok: true, job: { id: "abc123", status: "running", stdout: [{ message: "discovery: approved", created_at: new Date().toISOString() }] } }, 202);
    return;
  }
  if (url.pathname === "/api/investigations/abc123/clear" && request.method === "DELETE") {
    if (["queued", "running"].includes(caseStatus)) {
      sendJson(response, { ok: false, error: "Cancel the active investigation before clearing it." }, 409);
      return;
    }
    caseCleared = true;
    sendJson(response, { ok: true, cleared: "abc123" });
    return;
  }
  if (url.pathname === "/api/investigations/abc123/events" && request.method === "GET") {
    response.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
    response.end(": reconnect with polling\n\n");
    return;
  }
  if (url.pathname === "/api/investigations/abc123" && request.method === "GET") {
    if (casePollFailures > 0) {
      casePollFailures -= 1;
      transientPollObserved = true;
      sendJson(response, { ok: false, error: "Temporary upstream failure" }, 500);
      return;
    }
    const plan = {
      title: "Audit case",
      seeds: [{ kind: "address", value: "32 Store Street, London" }],
      expansionCycles: 1,
      expandPeople: true,
      entityCeiling: 5000,
      includeFormer: true,
      nearby: { enabled: false, radiusMetres: 250, maxAddresses: 200 },
    };
    if (caseStatus === "completed") {
      sendJson(response, { ok: true, job: { id: "abc123", status: "completed", stage: "completed", plan, progress: { processed: 4, active: 0, queued: 0, failed: 0, total: 4, nodes: 4, edges: 3, percent: 100 }, activity: { current: [], retrying: 0, skipped: 0 }, result: { artifact: { path: "/generated-graphs/audit-case/" } }, stdout: [{ message: "complete: 4 nodes, 3 edges", created_at: new Date().toISOString() }] } });
    } else {
      caseStatus = "planned";
      sendJson(response, { ok: true, job: { id: "abc123", status: "planned", plan, stdout: [{ message: "planner: scope ready", created_at: new Date().toISOString() }] } });
    }
    return;
  }
  if (url.pathname === "/generated-graphs/generated-check/") {
    const [template, graphJson] = await Promise.all([
      readFile(path.join(root, "generated-viewer-template.html"), "utf8"),
      readFile(path.join(root, "94-park-ave", "graph-data.json"), "utf8"),
    ]);
    const graph = JSON.parse(graphJson);
    const centre = graph.nodes.find((node) => node.kind === "organisation");
    const addedNode = { id: "person:latest-round", label: "LATEST ROUND PERSON", kind: "person", lane: 4 };
    graph.nodes.push(addedNode);
    graph.edges.push({
      id: "edge:latest-round",
      source: addedNode.id,
      target: centre.id,
      kind: "role",
      phrase: "is a director of",
      tooltip: "Is a director of",
      appointed_on: "2024-01-02",
      source_provider: "Companies House",
      source_url: "/company/00000001/appointments/test-appointment",
      confidence: "high",
    });
    graph.enrichment = {
      source_graph_id: "generated-check",
      source_version: 1,
      central_node_ids: [centre.id],
      added_node_ids: [addedNode.id],
    };
    const html = template.replace(
      /(<script id="graph-data" type="application\/json">)[\s\S]*?(<\/script>)/,
      `$1${JSON.stringify(graph).replaceAll("<", "\\u003c")}$2`,
    );
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(html);
    return;
  }
  const relative = decodeURIComponent(url.pathname).replace(/^\/+/, "");
  let file = path.resolve(root, relative || "index.html");
  if (!file.startsWith(root)) {
    response.writeHead(403).end();
    return;
  }
  try {
    if ((await stat(file)).isDirectory()) file = path.join(file, "index.html");
    const metadata = await stat(file);
    response.writeHead(200, {
      "content-type": contentTypes.get(path.extname(file)) || "application/octet-stream",
      "content-length": metadata.size,
    });
    createReadStream(file).pipe(response);
  } catch {
    response.writeHead(404).end();
  }
});

await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
const port = server.address().port;
let browserPath = "";
for (const candidate of browserCandidates) {
  try {
    await access(candidate);
    browserPath = candidate;
    break;
  } catch {
    // Try the next installed browser.
  }
}
assert.ok(browserPath, "Set PLAYWRIGHT_BROWSER_PATH or install Chrome/Edge to run the browser audit");
const browser = await chromium.launch({ executablePath: browserPath, headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
const runtimeErrors = [];
page.on("pageerror", (error) => runtimeErrors.push(error.message));
page.on("console", (message) => {
  const expectedTransientPollError = transientPollObserved
    && message.text().includes("status of 500 (Internal Server Error)");
  if (message.type() === "error" && !expectedTransientPollError) runtimeErrors.push(message.text());
});

try {
  await page.goto(`http://127.0.0.1:${port}/94-park-ave/`, { waitUntil: "networkidle" });
  await page.waitForSelector(".graph-node-label");
  assert.ok(await page.locator(".graph-node-label").count() >= 10, "graph labels did not render");
  assert.match(await page.locator("#stats").innerText(), /showing \d+ nodes, \d+ edges/);

  const firstEdge = await page.evaluate(async () => (await (await fetch("graph-data.json")).json()).edges[0]);
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
  assert.match(await page.locator("#details-modal-body").innerText(), /Provider:/);
  assert.match(await page.locator("#details-modal-body").innerText(), /Provider: Companies House/);
  assert.ok(await page.getByRole("link", { name: "Open exact source" }).count());
  await page.locator("#details-modal-close").click();

  await page.locator("#graph-switcher-button").click();
  assert.equal(await page.locator('.graph-switcher-option[data-graph-key="94-park-ave"]').getAttribute("href"), "/94-park-ave/");
  assert.equal(await page.locator('.graph-switcher-option[data-graph-key="94-park-ave"]').getAttribute("aria-current"), "page");
  assert.equal(await page.locator('.graph-switcher-option.generated[data-graph-key="generated-check"]').count(), 1);
  assert.equal(
    await page.locator('.graph-switcher-option.generated[data-graph-key="generated-check"]').getAttribute("href"),
    "/generated-graphs/generated-check/",
  );
  assert.equal(await page.locator('.graph-delete-button[aria-label="Delete Generated check"] svg path').count(), 1);
  page.once("dialog", (dialog) => {
    deleteConfirmation = dialog.message();
    dialog.accept();
  });
  await page.locator('.graph-delete-button[aria-label="Delete Generated check"]').click();
  await page.waitForFunction(() => !document.querySelector('.graph-switcher-option.generated[data-graph-key="generated-check"]'));
  assert.equal(await page.locator('.graph-switcher-option.generated[data-graph-key="generated-check"]').count(), 0);
  assert.match(deleteConfirmation, /all stored graph data/i);

  const initialStats = await page.locator("#stats").innerText();
  await page.locator("#search").fill("AL-UMRAN");
  await page.waitForFunction((value) => document.querySelector("#stats")?.textContent !== value, initialStats);
  assert.ok((await page.locator("#stats").innerText()) !== initialStats, "search did not update the projection");
  await page.locator("#search").fill("");

  await page.locator("#search").fill("AL-UMRAN");
  const stageBox = await page.locator("#graph .graph-stage").boundingBox();
  assert.ok(stageBox, "graph stage has no bounds");
  await page.mouse.click(stageBox.x + 30, stageBox.y + stageBox.height - 30, { button: "right" });
  await page.getByRole("button", { name: "Add tree..." }).click();
  await page.locator("#canvas-search-input").fill("E&IT");
  await page.locator('.canvas-search-result[data-node-id="org:5"]').click();
  await page.waitForFunction(() => document.querySelector("#stats")?.textContent.includes("1 added tree"));
  const duplicateLabels = await page.locator(".graph-node-label").evaluateAll((elements) => {
    const counts = new Map();
    elements.forEach((element) => counts.set(element.dataset.nodeId, (counts.get(element.dataset.nodeId) || 0) + 1));
    return {
      duplicateCount: [...counts.values()].filter((count) => count > 1).length,
      blankCount: elements.filter((element) => !element.querySelector(".graph-node-text")?.textContent.trim()).length,
    };
  });
  assert.ok(duplicateLabels.duplicateCount > 0, "added trees did not render overlapping node instances");
  assert.equal(duplicateLabels.blankCount, 0, "an added tree rendered blank duplicate nodes");
  await page.locator("#compare-clear").click();
  await page.locator("#search").fill("");

  await page.locator('.sidebar-tab[data-tab="ranked"]').click();
  assert.ok(await page.locator("#score-panel [data-ranked-type]").count() >= 3, "ranked controls did not render");
  await page.locator('.sidebar-tab[data-tab="legend"]').click();
  await page.locator("#show-companies").uncheck();
  assert.ok(!(await page.locator("#show-companies").isChecked()), "company filter did not toggle");
  await page.locator("#show-companies").check();

  await page.locator("#show-low-confidence-nodes").check();
  assert.ok(await page.locator("#show-low-confidence-nodes").isChecked(), "low-confidence nodes did not load");
  await page.locator("#toggle-sidebar").click();
  assert.ok(!(await page.locator("#viewer-sidebar").evaluate((element) => element.classList.contains("open"))), "sidebar did not close");
  await page.locator("#toggle-sidebar").click();

  await page.locator('.sidebar-tab[data-tab="resolve"]').click();
  assert.match(await page.locator("#resolution-panel").innerText(), /Reviewed as different people/);
  await page.locator('.resolution-decision [data-resolution-index]').click();
  await page.waitForFunction(() => document.querySelector("#resolution-panel")?.textContent.includes("No manual decisions yet"));

  async function chooseNodeAction(locator, actionName) {
    const box = await locator.boundingBox();
    assert.ok(box, `${actionName}: node has no hit area`);
    await page.mouse.click(box.x + 8, box.y + (box.height / 2), { button: "right" });
    await page.getByRole("button", { name: actionName }).click();
  }
  async function ctrlSelectNodes(locators) {
    const points = [];
    for (const locator of locators) {
      const box = await locator.boundingBox();
      assert.ok(box, "selected node has no hit area");
      points.push({ x: box.x + 8, y: box.y + (box.height / 2) });
    }
    for (let index = 0; index < points.length; index += 1) {
      await page.locator(".graph-stage").dispatchEvent("click", {
        bubbles: true,
        clientX: points[index].x,
        clientY: points[index].y,
        ctrlKey: true,
      });
      await page.waitForFunction(
        (expected) => document.querySelector("#graph-selection-count")?.textContent === `${expected} selected`,
        index + 1,
      );
    }
  }
  await chooseNodeAction(sourceLabel, "Select");
  assert.equal(await page.locator("#graph-selection-count").innerText(), "1 selected");
  assert.ok(await sourceLabel.evaluate((element) => element.classList.contains("selected")), "selected node is not highlighted");
  assert.ok(!(await sourceLabel.evaluate((element) => element.classList.contains("highlight"))), "selected node still has the focus highlight");
  assert.equal(await page.locator('.sidebar-tab[data-tab="ask"]').count(), 0);
  assert.equal(await page.getByRole("button", { name: /question selection|selected subgraph/i }).count(), 0);
  await chooseNodeAction(sourceLabel, "Deselect");
  assert.ok(await page.locator("#graph-selection-actions").evaluate((element) => element.classList.contains("hidden")));

  const firstLabel = page.locator(".graph-node-label").first();
  const firstBox = await firstLabel.boundingBox();
  assert.ok(firstBox, "first node has no hit area");
  await page.mouse.click(firstBox.x + 8, firstBox.y + (firstBox.height / 2), { button: "right" });
  assert.ok(await page.locator("#context-menu").isVisible(), "node context menu did not open");
  assert.equal(await page.getByRole("button", { name: /connection analysis/i }).count(), 0);
  await page.getByRole("button", { name: "Explain claims and attribution" }).click();
  assert.ok(await page.locator("#details-modal").evaluate((element) => element.classList.contains("open")), "claims modal did not open");
  await page.locator("#details-modal-close").click();

  await page.locator('.sidebar-tab[data-tab="map"]').click();
  await page.waitForSelector("#address-map.leaflet-container", { timeout: 15_000 });

  await page.locator("#mode-builder").click();
  assert.ok(await page.locator("#builder-panel").isVisible(), "Builder did not open");
  await page.locator("#case-query").fill("Everything connected to 32 Store Street, including former appointments");
  await page.locator("#case-plan-submit").click();
  await page.waitForSelector("#case-plan:not(.hidden)").catch(async (error) => {
    const status = await page.locator("#builder-status").innerText().catch(() => "");
    throw new Error(`${error.message}\nBuilder output: ${status}\nBrowser errors: ${runtimeErrors.join(" | ")}`);
  });
  assert.match(await page.locator("#builder-feedback").innerText(), /Scope ready/);
  assert.deepEqual(await page.locator("#case-expansion option").evaluateAll((options) => options.map((option) => option.value)), ["0", "1", "2", "3", "4", "5"]);
  assert.equal(await page.locator("#case-entities").getAttribute("max"), "5000");
  assert.equal(await page.locator("#case-recipe").count(), 0, "obsolete recipe control remains");
  await page.locator("#case-nearby-enabled").check();
  assert.equal(await page.locator("#case-nearby-centre").inputValue(), "32 Store Street, London");
  assert.ok(await page.locator("#case-nearby-centre").getAttribute("readonly") !== null);
  await page.locator("#case-nearby-radius").fill("250");
  await page.waitForFunction(() => document.querySelector("#case-nearby-summary")?.textContent.includes("2 registered addresses"));
  assert.ok(await page.locator("#case-nearby-map.leaflet-container").isVisible(), "Nearby radius map is hidden");
  assert.match(await page.locator("#case-nearby-summary").innerText(), /4 companies within 250 m/);
  await page.locator("#case-add-input").click();
  assert.equal(await page.locator(".case-input").count(), 2);
  assert.equal(await page.locator(".case-input-kind").last().locator('option[value="area"]').count(), 0);
  await page.locator(".case-input-kind").last().selectOption("person");
  await page.locator(".case-input-value").last().fill("Alice Example");
  assert.equal(await page.locator("#case-expand-people").isChecked(), true);
  await page.locator("#case-expand-people").uncheck();
  await page.locator("#case-run").click();
  await page.waitForSelector("#case-open-result:not(.hidden)");
  const openGraphControl = await page.locator("#case-open-result").evaluate((element) => {
    const style = getComputedStyle(element);
    return {
      height: element.getBoundingClientRect().height,
      paddingLeft: style.paddingLeft,
      paddingRight: style.paddingRight,
    };
  });
  assert.deepEqual(openGraphControl, { height: 32, paddingLeft: "12px", paddingRight: "12px" });
  assert.deepEqual(approvedPlan?.seeds, [
    { kind: "address", value: "32 Store Street, London" },
    { kind: "person", value: "Alice Example" },
  ]);
  assert.equal(approvedPlan?.expansionCycles, 1);
  assert.equal(approvedPlan?.expandPeople, false);
  assert.equal(approvedPlan?.entityCeiling, 5000);
  assert.equal(approvedPlan?.includeFormer, true);
  assert.deepEqual(approvedPlan?.nearby, { enabled: true, radiusMetres: 250, maxAddresses: 200 });
  assert.ok(transientPollObserved, "Builder audit did not exercise transient status recovery");
  assert.match(await page.locator("#builder-status").innerText(), /complete: 4 nodes, 3 edges/);
  assert.ok(await page.locator("#case-progress").isVisible(), "Discovery progress is hidden");
  assert.equal(await page.locator("#case-progress-bar").getAttribute("value"), "100");
  assert.match(await page.locator("#case-progress-detail").innerText(), /4 nodes \/ 3 relationships \/ 4 checks complete/);
  assert.equal(await page.locator("#case-task-list .case-task-row").count(), 1);
  assert.match(await page.locator("#case-task-list .case-task-status").innerText(), /Graph ready/);
  await page.locator("#case-progress-open").click();
  assert.ok(await page.locator("#run-log-sheet").isVisible(), "Run log sheet did not open");
  assert.match(await page.locator("#builder-status").innerText(), /complete: 4 nodes, 3 edges/);
  await page.locator("#run-log-close").click();
  assert.ok(await page.locator("#run-log-sheet").isHidden(), "Run log sheet did not close");
  let taskClearConfirmation = "";
  page.once("dialog", async (dialog) => {
    taskClearConfirmation = dialog.message();
    await dialog.accept();
  });
  await page.locator('.case-task-clear[aria-label="Clear task Audit case"]').click();
  await page.waitForSelector("#case-task-list .case-task-empty");
  assert.match(taskClearConfirmation, /generated graph will not be deleted/i);
  assert.equal(await page.locator("#case-task-list .case-task-row").count(), 0);
  await page.locator("#case-reset").click();
  await page.locator("#case-direct").click();
  assert.equal(await page.locator("#case-expand-people").isChecked(), true);
  await page.locator("#case-plan-title").fill("Direct contract audit");
  await page.locator("#case-plan-id").fill("direct-contract-audit");
  await page.locator(".case-input-kind").first().selectOption("address");
  await page.locator(".case-input-value").first().fill("32 Store Street, London");
  await page.locator("#case-expansion").selectOption("0");
  await page.locator("#case-run").click();
  await page.waitForSelector("#case-open-result:not(.hidden)");
  assert.equal(directPlan?.title, "Direct contract audit");
  assert.equal(createdGraphId, "direct-contract-audit");
  assert.deepEqual(directPlan?.seeds, [{ kind: "address", value: "32 Store Street, London" }]);
  assert.equal(directPlan?.expansionCycles, 0);
  assert.equal("recipe" in directPlan, false);
  await page.locator("#mode-viewer").click();
  assert.ok(await page.locator("#builder-panel").isHidden(), "Viewer did not reopen");

  await page.goto(`http://127.0.0.1:${port}/generated-graphs/generated-check/`, { waitUntil: "networkidle" });
  await page.waitForSelector(".graph-node-label");
  assert.equal(await page.locator('.sidebar-tab[data-tab="enrich"]').count(), 0, "obsolete Enrich tool is still visible");
  const enrichmentTargets = page.locator('.graph-node-label[data-node-id^="org:"]');
  const enrichmentCentre = enrichmentTargets.first();
  const enrichmentCentreLabel = String(await enrichmentCentre.textContent()).trim();
  assert.ok(await page.locator("#graph").getByText("LATEST ROUND PERSON", { exact: true }).isVisible());
  const latestPerson = page.locator('.graph-node-label[data-node-id="person:latest-round"]');
  const latestPersonBox = await latestPerson.boundingBox();
  const enrichmentCentreBox = await enrichmentCentre.boundingBox();
  assert.ok(latestPersonBox && enrichmentCentreBox, "latest-round evidence endpoints have no hit area");
  await page.mouse.click(
    ((latestPersonBox.x + (latestPersonBox.width / 2)) + (enrichmentCentreBox.x + (enrichmentCentreBox.width / 2))) / 2,
    ((latestPersonBox.y + (latestPersonBox.height / 2)) + (enrichmentCentreBox.y + (enrichmentCentreBox.height / 2))) / 2,
    { button: "right" },
  );
  await page.getByRole("button", { name: "View relationship evidence" }).click();
  const latestEvidenceText = await page.locator("#details-modal-body").innerText();
  assert.ok(latestEvidenceText.includes(`LATEST ROUND PERSON is a director of ${enrichmentCentreLabel}.`));
  assert.match(latestEvidenceText, /Appointment date: 2024-01-02/);
  assert.equal(
    await page.getByRole("link", { name: "Open exact source" }).getAttribute("href"),
    "https://find-and-update.company-information.service.gov.uk/company/00000001/officers",
  );
  await page.locator("#details-modal-close").click();
  await chooseNodeAction(enrichmentCentre, "Hide expanded round");
  assert.equal(await page.locator("#graph").getByText("LATEST ROUND PERSON", { exact: true }).count(), 0);
  assert.match(await page.locator("#stats").innerText(), /1 latest-round node hidden/);
  await chooseNodeAction(enrichmentCentre, "Show expanded round (1)");
  assert.ok(await page.locator("#graph").getByText("LATEST ROUND PERSON", { exact: true }).isVisible());

  const promotionTargets = page.locator('.graph-node-label[data-node-id*="person"]');
  assert.ok(await promotionTargets.count() >= 2, "batch promotion requires two visible people");
  await ctrlSelectNodes([promotionTargets.nth(0), promotionTargets.nth(1)]);
  await page.waitForSelector("#graph-selection-actions:not(.hidden)");
  assert.equal(await page.locator("#graph-selection-count").innerText(), "2 selected");
  page.once("dialog", (dialog) => dialog.accept());
  await page.locator("#graph-selection-promote").click();
  await page.waitForFunction(() => document.querySelector("#graph-selection-actions")?.classList.contains("hidden"));
  assert.equal(seedBatchRequest?.operation, "add_many");
  assert.equal(seedBatchRequest?.kind, "seed");
  assert.equal(seedBatchRequest?.rows.length, 2);
  const restoreTargetId = await promotionTargets.nth(0).getAttribute("data-node-id");
  const restoreTargetLabel = String(await promotionTargets.nth(0).textContent()).trim();
  const persistedAliasKey = `label:${restoreTargetLabel.toLowerCase()}`;
  mergeOverrides.seed = [{ nodeId: persistedAliasKey, label: restoreTargetLabel }];
  await page.reload({ waitUntil: "networkidle" });
  await page.waitForSelector(".graph-node-label");
  const restoreTarget = page.locator(`.graph-node-label[data-node-id="${restoreTargetId}"]`);
  page.once("dialog", (dialog) => dialog.accept());
  await chooseNodeAction(restoreTarget, "Restore as person");
  assert.equal(seedSingleRequest?.operation, "remove");
  assert.equal(seedSingleRequest?.nodeId, persistedAliasKey);
  assert.equal(mergeOverrides.seed.length, 0);

  const directEnrichmentTarget = enrichmentTargets.nth(1);
  const stableTarget = page.locator('.graph-node-label[data-node-id*="address:"]').first();
  const stableNodeId = await stableTarget.getAttribute("data-node-id");
  const stableBox = await stableTarget.boundingBox();
  assert.ok(stableBox, "stable node has no hit area");
  await ctrlSelectNodes([directEnrichmentTarget, enrichmentTargets.nth(2)]);
  assert.equal(await page.locator("#graph-selection-count").innerText(), "2 selected");
  assert.equal(await page.locator("#graph-selection-expand").innerText(), "Expand 2");
  const collisionTarget = enrichmentTargets.nth(3);
  const collisionAnchor = enrichmentTargets.nth(4);
  const collisionBox = await collisionTarget.boundingBox();
  const anchorBox = await collisionAnchor.boundingBox();
  assert.ok(collisionBox && anchorBox, "collision test nodes have no hit area");
  await page.mouse.move(collisionBox.x + (collisionBox.width / 2), collisionBox.y + (collisionBox.height / 2));
  await page.mouse.down();
  await page.mouse.move(anchorBox.x + (anchorBox.width / 2), anchorBox.y + (anchorBox.height / 2), { steps: 5 });
  await page.mouse.up();
  const navigation = page.waitForNavigation({ waitUntil: "networkidle" });
  await page.locator("#graph-selection-expand").click();
  await navigation;
  assert.equal(enrichmentRequest?.centralNodeIds.length, 2);
  assert.deepEqual(enrichmentRequest?.scopeNodeIds, []);
  assert.equal(enrichmentRequest?.expansionCycles, 0);
  assert.equal(enrichmentRequest?.expandPeople, true);
  assert.equal(enrichmentRequest?.enrichMissingDocuments, false);
  const refreshedStableTarget = page.locator(`.graph-node-label[data-node-id="${stableNodeId}"]`).first();
  const refreshedStableBox = await refreshedStableTarget.boundingBox();
  assert.ok(refreshedStableBox, "expanded graph did not render the stable node");
  assert.ok(Math.abs(refreshedStableBox.x - stableBox.x) < 3, "stable node x-position changed after automatic refresh");
  assert.ok(Math.abs(refreshedStableBox.y - stableBox.y) < 3, "stable node y-position changed after automatic refresh");
  assert.ok(await page.locator("#graph").getByText("LATEST ROUND PERSON", { exact: true }).isVisible());
  assert.match(await page.locator("#graph-expansion-label").innerText(), /resolution checked the expanded names/i);
  const overlapCount = await page.locator(".graph-node-label").evaluateAll((elements) => {
    const boxes = elements.map((element) => element.getBoundingClientRect());
    let count = 0;
    for (let left = 0; left < boxes.length; left += 1) {
      for (let right = left + 1; right < boxes.length; right += 1) {
        const horizontal = Math.min(boxes[left].right, boxes[right].right) - Math.max(boxes[left].left, boxes[right].left);
        const vertical = Math.min(boxes[left].bottom, boxes[right].bottom) - Math.max(boxes[left].top, boxes[right].top);
        if (horizontal > 2 && vertical > 2) count += 1;
      }
    }
    return count;
  });
  assert.equal(overlapCount, 0, "automatic refresh retained overlapping node positions");
  assert.equal(await page.getByRole("button", { name: "Configure custom enrichment" }).count(), 0);

  assert.deepEqual(runtimeErrors, [], `browser errors:\n${runtimeErrors.join("\n")}`);
  console.log("Browser audit passed: rendering, evidence, aligned selection, resolution, batch promotion, multi-node expansion, automatic position-preserving refresh, Builder paths, task feedback, task history, and run logs.");
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
