import assert from "node:assert/strict";
import { createReadStream } from "node:fs";
import { access, stat } from "node:fs/promises";
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
let casePollFailures = 0;
let transientPollObserved = false;
let directPlan = null;
let approvedPlan = null;
const pumpTargets = [];
let generatedGraphDeleted = false;
let deleteConfirmation = "";
let mergeOverrides = {
  address: [], name: [], organisation: [], hidden: [],
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
    if (request.method === "POST") mergeOverrides = { address: [], name: [], organisation: [], hidden: [], rejected: [], audit: mergeOverrides.audit };
    sendJson(response, { overrides: mergeOverrides });
    return;
  }
  if (url.pathname === "/.netlify/functions/analyze-connection" && request.method === "POST") {
    const body = await requestJson(request);
    if (body.question) {
      const edge = body.subgraph.edges[0];
      sendJson(response, {
        answer: "The selected nodes are connected by the cited visible relationship.",
        claims: [{ text: "The visible graph supports this connection.", edge_ids: [edge.id], evidence_ids: [] }],
        context: { nodes: body.subgraph.nodes, edges: body.subgraph.edges, evidence: [] },
      });
      return;
    }
    sendJson(response, {
      sourceNodeId: body.source_id,
      targetNodeId: body.target_id,
      summary: "The selected nodes are linked by the displayed graph path.",
      claims: [],
      evidence: [],
      path: { edges: [{ source_label: "Source", phrase: "is linked to", target_label: "Target" }] },
    });
    return;
  }
  if (url.pathname === "/.netlify/functions/istari-job-pump-background" && request.method === "POST") {
    pumpTargets.push((await requestJson(request)).job_id);
    response.writeHead(202).end();
    return;
  }
  if (url.pathname === "/api/nearby-addresses/preview" && request.method === "POST") {
    const body = await requestJson(request);
    sendJson(response, {
      ok: true,
      centre: { address: body.address, postcode: "N4 2QH", lat: 51.562927, lon: -0.105696 },
      radius_metres: body.radius_metres,
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
  if (url.pathname === "/api/case-jobs" && request.method === "POST") {
    const body = await requestJson(request);
    if (body.plan) {
      directPlan = body.plan;
      caseStatus = "planned";
      sendJson(response, {
        ok: true,
        job: {
          id: "abc123",
          status: "planned",
          plan: body.plan,
          stdout: [{ message: "contract: ready for approval", created_at: new Date().toISOString() }],
        },
      }, 202);
      return;
    }
    caseStatus = "queued";
    casePollFailures = 1;
    sendJson(response, { ok: true, job: { id: "abc123", status: "queued", stdout: [{ message: "planner: queued", created_at: new Date().toISOString() }] } }, 202);
    return;
  }
  if (url.pathname === "/api/case-jobs/abc123/run" && request.method === "POST") {
    approvedPlan = (await requestJson(request)).plan;
    caseStatus = "completed";
    sendJson(response, { ok: true, job: { id: "abc123", status: "running", stdout: [{ message: "discovery: approved", created_at: new Date().toISOString() }] } }, 202);
    return;
  }
  if (url.pathname === "/api/case-jobs/abc123" && request.method === "GET") {
    if (casePollFailures > 0) {
      casePollFailures -= 1;
      transientPollObserved = true;
      sendJson(response, { ok: false, error: "Temporary upstream failure" }, 500);
      return;
    }
    const plan = {
      id: "audit-case",
      title: "Audit case",
      recipe: "registry-light",
      inputs: [{ kind: "company", value: "00000006" }],
      policy: { max_rounds: 2, max_entities: 500, pivot_kinds: ["address", "company", "charity"], leaf_kinds: ["person"] },
      enrichments: { sanctions: true, documents: false, negative_news: false },
    };
    if (caseStatus === "completed") {
      sendJson(response, { ok: true, job: { id: "abc123", status: "completed", stage: "completed", plan, progress: { processed: 4, queued: 0, failed: 0, total: 4, nodes: 4, edges: 3, percent: 100 }, result: { artifact: { path: "/generated-graphs/audit-case/" } }, stdout: [{ message: "complete: 4 nodes, 3 edges", created_at: new Date().toISOString() }] } });
    } else {
      caseStatus = "planned";
      sendJson(response, { ok: true, job: { id: "abc123", status: "planned", plan, stdout: [{ message: "planner: scope ready", created_at: new Date().toISOString() }] } });
    }
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
  assert.equal(await page.locator('.graph-switcher-option[data-graph-key="94-park-ave"]').getAttribute("aria-current"), "page");
  assert.equal(await page.locator('.graph-switcher-option.generated[data-graph-key="generated-check"]').count(), 1);
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
  await chooseNodeAction(sourceLabel, "Add to question selection");
  await chooseNodeAction(targetLabel, "Add to question selection");
  await chooseNodeAction(targetLabel, "Ask about selected subgraph");
  await page.locator("#question-input").fill("How are these nodes connected?");
  await page.locator("#question-submit").click();
  await page.waitForSelector(".question-citation");
  assert.match(await page.locator("#question-result").innerText(), /visible graph supports this connection/i);
  await page.locator("#question-clear").click();

  const firstLabel = page.locator(".graph-node-label").first();
  const firstBox = await firstLabel.boundingBox();
  assert.ok(firstBox, "first node has no hit area");
  await page.mouse.click(firstBox.x + 8, firstBox.y + (firstBox.height / 2), { button: "right" });
  assert.ok(await page.locator("#context-menu").isVisible(), "node context menu did not open");
  assert.ok(await page.getByRole("button", { name: "Add to connection analysis" }).isVisible());
  await page.getByRole("button", { name: "Explain claims and attribution" }).click();
  assert.ok(await page.locator("#details-modal").evaluate((element) => element.classList.contains("open")), "claims modal did not open");
  await page.locator("#details-modal-close").click();

  await page.mouse.click(firstBox.x + 8, firstBox.y + (firstBox.height / 2), { button: "right" });
  await page.getByRole("button", { name: "Add to connection analysis" }).click();
  const secondLabel = page.locator(".graph-node-label").nth(1);
  const secondBox = await secondLabel.boundingBox();
  assert.ok(secondBox, "second node has no hit area");
  await page.mouse.click(secondBox.x + 8, secondBox.y + (secondBox.height / 2), { button: "right" });
  await page.getByRole("button", { name: "Add to connection analysis" }).click();
  await page.waitForSelector(".analysis-path-item");
  assert.match(await page.locator(".analysis-path-item").innerText(), /Source is linked to Target/);

  await page.locator('.sidebar-tab[data-tab="map"]').click();
  await page.waitForSelector("#address-map.leaflet-container", { timeout: 15_000 });

  await page.locator("#mode-builder").click();
  assert.ok(await page.locator("#builder-panel").isVisible(), "Builder did not open");
  await page.locator("#case-query").fill("Investigate company 00000006");
  await page.locator("#case-plan-submit").click();
  await page.waitForSelector("#case-plan:not(.hidden)").catch(async (error) => {
    const status = await page.locator("#builder-status").innerText().catch(() => "");
    throw new Error(`${error.message}\nBuilder output: ${status}\nBrowser errors: ${runtimeErrors.join(" | ")}`);
  });
  assert.equal(await page.locator("#case-rounds").getAttribute("max"), "5");
  assert.equal(await page.locator("#case-entities").getAttribute("max"), "5000");
  await page.locator("#case-recipe").selectOption("area-clusters");
  assert.ok(await page.locator("#case-minimum-occupancy").isVisible(), "Area occupancy control is hidden");
  assert.ok(await page.locator("#case-max-addresses").isVisible(), "Area address ceiling is hidden");
  await page.locator("#case-recipe").selectOption("registry-light");
  await page.locator("#case-nearby-enabled").check();
  await page.locator("#case-nearby-centre").fill("7-11 St Thomas's Road, London, N4 2QH");
  await page.locator("#case-nearby-radius").fill("250");
  await page.waitForFunction(() => document.querySelector("#case-nearby-summary")?.textContent.includes("2 registered addresses"));
  assert.ok(await page.locator("#case-nearby-map.leaflet-container").isVisible(), "Nearby radius map is hidden");
  assert.match(await page.locator("#case-nearby-summary").innerText(), /4 companies within 250 m/);
  await page.locator("#case-add-input").click();
  assert.equal(await page.locator(".case-input").count(), 2);
  assert.equal(await page.locator(".case-input-kind").last().locator('option[value="area"]').count(), 1);
  await page.locator(".case-input-remove").last().click();
  await page.locator("#case-run").click();
  await page.waitForSelector("#case-open-result:not(.hidden)");
  assert.deepEqual(approvedPlan?.policy?.pivot_kinds, ["company", "charity"]);
  assert.equal(approvedPlan?.policy?.nearby_centre, "7-11 St Thomas's Road, London, N4 2QH");
  assert.equal(approvedPlan?.policy?.nearby_radius_metres, 250);
  assert.ok(transientPollObserved, "Builder audit did not exercise transient status recovery");
  assert.ok(pumpTargets.length >= 2 && pumpTargets.every((jobId) => jobId === "abc123"), "Builder did not target its exact job pumps");
  assert.match(await page.locator("#builder-status").innerText(), /complete: 4 nodes, 3 edges/);
  assert.ok(await page.locator("#case-progress").isVisible(), "Discovery progress is hidden");
  assert.equal(await page.locator("#case-progress-bar").getAttribute("value"), "100");
  assert.match(await page.locator("#case-progress-detail").innerText(), /4 processed \| 0 queued \| 4 nodes \| 3 edges/);
  await page.locator("#case-progress").click();
  assert.ok(await page.locator("#run-log-sheet").isVisible(), "Run log sheet did not open");
  assert.match(await page.locator("#builder-status").innerText(), /complete: 4 nodes, 3 edges/);
  await page.locator("#run-log-close").click();
  assert.ok(await page.locator("#run-log-sheet").isHidden(), "Run log sheet did not close");
  await page.locator("#case-reset").click();
  await page.locator("#case-direct").click();
  await page.locator("#case-plan-title").fill("Direct contract audit");
  await page.locator("#case-plan-id").fill("direct-contract-audit");
  await page.locator(".case-input-kind").first().selectOption("address");
  await page.locator(".case-input-value").first().fill("32 Store Street, London");
  await page.locator("#case-recipe").selectOption("address-network");
  await page.locator("#case-run").click();
  await page.waitForSelector("#case-open-result:not(.hidden)");
  assert.equal(directPlan?.title, "Direct contract audit");
  assert.equal(directPlan?.id, "direct-contract-audit");
  assert.deepEqual(directPlan?.inputs, [{ kind: "address", value: "32 Store Street, London" }]);
  assert.equal(directPlan?.recipe, "address-network");
  assert.deepEqual(approvedPlan?.policy?.pivot_kinds, ["address", "company", "charity"]);
  await page.locator("#mode-viewer").click();
  assert.ok(await page.locator("#builder-panel").isHidden(), "Viewer did not reopen");

  assert.deepEqual(runtimeErrors, [], `browser errors:\n${runtimeErrors.join("\n")}`);
  console.log("Browser audit passed: rendering, repeated edge hover, evidence, added trees, resolution, subgraph questions, and both Builder entry paths.");
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
