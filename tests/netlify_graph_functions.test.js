const assert = require("node:assert/strict");
const test = require("node:test");
const analysis = require("../netlify/functions/analyze-connection")._private;
const merges = require("../netlify/functions/merge-overrides")._private;

test("graph functions preserve every static and generated graph key", () => {
  for (const key of ["mb", "94-park-ave", "iums", "iran", "sevenspikes", "expanded-mb-names", "backend-rebuild-smoke"]) {
    assert.equal(analysis.normalizeGraphKey(key), key);
    assert.equal(merges.normalizeGraphKey(key), key);
  }
  assert.equal(analysis.normalizeGraphKey("../../secret"), "mb");
});

test("connection analysis loads static and generated graph paths", () => {
  assert.equal(analysis.graphDataPath("94-park-ave"), "/94-park-ave/graph-data.json");
  assert.equal(analysis.graphDataPath("backend-rebuild-smoke"), "/generated-graphs/backend-rebuild-smoke/graph-data.json");
});

test("shortest paths preserve edge endpoint referents", () => {
  const graph = {
    nodes: [{ id: "a", label: "A" }, { id: "b", label: "B" }, { id: "c", label: "C" }],
    edges: [{ source: "a", target: "b", phrase: "knows" }, { source: "b", target: "c", phrase: "directs" }],
  };
  const context = analysis.buildPathContext(graph, "a", "c");
  assert.deepEqual(context.path.node_ids, ["a", "b", "c"]);
  assert.deepEqual(context.path.edges.map(({ source_id, target_id }) => [source_id, target_id]), [["a", "b"], ["b", "c"]]);
});

test("merge override stores are isolated per graph", () => {
  assert.equal(merges.storeKeyForGraph("mb"), "overrides");
  assert.equal(merges.storeKeyForGraph("94-park-ave"), "overrides:94-park-ave");
  assert.equal(merges.storeKeyForGraph("backend-rebuild-smoke"), "overrides:backend-rebuild-smoke");
});

test("selected subgraphs retain only edges with valid node referents", () => {
  const context = analysis.buildSubgraphContext({
    nodes: [{ id: "a", label: "A" }, { id: "b", label: "B" }],
    edges: [
      { id: "ab", source: "a", target: "b", phrase: "controls", evidence: { title: "Filing", document_url: "https://example.test/filing" } },
      { id: "missing", source: "a", target: "c" },
    ],
  });
  assert.deepEqual(context.edges.map((edge) => edge.id), ["ab"]);
  const fallback = analysis.fallbackSubgraphAnswer("How?", context);
  assert.deepEqual(fallback.claims[0].edge_ids, ["ab"]);
  assert.deepEqual(fallback.claims[0].evidence_ids, ["e1"]);
});

test("resolution decisions preserve metadata and bounded audit history", () => {
  const normalized = merges.normalizeOverrides({
    organisation: [{ sourceId: "org:a", targetId: "org:b", leaderId: "org:b", sourceLabel: "A", targetLabel: "B", reason: "Same registry identifier" }],
    seed: [{ nodeId: "node-id:person:a", label: "A Person", decidedAt: "2026-01-01T00:00:00Z" }],
    rejected: [{ sourceId: "person:a", targetId: "person:b", kind: "name", sourceLabel: "A Person", targetLabel: "Another Person" }],
    audit: Array.from({ length: 205 }, (_, index) => ({ action: "merge", at: `2026-01-01T00:00:${String(index).padStart(2, "0")}Z` })),
  });
  assert.equal(normalized.organisation[0].reason, "Same registry identifier");
  assert.deepEqual(normalized.seed[0], { nodeId: "node-id:person:a", label: "A Person", decidedAt: "2026-01-01T00:00:00Z" });
  assert.equal(normalized.rejected[0].kind, "name");
  assert.equal(normalized.audit.length, 200);
});

test("graph analysis uses the configured OpenRouter chat-completions model", async () => {
  const previous = {
    key: process.env.OPENROUTER_API_KEY,
    base: process.env.OPENROUTER_BASE_URL,
    model: process.env.OPENROUTER_RESOLUTION_MODEL,
    fetch: global.fetch,
  };
  process.env.OPENROUTER_API_KEY = "test-key";
  process.env.OPENROUTER_BASE_URL = "https://openrouter.example/api/v1";
  process.env.OPENROUTER_RESOLUTION_MODEL = "deepseek/test";
  global.fetch = async (url, options) => {
    assert.equal(url, "https://openrouter.example/api/v1/chat/completions");
    const body = JSON.parse(options.body);
    assert.equal(body.model, "deepseek/test");
    assert.equal(body.messages[0].content, "question");
    assert.deepEqual(body.provider, { data_collection: "deny", zdr: true });
    return new Response(JSON.stringify({ choices: [{ message: { content: '{"answer":"grounded"}' } }] }));
  };
  try {
    assert.deepEqual(await analysis.requestModelJson("question"), { answer: "grounded" });
  } finally {
    global.fetch = previous.fetch;
    if (previous.key == null) delete process.env.OPENROUTER_API_KEY;
    else process.env.OPENROUTER_API_KEY = previous.key;
    if (previous.base == null) delete process.env.OPENROUTER_BASE_URL;
    else process.env.OPENROUTER_BASE_URL = previous.base;
    if (previous.model == null) delete process.env.OPENROUTER_RESOLUTION_MODEL;
    else process.env.OPENROUTER_RESOLUTION_MODEL = previous.model;
  }
});
