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
