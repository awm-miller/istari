const assert = require("node:assert/strict");
const test = require("node:test");
const merges = require("../netlify/functions/merge-overrides")._private;

test("graph functions preserve every static and generated graph key", () => {
  for (const key of ["mb", "94-park-ave", "iums", "iran", "sevenspikes", "expanded-mb-names", "backend-rebuild-smoke"]) {
    assert.equal(merges.normalizeGraphKey(key), key);
  }
});

test("merge override stores are isolated per graph", () => {
  assert.equal(merges.storeKeyForGraph("mb"), "overrides");
  assert.equal(merges.storeKeyForGraph("94-park-ave"), "overrides:94-park-ave");
  assert.equal(merges.storeKeyForGraph("backend-rebuild-smoke"), "overrides:backend-rebuild-smoke");
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

test("batch merge rows are bounded and reject invalid pairs", () => {
  const rows = merges.normalizeMergeRows([
    { sourceId: "person:a", targetId: "person:b", leaderId: "person:b", sourceLabel: "A", targetLabel: "B" },
    { sourceId: "person:a", targetId: "person:a" },
    { sourceId: "", targetId: "person:b" },
  ]);
  assert.deepEqual(rows, [{
    sourceId: "person:a",
    targetId: "person:b",
    leaderId: "person:b",
    sourceLabel: "A",
    targetLabel: "B",
  }]);
});

test("batch seed rows are bounded and reject invalid nodes", () => {
  const rows = merges.normalizeSeedRows([
    { nodeId: "person:a", label: "A Person" },
    { nodeId: "", label: "Missing" },
    { nodeId: "person:b" },
  ]);
  assert.deepEqual(rows, [
    { nodeId: "person:a", label: "A Person" },
    { nodeId: "person:b" },
  ]);
  assert.equal(
    merges.normalizeSeedRows(Array.from({ length: 30 }, (_, index) => ({ nodeId: `person:${index}` }))).length,
    25,
  );
});
