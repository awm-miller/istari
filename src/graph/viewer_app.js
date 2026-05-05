(function () {
  const rawMainNodes = {nodes_json};
  const rawMainEdges = {edges_json}.filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed");
  const LOW_CONFIDENCE_DATA_URL = "graph-data-open-letters.json";
  const LOW_CONFIDENCE_NODES_DATA_URL = "graph-data-low-confidence-nodes.json";
  const GRAPH_OPTIONS = [
    { key: "mb", label: "MB", path: "/mb/" },
    { key: "iums", label: "IUMS", path: "/iums/" },
    { key: "sevenspikes", label: "Seven Spikes", path: "/sevenspikes/" },
    { key: "expanded-mb-names", label: "Expanded MB Names", path: "/expanded-mb-names/" },
  ];
  const ANALYZE_CONNECTION_URL = "/.netlify/functions/analyze-connection";
  const EVIDENCE_FILE_URL = "/.netlify/functions/evidence-file";
  const MERGE_OVERRIDES_URL = "/.netlify/functions/merge-overrides";

  const COLORS = {
    amber: 0xfacc15,
    blue: 0x58a6ff,
    green: 0x3fb950,
    pink: 0xff5fbf,
    purple: 0xb382f0,
    slate: 0x64748b,
    red: 0xff2222,
    white: 0xd0d4dc,
  };

  const container = document.getElementById("graph");
  const tooltip = document.getElementById("tooltip");
  const searchInput = document.getElementById("search");
  const graphSwitcherEl = document.querySelector(".graph-switcher");
  const graphSwitcherButtonEl = document.getElementById("graph-switcher-button");
  const graphSwitcherLabelEl = document.getElementById("graph-switcher-label");
  const graphSwitcherMenuEl = document.getElementById("graph-switcher-menu");
  const graphSwitcherOptionEls = [...document.querySelectorAll(".graph-switcher-option")];
  const modeViewerButton = document.getElementById("mode-viewer");
  const modeBuilderButton = document.getElementById("mode-builder");
  const builderPanelEl = document.getElementById("builder-panel");
  const builderFormEl = document.getElementById("builder-form");
  const builderModeInput = document.getElementById("builder-mode");
  const builderSeedNameInput = document.getElementById("builder-seed-name");
  const builderSeedNamesInput = document.getElementById("builder-seed-names");
  const builderRootsInput = document.getElementById("builder-roots");
  const builderTargetNamesInput = document.getElementById("builder-target-names");
  const builderGraphIdInput = document.getElementById("builder-graph-id");
  const builderGraphTitleInput = document.getElementById("builder-graph-title");
  const builderSaveModeInput = document.getElementById("builder-save-mode");
  const builderGraphVersionInput = document.getElementById("builder-graph-version");
  const builderNotifyEmailInput = document.getElementById("builder-notify-email");
  const builderLimitInput = document.getElementById("builder-limit");
  const builderRefreshGraphsButton = document.getElementById("builder-refresh-graphs");
  const builderGraphListEl = document.getElementById("builder-graph-list");
  const builderStatusEl = document.getElementById("builder-status");
  const compareSummaryEl = document.getElementById("compare-summary");
  const compareSummaryLabelEl = document.getElementById("compare-summary-label");
  const compareClearButton = document.getElementById("compare-clear");
  const canvasSearchPopoverEl = document.getElementById("canvas-search-popover");
  const canvasSearchTitleEl = document.getElementById("canvas-search-title");
  const canvasSearchInput = document.getElementById("canvas-search-input");
  const canvasSearchResultsEl = document.getElementById("canvas-search-results");
  const statsEl = document.getElementById("stats");
  const legendEl = document.getElementById("legend");
  const compactLegendEl = document.getElementById("compact-legend");
  const contextMenuEl = document.getElementById("context-menu");
  const sidebarEl = document.getElementById("viewer-sidebar");
  const toggleSidebarButton = document.getElementById("toggle-sidebar");
  const sidebarTabEls = [...document.querySelectorAll(".sidebar-tab")];
  const sidebarPaneEls = [...document.querySelectorAll(".sidebar-pane")];
  const scorePanelEl = document.getElementById("score-panel");
  const indirectOnlyInput = document.getElementById("indirect-only");
  const sanctionedOnlyInput = document.getElementById("sanctioned-only");
  const negativeNewsOnlyInput = document.getElementById("negative-news-only");
  const detailsModalEl = document.getElementById("details-modal");
  const detailsModalTitleEl = document.getElementById("details-modal-title");
  const detailsModalStatusEl = document.getElementById("details-modal-status");
  const detailsModalBodyEl = document.getElementById("details-modal-body");
  const detailsModalCloseEl = document.getElementById("details-modal-close");
  const ADDRESS_COORDINATES_URL = "address-coordinates.json";
  const currentGraphKey = detectGraphKey(window.location.pathname);
  const BUILDER_API_BASE = String(window.ISTARI_API_BASE || "").replace(/\/$/, "");

  let showIdentitiesInput;
  let showCompaniesInput;
  let showCharitiesInput;
  let showPeopleInput;
  let showAddressesInput;
  let showLowConfidenceInput;
  let showLowConfidenceNodesInput;

  let baseNodes = rawMainNodes.slice();
  let baseEdges = rawMainEdges.slice();
  let baseNodeById = new Map(baseNodes.map((node) => [node.id, node]));
  let baseEdgesByNodeId = new Map();
  let allNodes = baseNodes.slice();
  let allEdges = baseEdges.slice();
  let visibleNodes = [];
  let visibleEdges = [];
  let lowConfidenceNodes = [];
  let lowConfidenceEdges = [];
  let lowConfidenceLoaded = false;
  let lowConfidenceLoadingPromise = null;
  let lowConfidenceNodeById = new Map();
  let lowConfidenceEdgesByNodeId = new Map();
  let lowConfidenceOrgNodes = [];
  let lowConfidenceOrgEdges = [];
  let lowConfidenceOrgLoaded = false;
  let lowConfidenceOrgLoadingPromise = null;
  let lowConfidenceOrgNodeById = new Map();
  let lowConfidenceOrgEdgesByNodeId = new Map();

  let nodeById = new Map();
  let edgesByNodeId = new Map();
  let directEdgePairs = new Set();
  let orgLinkIds = new Map();
  let orgAddressIds = new Map();
  let addressOrgIds = new Map();
  let indirectIdentityIdsByOrg = new Map();
  let renderer = null;
  let addressMap = null;
  let addressMarkersLayer = null;
  let addressMarkerByNodeId = new Map();
  let addressCoordinateByNodeId = new Map();
  let addressCoordinatesLoaded = false;
  let addressCoordinatesLoadingPromise = null;
  let mergeOverrides = { address: [], name: [], organisation: [], hidden: [] };
  let mergeOverridesLoadingPromise = null;
  let canvasSearchAnchor = { x: 0, y: 0 };
  let generatedGraphs = [];

  const viewerState = {
    searchQuery: "",
    focusedNodeIds: new Set(),
    extraRootIds: [],
    expandedHiddenConnections: [],
    hiddenTypes: new Set(),
    showLowConfidence: false,
    showLowConfidenceNodes: false,
    showIndirectOnly: false,
    showSanctionedOnly: false,
    showNegativeNewsOnly: false,
    analysisNodeIds: [],
    pendingMergeNodeId: "",
    expandedLowConfidenceNodeIds: new Set(),
    rankedCategory: "people",
  };

  const measureCtx = document.createElement("canvas").getContext("2d");

  function detectGraphKey(pathname) {
    const path = String(pathname || "").toLowerCase();
    if (path.startsWith("/iums/") || path === "/iums") return "iums";
    if (path.startsWith("/sevenspikes/") || path === "/sevenspikes") return "sevenspikes";
    if (path.startsWith("/expanded-mb-names/") || path === "/expanded-mb-names") return "expanded-mb-names";
    if (path.startsWith("/mb/") || path === "/mb") return "mb";
    return "mb";
  }

  function graphFunctionUrl(baseUrl) {
    const url = new URL(baseUrl, window.location.origin);
    url.searchParams.set("graph", currentGraphKey);
    return url.toString();
  }

  function builderApiUrl(path) {
    return `${BUILDER_API_BASE}${path}`;
  }

  function splitLines(value) {
    return String(value || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
  }

  function sanitizeBuilderGraphId(value) {
    let safe = String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
    while (safe.includes("--")) safe = safe.replaceAll("--", "-");
    return safe.slice(0, 80);
  }

  function builderGraphKey() {
    return sanitizeBuilderGraphId(
      builderGraphIdInput?.value
      || builderGraphTitleInput?.value
      || builderSeedNameInput?.value
      || splitLines(builderSeedNamesInput?.value)[0]
      || "",
    );
  }

  function versionNumber(value) {
    const number = Number(String(value || "").replace(/^v/i, ""));
    return Number.isFinite(number) && number > 0 ? number : 0;
  }

  function nextBuilderVersion() {
    const graphKey = builderGraphKey();
    const graph = generatedGraphs.find((entry) => String(entry.id || "") === graphKey);
    const versions = Array.isArray(graph?.versions) ? graph.versions : [];
    const latest = versions.reduce((max, version) => Math.max(max, versionNumber(version.version)), 0);
    return latest + 1;
  }

  function updateBuilderVersionInput() {
    if (!builderGraphVersionInput) return;
    const saveMode = String(builderSaveModeInput?.value || "new_version");
    if (saveMode === "overwrite_version") {
      builderGraphVersionInput.readOnly = false;
      builderGraphVersionInput.placeholder = "Version";
      if (!builderGraphVersionInput.value) {
        builderGraphVersionInput.value = String(Math.max(1, nextBuilderVersion() - 1));
      }
      return;
    }
    builderGraphVersionInput.value = String(nextBuilderVersion());
    builderGraphVersionInput.readOnly = true;
    builderGraphVersionInput.placeholder = "Auto";
  }

  function setBuilderStatus(message, isError = false) {
    if (!builderStatusEl) return;
    builderStatusEl.textContent = message;
    builderStatusEl.classList.toggle("error", !!isError);
  }

  function setAppMode(mode) {
    const isBuilder = mode === "builder";
    document.body.classList.toggle("builder-mode", isBuilder);
    builderPanelEl?.classList.toggle("hidden", !isBuilder);
    modeViewerButton?.classList.toggle("active", !isBuilder);
    modeBuilderButton?.classList.toggle("active", isBuilder);
    if (!isBuilder && renderer) {
      window.requestAnimationFrame(() => applyViewerState());
    }
  }

  function builderPayload() {
    const mode = String(builderModeInput?.value || "name_seed");
    const saveMode = String(builderSaveModeInput?.value || "new_version");
    const payload = {
      mode,
      seed_name: String(builderSeedNameInput?.value || "").trim(),
      seed_names: splitLines(builderSeedNamesInput?.value),
      roots: splitLines(builderRootsInput?.value),
      target_names: splitLines(builderTargetNamesInput?.value),
      graph_id: String(builderGraphIdInput?.value || builderGraphTitleInput?.value || "").trim(),
      graph_title: String(builderGraphTitleInput?.value || "").trim(),
      save_mode: saveMode,
      notify_email: String(builderNotifyEmailInput?.value || "").trim(),
      limit: Number(builderLimitInput?.value || 30),
    };
    if (saveMode === "overwrite_version") {
      payload.graph_version = String(builderGraphVersionInput?.value || "").trim();
    }
    return payload;
  }

  async function postBuilderJson(path, payload) {
    const response = await fetch(builderApiUrl(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || `Request failed with ${response.status}`);
    }
    return data;
  }

  async function submitBuilderJob() {
    setBuilderStatus("Submitting graph build...");
    const data = await postBuilderJson("/api/tree-jobs", builderPayload());
    const job = data.job || {};
    setBuilderStatus(`Graph build queued.\nJob ID: ${job.id || "unknown"}\nYou will receive an email when it is ready if SMTP is configured.`);
    if (job.id) pollBuilderJob(job.id).catch((error) => console.warn("Job polling failed", error));
  }

  async function pollBuilderJob(jobId) {
    for (let attempt = 0; attempt < 120; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 5000));
      const response = await fetch(builderApiUrl(`/api/tree-jobs/${encodeURIComponent(jobId)}`));
      const data = await response.json().catch(() => ({}));
      const job = data.job || {};
      if (job.status === "completed") {
        const graph = job.result?.graph || {};
        const path = graph.path || "";
        setBuilderStatus(path ? `Graph ready.\n${graph.title || jobId}\nOpen: ${path}` : `Graph job ${jobId} completed.`);
        await loadGeneratedGraphOptions();
        return;
      }
      if (job.status === "failed") {
        setBuilderStatus(job.error || `Graph job ${jobId} failed.`, true);
        return;
      }
      setBuilderStatus(`Graph job ${jobId} is ${job.status || "running"}...`);
    }
    setBuilderStatus(`Graph job ${jobId} is still running. You will receive an email when it is ready.`);
  }

  async function deleteBuilderJson(path) {
    const response = await fetch(builderApiUrl(path), { method: "DELETE" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) {
      throw new Error(data.error || `Delete failed with ${response.status}`);
    }
    return data;
  }

  function currentGraphOption() {
    return GRAPH_OPTIONS.find((option) => option.key === currentGraphKey) || GRAPH_OPTIONS[0];
  }

  function setGraphSwitcherOpen(isOpen) {
    if (!graphSwitcherEl || !graphSwitcherButtonEl || !graphSwitcherMenuEl) return;
    graphSwitcherEl.classList.toggle("open", !!isOpen);
    graphSwitcherMenuEl.classList.toggle("hidden", !isOpen);
    graphSwitcherButtonEl.setAttribute("aria-expanded", isOpen ? "true" : "false");
  }

  function initGraphSwitcher() {
    if (!graphSwitcherEl || !graphSwitcherButtonEl || !graphSwitcherLabelEl || !graphSwitcherMenuEl) return;
    const activeOption = currentGraphOption();
    graphSwitcherLabelEl.textContent = activeOption.label;
    graphSwitcherOptionEls.forEach((optionEl) => {
      const isActive = optionEl.dataset.graphKey === currentGraphKey;
      optionEl.classList.toggle("active", isActive);
      optionEl.setAttribute("aria-current", isActive ? "page" : "false");
    });
    graphSwitcherButtonEl.addEventListener("click", (event) => {
      event.stopPropagation();
      setGraphSwitcherOpen(graphSwitcherMenuEl.classList.contains("hidden"));
    });
    graphSwitcherOptionEls.forEach((optionEl) => {
      optionEl.addEventListener("click", () => {
        const selected = GRAPH_OPTIONS.find((option) => option.key === optionEl.dataset.graphKey);
        if (!selected) return;
        setGraphSwitcherOpen(false);
        window.location.assign(selected.path);
      });
    });
    loadGeneratedGraphOptions().catch((error) => {
      console.warn("Generated graph list failed to load", error);
    });
    document.addEventListener("pointerdown", (event) => {
      if (!graphSwitcherEl.contains(event.target)) {
        setGraphSwitcherOpen(false);
      }
    }, true);
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        setGraphSwitcherOpen(false);
      }
    });
  }

  async function loadGeneratedGraphOptions() {
    if (!graphSwitcherMenuEl) return;
    const response = await fetch(builderApiUrl("/api/generated-graphs"));
    if (!response.ok) return;
    const data = await response.json();
    const graphs = Array.isArray(data.graphs) ? data.graphs : [];
    generatedGraphs = graphs;
    graphSwitcherMenuEl.querySelectorAll(".graph-switcher-option.generated").forEach((element) => element.remove());
    graphs.forEach((graph) => {
      const path = String(graph.path || "");
      const title = String(graph.title || graph.id || "").trim();
      if (!path || !title) return;
      const button = document.createElement("button");
      button.className = "graph-switcher-option generated";
      button.type = "button";
      button.role = "menuitem";
      button.textContent = title;
      button.addEventListener("click", () => {
        setGraphSwitcherOpen(false);
        window.location.assign(path);
      });
      graphSwitcherMenuEl.appendChild(button);
    });
    renderGeneratedGraphManager(graphs);
    updateBuilderVersionInput();
    return graphs;
  }

  function renderGeneratedGraphManager(graphs) {
    if (!builderGraphListEl) return;
    if (!graphs.length) {
      builderGraphListEl.textContent = "No saved generated graphs yet.";
      return;
    }
    builderGraphListEl.innerHTML = graphs.map((graph) => {
      const versions = Array.isArray(graph.versions) ? graph.versions : [];
      const versionButtons = versions.map((version) => `
        <button class="toolbar-btn" type="button" data-graph-action="open-version" data-graph-id="${escapeHtml(graph.id)}" data-version="${escapeHtml(version.version)}">${escapeHtml(version.version)}</button>
        <button class="toolbar-btn" type="button" data-graph-action="activate-version" data-graph-id="${escapeHtml(graph.id)}" data-version="${escapeHtml(version.version)}">Make active</button>
        <button class="toolbar-btn danger" type="button" data-graph-action="delete-version" data-graph-id="${escapeHtml(graph.id)}" data-version="${escapeHtml(version.version)}">Delete ${escapeHtml(version.version)}</button>
      `).join("");
      return `
        <div class="builder-graph-item">
          <div class="builder-graph-item-title">
            <strong>${escapeHtml(graph.title || graph.id)}</strong>
            <span>Active ${escapeHtml(graph.active_version || "n/a")}</span>
          </div>
          <div class="builder-graph-actions">
            <button class="toolbar-btn" type="button" data-graph-action="open-active" data-graph-id="${escapeHtml(graph.id)}">Open active</button>
            <button class="toolbar-btn danger" type="button" data-graph-action="delete-graph" data-graph-id="${escapeHtml(graph.id)}">Delete graph</button>
            ${versionButtons}
          </div>
        </div>
      `;
    }).join("");
  }

  async function handleGeneratedGraphAction(button) {
    const graphId = String(button.dataset.graphId || "");
    const version = String(button.dataset.version || "");
    const action = String(button.dataset.graphAction || "");
    if (!graphId) return;
    if (action === "open-active") {
      window.location.assign(`/generated-graphs/${encodeURIComponent(graphId)}/`);
      return;
    }
    if (action === "open-version" && version) {
      window.location.assign(`/generated-graphs/${encodeURIComponent(graphId)}/versions/${encodeURIComponent(version)}/`);
      return;
    }
    if (action === "activate-version" && version) {
      await postBuilderJson(`/api/generated-graphs/${encodeURIComponent(graphId)}/active`, { version });
      setBuilderStatus(`${graphId} ${version} is now active.`);
      await loadGeneratedGraphOptions();
      return;
    }
    if (action === "delete-version" && version) {
      if (!window.confirm(`Delete ${graphId} ${version}?`)) return;
      await deleteBuilderJson(`/api/generated-graphs/${encodeURIComponent(graphId)}/versions/${encodeURIComponent(version)}`);
      setBuilderStatus(`${graphId} ${version} deleted.`);
      await loadGeneratedGraphOptions();
      return;
    }
    if (action === "delete-graph") {
      if (!window.confirm(`Delete all versions of ${graphId}?`)) return;
      await deleteBuilderJson(`/api/generated-graphs/${encodeURIComponent(graphId)}`);
      setBuilderStatus(`${graphId} deleted.`);
      await loadGeneratedGraphOptions();
    }
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function summarizeLabelList(values, maxItems = 3) {
    const labels = [...new Set((Array.isArray(values) ? values : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
      .map((value) => value.toLowerCase()))]
      .map((lowered) => (Array.isArray(values) ? values : []).find((value) => String(value || "").trim().toLowerCase() === lowered))
      .filter(Boolean);
    if (!labels.length) return "";
    if (labels.length === 1) return labels[0];
    if (labels.length === 2) return `${labels[0]} and ${labels[1]}`;
    const visible = labels.slice(0, maxItems);
    if (labels.length > maxItems) return `${visible.slice(0, -1).join(", ")}, ${visible[visible.length - 1]}, and ${labels.length - maxItems} others`;
    return `${visible.slice(0, -1).join(", ")}, and ${visible[visible.length - 1]}`;
  }

  function isComparableNode(node) {
    return !!node && node.kind !== "seed";
  }

  function setSingleFocus(nodeId = "") {
    viewerState.focusedNodeIds = nodeId ? new Set([nodeId]) : new Set();
  }

  function currentFocusedNode() {
    const focusId = [...viewerState.focusedNodeIds][0] || "";
    return nodeById.get(focusId) || null;
  }

  function clearExtraRoots() {
    viewerState.extraRootIds = [];
  }

  function renderExtraTreeSummary() {
    const extraNodes = viewerState.extraRootIds.map((id) => nodeById.get(id)).filter(isComparableNode);
    compareSummaryEl.classList.toggle("hidden", !extraNodes.length);
    compareClearButton.disabled = !extraNodes.length;
    if (!extraNodes.length) {
      compareSummaryLabelEl.textContent = "";
      return;
    }
    compareSummaryLabelEl.textContent = `Added trees: ${extraNodes.map((node) => node.label || node.id).join(", ")}`;
  }

  function sanitizeSelectionState() {
    setSingleFocus(
      [...viewerState.focusedNodeIds].find((id) => isComparableNode(nodeById.get(id))) || "",
    );
    viewerState.extraRootIds = viewerState.extraRootIds.filter((id, index, ids) => (
      ids.indexOf(id) === index && isComparableNode(nodeById.get(id))
    ));
    viewerState.expandedLowConfidenceNodeIds = new Set(
      [...viewerState.expandedLowConfidenceNodeIds].filter((id) => isLowConfidenceDocumentNode(lowConfidenceNodeLookup(id))),
    );
  }

  function addExtraRoot(nodeId) {
    const node = nodeById.get(nodeId);
    if (!isComparableNode(node)) return false;
    if (viewerState.extraRootIds.includes(nodeId)) return false;
    if (viewerState.focusedNodeIds.has(nodeId) && !viewerState.searchQuery) return false;
    viewerState.extraRootIds = [...viewerState.extraRootIds, nodeId];
    return true;
  }

  function removeExtraRoot(nodeId) {
    viewerState.extraRootIds = viewerState.extraRootIds.filter((id) => id !== nodeId);
  }

  function isLowConfidenceNode(node) {
    return !!node?.is_low_confidence;
  }

  function isLowConfidenceDocumentNode(node) {
    return !!node
      && isLowConfidenceNode(node)
      && node.kind === "organisation"
      && String(node.registry_type || "").toLowerCase() === "other"
      && !!node.low_confidence_expandable;
  }

  function isIdentityNode(node) {
    return !!node && (node.kind === "seed_alias" || node.lane === 1);
  }

  function isPersonAnchorNode(node) {
    return !!node && (node.kind === "person" || isIdentityNode(node) || node.kind === "seed");
  }

  function normalizeNodeKind(node) {
    if (!node) return "";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "charity") return "charity";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "company") return "company";
    return node.kind;
  }

  function nodeTypeLabel(node) {
    const kind = normalizeNodeKind(node);
    if (kind === "seed_alias") return "seed";
    if (kind === "organisation") return "organisation";
    return kind || "node";
  }

  function nodeTypeKey(node) {
    if (node.kind === "seed" || node.lane === 1) return "identity";
    if (node.kind === "address") return "address";
    if (node.kind === "person") return "person";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "charity") return "charity";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "company") return "company";
    return "organisation";
  }

  function canvasSearchCandidates(query) {
    const trimmed = String(query || "").trim().toLowerCase();
    if (!trimmed) return [];
    return allNodes
      .filter((node) => (
        isComparableNode(node)
        && !viewerState.extraRootIds.includes(node.id)
        && nodeMatchesQuery(node, trimmed)
      ))
      .sort((left, right) => {
        const leftStarts = String(left.label || "").toLowerCase().startsWith(trimmed) ? 1 : 0;
        const rightStarts = String(right.label || "").toLowerCase().startsWith(trimmed) ? 1 : 0;
        if (leftStarts !== rightStarts) return rightStarts - leftStarts;
        const scoreDiff = nodeRankScore(right) - nodeRankScore(left);
        if (scoreDiff !== 0) return scoreDiff;
        return String(left.label || "").localeCompare(String(right.label || ""));
      })
      .slice(0, 8);
  }

  function positionCanvasSearchPopover(clientX, clientY) {
    const width = Math.min(360, Math.max(280, window.innerWidth - 24));
    const left = Math.max(12, Math.min(clientX, window.innerWidth - width - 12));
    const top = Math.max(72, Math.min(clientY, window.innerHeight - 340));
    canvasSearchPopoverEl.style.left = `${left}px`;
    canvasSearchPopoverEl.style.top = `${top}px`;
  }

  function renderCanvasSearchResults() {
    const results = canvasSearchCandidates(canvasSearchInput.value);
    canvasSearchResultsEl.innerHTML = results.length
      ? results.map((node) => `
        <button type="button" class="canvas-search-result" data-node-id="${escapeHtml(node.id)}">
          <strong>${escapeHtml(node.label || node.id)}</strong>
          <span>${escapeHtml(nodeTypeLabel(node))}</span>
        </button>
      `).join("")
      : '<div class="canvas-search-empty">Type a name, address, or alias to add another tree.</div>';
  }

  function hideCanvasSearchPopover() {
    canvasSearchPopoverEl.classList.add("hidden");
    canvasSearchInput.value = "";
    canvasSearchResultsEl.innerHTML = "";
  }

  function showCanvasSearchPopover(clientX, clientY) {
    canvasSearchAnchor = { x: clientX, y: clientY };
    canvasSearchTitleEl.textContent = "Add tree";
    canvasSearchPopoverEl.classList.remove("hidden");
    positionCanvasSearchPopover(clientX, clientY);
    canvasSearchInput.value = "";
    renderCanvasSearchResults();
    setTimeout(() => canvasSearchInput.focus(), 0);
  }

  function addTreeFromCanvasSearch(nodeId) {
    hideCanvasSearchPopover();
    const hasBaseTree = !!viewerState.searchQuery
      || viewerState.focusedNodeIds.size > 0
      || viewerState.showIndirectOnly
      || viewerState.showSanctionedOnly
      || viewerState.showNegativeNewsOnly;
    if (!hasBaseTree && !viewerState.extraRootIds.length) {
      setSingleFocus(nodeId);
      applyViewerState();
      return;
    }
    if (addExtraRoot(nodeId)) {
      applyViewerState();
    }
  }

  function isFilterableType(typeKey) {
    return ["identity", "company", "charity", "address", "person"].includes(typeKey);
  }

  function nodeMatchesQuery(node, query) {
    if (!query) return false;
    const q = query.toLowerCase();
    if (String(node.label || "").toLowerCase().includes(q)) return true;
    return (Array.isArray(node.aliases) ? node.aliases : []).some((alias) => String(alias || "").toLowerCase().includes(q));
  }

  function isCompactLowConfidenceEdge(edge) {
    if (!edge?.is_low_confidence) return false;
    const sourceMainNode = baseNodeById.get(edge.source) || null;
    const targetMainNode = baseNodeById.get(edge.target) || null;
    if (!!sourceMainNode === !!targetMainNode) return false;
    const mainNode = sourceMainNode || targetMainNode;
    const overlayNode = lowConfidenceNodeById.get(sourceMainNode ? edge.target : edge.source) || null;
    if (!mainNode || !overlayNode) return false;
    return (mainNode.kind === "person" || mainNode.kind === "organisation" || mainNode.kind === "seed" || mainNode.kind === "seed_alias" || mainNode.lane === 1)
      && overlayNode.kind === "organisation";
  }

  function isMainGraphNodeId(nodeId) {
    return baseNodeById.has(nodeId);
  }

  function lowConfidenceNodeLookup(nodeId) {
    return baseNodeById.get(nodeId) || lowConfidenceNodeById.get(nodeId) || nodeById.get(nodeId) || null;
  }

  function collectExpandedLowConfidenceCluster(rootNodeId) {
    const visibleNodeIds = new Set([rootNodeId]);
    const visibleEdgeIds = new Set();
    const rootNode = lowConfidenceNodeById.get(rootNodeId) || nodeById.get(rootNodeId) || null;
    if (!isLowConfidenceDocumentNode(rootNode)) return { nodeIds: visibleNodeIds, edgeIds: visibleEdgeIds };

    const connectedPersonIds = new Set();
    (lowConfidenceEdgesByNodeId.get(rootNodeId) || []).forEach((edge) => {
      const otherId = edge.source === rootNodeId ? edge.target : edge.source;
      const otherNode = lowConfidenceNodeLookup(otherId);
      if (!otherNode || (!isPersonAnchorNode(otherNode) && otherNode.kind !== "organisation")) return;
      visibleNodeIds.add(otherId);
      visibleEdgeIds.add(edge.id);
      if (isPersonAnchorNode(otherNode)) connectedPersonIds.add(otherId);
    });

    connectedPersonIds.forEach((personId) => {
      (lowConfidenceEdgesByNodeId.get(personId) || []).forEach((edge) => {
        const otherId = edge.source === personId ? edge.target : edge.source;
        const otherNode = lowConfidenceNodeLookup(otherId);
        if (!otherNode || otherId === rootNodeId) return;
        if (otherNode.kind !== "organisation") return;
        visibleNodeIds.add(personId);
        visibleNodeIds.add(otherId);
        visibleEdgeIds.add(edge.id);
      });
    });

    return { nodeIds: visibleNodeIds, edgeIds: visibleEdgeIds };
  }

  function setLowConfidenceNodeExpanded(nodeId, expanded) {
    const nextIds = new Set(viewerState.expandedLowConfidenceNodeIds);
    if (expanded) nextIds.add(nodeId);
    else nextIds.delete(nodeId);
    viewerState.expandedLowConfidenceNodeIds = nextIds;
  }

  function expandLowConfidenceSearchContext(seedIds, visibleIds, options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    if (!includeLowConfidence) return;
    const queue = [];
    const visited = new Set();
    seedIds.forEach((id) => {
      queue.push({ id, depth: 0 });
      visited.add(id);
      (edgesByNodeId.get(id) || []).forEach((edge) => {
        if (edge.kind !== "alias") return;
        const otherId = edge.source === id ? edge.target : edge.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind !== "seed" || visited.has(otherId)) return;
        visibleIds.add(otherId);
        queue.push({ id: otherId, depth: 0 });
        visited.add(otherId);
      });
    });
    while (queue.length) {
      const current = queue.shift();
      const currentNode = nodeById.get(current.id);
      if (!currentNode) continue;
      (edgesByNodeId.get(current.id) || []).forEach((edge) => {
        if (!edge.is_low_confidence) return;
        const otherId = edge.source === current.id ? edge.target : edge.source;
        if (visited.has(otherId)) return;
        const otherNode = nodeById.get(otherId);
        if (!otherNode) return;
        if (current.depth === 0) {
          visibleIds.add(otherId);
          visited.add(otherId);
          if (isLowConfidenceDocumentNode(otherNode)) queue.push({ id: otherId, depth: 1 });
          return;
        }
        if (current.depth === 1 && otherNode.kind === "organisation" && isMainGraphNodeId(otherId)) {
          visibleIds.add(otherId);
          visited.add(otherId);
        }
      });
    }
  }

  function walkUpstreamFromNode(nodeId, visibleIds, visited) {
    if (visited.has(nodeId)) return;
    visited.add(nodeId);
    const node = nodeById.get(nodeId);
    if (!node || node.kind === "seed") return;
    visibleIds.add(nodeId);
    const nodeLane = node.lane ?? 0;
    (edgesByNodeId.get(nodeId) || []).forEach((edge) => {
      const otherId = edge.source === nodeId ? edge.target : edge.source;
      const otherNode = nodeById.get(otherId);
      if (!otherNode || otherNode.kind === "seed") return;
      const otherLane = otherNode.lane ?? 0;
      if (otherLane < nodeLane) walkUpstreamFromNode(otherId, visibleIds, visited);
    });
  }

  function expandOpenLetterUpstreamContext(visibleIds, upstreamVisited) {
    [...visibleIds].forEach((nodeId) => {
      const docNode = lowConfidenceNodeById.get(nodeId) || null;
      if (!isLowConfidenceDocumentNode(docNode)) return;
      (lowConfidenceEdgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const connectedNodeId = edge.source === nodeId ? edge.target : edge.source;
        const connectedNode = nodeById.get(connectedNodeId);
        if (!connectedNode || connectedNode.kind !== "organisation") return;
        walkUpstreamFromNode(connectedNodeId, visibleIds, upstreamVisited);
      });
    });
  }

  function nodeColorValue(node) {
    if (node?.sanctioned) return COLORS.red;
    const kind = normalizeNodeKind(node);
    if (kind === "seed_alias") return COLORS.amber;
    if (kind === "charity" || kind === "company" || kind === "organisation") return COLORS.green;
    if (kind === "address") return COLORS.purple;
    return COLORS.blue;
  }

  function nodeRankScore(node) {
    const seedFlag = node.kind === "seed_alias" ? 2.8 : node.kind === "person" ? 1.4 : 0;
    const sanctionedFlag = node.sanctioned ? 3.5 : 0;
    const egyptJudgmentFlag = node.egypt_judgment_hit ? 3.1 : 0;
    const adverseMediaFlag = node.adverse_media_hit ? 2.8 : 0;
    return (Number(node.score || 0) * 4.5)
      + (Number(node.role_count || 0) * 0.8)
      + (Number(node.org_count || 0) * 0.45)
      + seedFlag
      + sanctionedFlag
      + egyptJudgmentFlag
      + adverseMediaFlag;
  }

  function edgeColorValue(edge) {
    if (edge.is_low_confidence) {
      return String(edge.low_confidence_category || "") === "unresolved_org"
        ? COLORS.pink
        : COLORS.amber;
    }
    if (edge.kind === "hidden_connection") return 0x94a3b8;
    if (edge.kind === "alias") return COLORS.amber;
    const roleType = String(edge.role_type || "").toLowerCase();
    if (roleType.includes("trustee")) return COLORS.blue;
    if (roleType.includes("director")) return COLORS.purple;
    if (roleType.includes("secretary")) return 0x0ea5e9;
    if (edge.kind === "address_link") return COLORS.purple;
    if (edge.kind === "org_link") return COLORS.green;
    return 0x2a3040;
  }

  function renderLegend() {
    const items = [
      ["show-identities", "Seed", true],
      ["show-charities", "Charity", true],
      ["show-companies", "Company", true],
      ["show-addresses", "Address", true],
      ["show-people", "Person", true],
      ["show-low-confidence", "Open letters", false],
      ["show-low-confidence-nodes", "Low confidence nodes", false],
    ];
    legendEl.innerHTML = items.map(([id, label, checked]) => `
      <label class="row">
        <span class="legend-key">${escapeHtml(label)}</span>
        <input class="legend-toggle" id="${id}" type="checkbox" ${checked ? "checked" : ""} />
      </label>
    `).join("");
    compactLegendEl.innerHTML = "";

    showIdentitiesInput = document.getElementById("show-identities");
    showCompaniesInput = document.getElementById("show-companies");
    showCharitiesInput = document.getElementById("show-charities");
    showPeopleInput = document.getElementById("show-people");
    showAddressesInput = document.getElementById("show-addresses");
    showLowConfidenceInput = document.getElementById("show-low-confidence");
    showLowConfidenceNodesInput = document.getElementById("show-low-confidence-nodes");
  }

  function rebuildIndexes() {
    nodeById = new Map(allNodes.map((node) => [node.id, node]));
    edgesByNodeId = new Map();
    allEdges.forEach((edge) => {
      if (!edgesByNodeId.has(edge.source)) edgesByNodeId.set(edge.source, []);
      if (!edgesByNodeId.has(edge.target)) edgesByNodeId.set(edge.target, []);
      edgesByNodeId.get(edge.source).push(edge);
      edgesByNodeId.get(edge.target).push(edge);
    });
    directEdgePairs = new Set(
      allEdges
        .filter((edge) => !edge.is_low_confidence && edge.kind !== "hidden_connection")
        .map((edge) => [edge.source, edge.target].sort().join("||")),
    );
    orgLinkIds = new Map();
    allEdges.filter((edge) => edge.kind === "org_link").forEach((edge) => {
      if (!orgLinkIds.has(edge.source)) orgLinkIds.set(edge.source, new Set());
      if (!orgLinkIds.has(edge.target)) orgLinkIds.set(edge.target, new Set());
      orgLinkIds.get(edge.source).add(edge.target);
      orgLinkIds.get(edge.target).add(edge.source);
    });
    orgAddressIds = new Map();
    addressOrgIds = new Map();
    allEdges.filter((edge) => edge.kind === "address_link").forEach((edge) => {
      const sourceNode = nodeById.get(edge.source);
      const targetNode = nodeById.get(edge.target);
      const orgId = sourceNode?.kind === "organisation" ? edge.source : targetNode?.kind === "organisation" ? edge.target : null;
      const addressId = sourceNode?.kind === "address" ? edge.source : targetNode?.kind === "address" ? edge.target : null;
      if (!orgId || !addressId) return;
      if (!orgAddressIds.has(orgId)) orgAddressIds.set(orgId, new Set());
      orgAddressIds.get(orgId).add(addressId);
      if (!addressOrgIds.has(addressId)) addressOrgIds.set(addressId, new Set());
      addressOrgIds.get(addressId).add(orgId);
    });
    indirectIdentityIdsByOrg = new Map();
    allNodes.filter((node) => node.lane === 1).forEach((identity) => {
      const directOrgs = new Set();
      (edgesByNodeId.get(identity.id) || []).forEach((edge) => {
        if (edge.kind !== "role") return;
        const otherId = edge.source === identity.id ? edge.target : edge.source;
        if (nodeById.get(otherId)?.kind === "organisation") directOrgs.add(otherId);
      });
      if (!directOrgs.size) return;
      const reachableOrgs = new Set();
      directOrgs.forEach((orgId) => {
        (orgLinkIds.get(orgId) || new Set()).forEach((id) => reachableOrgs.add(id));
        (orgAddressIds.get(orgId) || new Set()).forEach((addressId) => {
          (addressOrgIds.get(addressId) || new Set()).forEach((id) => reachableOrgs.add(id));
        });
      });
      directOrgs.forEach((id) => reachableOrgs.delete(id));
      reachableOrgs.forEach((orgId) => {
        if (!indirectIdentityIdsByOrg.has(orgId)) indirectIdentityIdsByOrg.set(orgId, new Set());
        indirectIdentityIdsByOrg.get(orgId).add(identity.id);
      });
    });
  }

  function rebuildActiveGraph() {
    const mainNodeIds = new Set(baseNodes.map((node) => node.id));
    allEdges = baseEdges.filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed").map((edge) => ({ ...edge }));
    if ((!viewerState.showLowConfidence || !lowConfidenceLoaded)
      && (!viewerState.showLowConfidenceNodes || !lowConfidenceOrgLoaded)) {
      allNodes = baseNodes.filter((node) => node.kind !== "seed").map((node) => ({ ...node }));
      if (Array.isArray(mergeOverrides?.organisation) && mergeOverrides.organisation.length) {
        const merged = applyMergeOverrides(allNodes, allEdges, { organisation: mergeOverrides.organisation });
        allNodes = merged.nodes;
        allEdges = merged.edges;
      }
      rebuildIndexes();
      return;
    }
    const activeSeedIds = new Set(
      baseNodes
        .filter((node) => node.kind === "seed" && viewerState.searchQuery && nodeMatchesQuery(node, viewerState.searchQuery))
        .map((node) => node.id),
    );
    const activeLowNodeIds = new Set();
    const activeLowEdgeIds = new Set();
    if (viewerState.showLowConfidence && lowConfidenceLoaded) {
      lowConfidenceEdges.forEach((edge) => {
        if (isCompactLowConfidenceEdge(edge)) {
          activeLowEdgeIds.add(edge.id);
          if (!mainNodeIds.has(edge.source)) activeLowNodeIds.add(edge.source);
          if (!mainNodeIds.has(edge.target)) activeLowNodeIds.add(edge.target);
          if (baseNodeById.get(edge.source)?.kind === "seed") activeSeedIds.add(edge.source);
          if (baseNodeById.get(edge.target)?.kind === "seed") activeSeedIds.add(edge.target);
        }
      });
      [...viewerState.expandedLowConfidenceNodeIds].forEach((nodeId) => {
        const cluster = collectExpandedLowConfidenceCluster(nodeId);
        cluster.edgeIds.forEach((edgeId) => activeLowEdgeIds.add(edgeId));
        cluster.nodeIds.forEach((visibleNodeId) => {
          if (!mainNodeIds.has(visibleNodeId)) activeLowNodeIds.add(visibleNodeId);
        });
      });
      activateOpenLetterUpstreamSeeds(activeLowNodeIds, activeSeedIds);
    }

    const activeLowConfidenceOrgNodeIds = new Set();
    const activeLowConfidenceOrgEdgeIds = new Set();
    if (viewerState.showLowConfidenceNodes && lowConfidenceOrgLoaded) {
      lowConfidenceOrgEdges.forEach((edge) => {
        const sourceMainNode = baseNodeById.get(edge.source) || null;
        const targetMainNode = baseNodeById.get(edge.target) || null;
        if (!!sourceMainNode === !!targetMainNode) return;
        activeLowConfidenceOrgEdgeIds.add(edge.id);
        if (!mainNodeIds.has(edge.source)) activeLowConfidenceOrgNodeIds.add(edge.source);
        if (!mainNodeIds.has(edge.target)) activeLowConfidenceOrgNodeIds.add(edge.target);
      });
    }

    allNodes = baseNodes
      .filter((node) => node.kind !== "seed" || activeSeedIds.has(node.id))
      .map((node) => ({ ...node }));
    allNodes.push(...lowConfidenceNodes.filter((node) => activeLowNodeIds.has(node.id)).map((node) => ({ ...node })));
    allNodes.push(...lowConfidenceOrgNodes.filter((node) => activeLowConfidenceOrgNodeIds.has(node.id)).map((node) => ({ ...node })));
    allEdges.push(...lowConfidenceEdges.filter((edge) => activeLowEdgeIds.has(edge.id)).map((edge) => ({ ...edge })));
    allEdges.push(...lowConfidenceOrgEdges.filter((edge) => activeLowConfidenceOrgEdgeIds.has(edge.id)).map((edge) => ({ ...edge })));
    if (Array.isArray(mergeOverrides?.organisation) && mergeOverrides.organisation.length) {
      const merged = applyMergeOverrides(allNodes, allEdges, { organisation: mergeOverrides.organisation });
      allNodes = merged.nodes;
      allEdges = merged.edges;
    }
    rebuildIndexes();
  }

  function rebuildLowConfidenceIndexes() {
    lowConfidenceNodeById = new Map(lowConfidenceNodes.map((node) => [node.id, node]));
    lowConfidenceEdgesByNodeId = new Map();
    lowConfidenceEdges.forEach((edge) => {
      if (!lowConfidenceEdgesByNodeId.has(edge.source)) lowConfidenceEdgesByNodeId.set(edge.source, []);
      if (!lowConfidenceEdgesByNodeId.has(edge.target)) lowConfidenceEdgesByNodeId.set(edge.target, []);
      lowConfidenceEdgesByNodeId.get(edge.source).push(edge);
      lowConfidenceEdgesByNodeId.get(edge.target).push(edge);
    });
  }

  function rebuildLowConfidenceOrgIndexes() {
    lowConfidenceOrgNodeById = new Map(lowConfidenceOrgNodes.map((node) => [node.id, node]));
    lowConfidenceOrgEdgesByNodeId = new Map();
    lowConfidenceOrgEdges.forEach((edge) => {
      if (!lowConfidenceOrgEdgesByNodeId.has(edge.source)) lowConfidenceOrgEdgesByNodeId.set(edge.source, []);
      if (!lowConfidenceOrgEdgesByNodeId.has(edge.target)) lowConfidenceOrgEdgesByNodeId.set(edge.target, []);
      lowConfidenceOrgEdgesByNodeId.get(edge.source).push(edge);
      lowConfidenceOrgEdgesByNodeId.get(edge.target).push(edge);
    });
  }

  function activateSeedRootsForAnchorNode(nodeId, activeSeedIds) {
    const queue = [nodeId];
    const visited = new Set();
    while (queue.length) {
      const currentId = queue.shift();
      if (visited.has(currentId)) continue;
      visited.add(currentId);
      (baseEdgesByNodeId.get(currentId) || []).forEach((edge) => {
        if (edge.kind !== "alias") return;
        const otherId = edge.source === currentId ? edge.target : edge.source;
        if (visited.has(otherId)) return;
        const otherNode = baseNodeById.get(otherId);
        if (!otherNode || !isPersonAnchorNode(otherNode)) return;
        if (otherNode.kind === "seed") activeSeedIds.add(otherId);
        queue.push(otherId);
      });
    }
  }

  function activateOpenLetterUpstreamSeeds(activeLowNodeIds, activeSeedIds) {
    [...activeLowNodeIds].forEach((nodeId) => {
      const docNode = lowConfidenceNodeById.get(nodeId) || null;
      if (!isLowConfidenceDocumentNode(docNode)) return;
      (lowConfidenceEdgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const connectedNodeId = edge.source === nodeId ? edge.target : edge.source;
        const anchorNode = baseNodeById.get(connectedNodeId) || null;
        if (!anchorNode || !isPersonAnchorNode(anchorNode) || anchorNode.kind === "seed") return;
        activateSeedRootsForAnchorNode(connectedNodeId, activeSeedIds);
      });
    });
  }

  function syncHiddenTypeState() {
    viewerState.hiddenTypes = new Set([
      showIdentitiesInput?.checked ? null : "identity",
      showCompaniesInput?.checked ? null : "company",
      showCharitiesInput?.checked ? null : "charity",
      showPeopleInput?.checked ? null : "person",
      showAddressesInput?.checked ? null : "address",
    ].filter(Boolean));
    viewerState.showLowConfidence = !!showLowConfidenceInput?.checked;
    viewerState.showLowConfidenceNodes = !!showLowConfidenceNodesInput?.checked;
    viewerState.showIndirectOnly = !!indirectOnlyInput?.checked;
    viewerState.showSanctionedOnly = !!sanctionedOnlyInput?.checked;
    viewerState.showNegativeNewsOnly = !!negativeNewsOnlyInput?.checked;
  }

  function getMatchedNodeIds(query) {
    if (!query) return new Set();
    return new Set(
      allNodes
        .filter((node) => nodeMatchesQuery(node, query))
        .map((node) => node.id),
    );
  }

  function collectConnectedSubgraph(rootIds) {
    const reachableIds = new Set();
    const distances = new Map();
    const parents = new Map();
    const queue = [];
    rootIds.forEach((rootId) => {
      const rootNode = nodeById.get(rootId);
      if (!rootNode || rootNode.kind === "seed") return;
      reachableIds.add(rootId);
      distances.set(rootId, 0);
      queue.push(rootId);
    });
    while (queue.length) {
      const currentId = queue.shift();
      const currentDistance = distances.get(currentId) ?? 0;
      (edgesByNodeId.get(currentId) || []).forEach((edge) => {
        const nextId = edge.source === currentId ? edge.target : edge.source;
        const nextNode = nodeById.get(nextId);
        if (!nextNode || nextNode.kind === "seed" || distances.has(nextId)) return;
        distances.set(nextId, currentDistance + 1);
        parents.set(nextId, currentId);
        reachableIds.add(nextId);
        queue.push(nextId);
      });
    }
    return { reachableIds, distances, parents };
  }

  function edgePairKey(a, b) {
    return a < b ? `${a}||${b}` : `${b}||${a}`;
  }

  function hiddenConnectionExpansionKey(sourceId, targetId, hiddenNodeIds = []) {
    return `${sourceId}=>${(Array.isArray(hiddenNodeIds) ? hiddenNodeIds : []).join("=>")}=>${targetId}`;
  }

  function expandedHiddenCloneId(expansionKey, nodeId) {
    return `expanded:${expansionKey}:${nodeId}`;
  }

  function isBridgeStartNode(node) {
    return !!node && node.kind === "organisation";
  }

  function isBridgeTargetNode(node) {
    return !!node && node.lane === 1;
  }

  function hiddenNodeTypeLabel(node) {
    if (!node) return "node";
    return nodeTypeKey(node);
  }

  function displayNodeForEdgeId(nodeId, fallbackNode = null) {
    const directNode = fallbackNode || nodeById.get(nodeId) || null;
    const baseId = String(directNode?._expandedIndirectBaseId || "").trim();
    if (baseId) return nodeById.get(baseId) || directNode;
    return directNode;
  }

  function displayNodeLabelForEdgeId(nodeId, fallbackNode = null, fallbackLabel = "Node") {
    const node = displayNodeForEdgeId(nodeId, fallbackNode);
    return String(node?.label || nodeId || fallbackLabel);
  }

  function hiddenConnectionStepLine(edge) {
    if (edge.tooltip) return edge.tooltip;
    const source = displayNodeForEdgeId(edge.source, edge?._sourceNode);
    const target = displayNodeForEdgeId(edge.target, edge?._targetNode);
    return `${source?.label || edge.source} is linked to ${target?.label || edge.target}`;
  }

  function isBridgeTraversableEdge(edge) {
    return !!edge && edge.kind !== "hidden_connection" && !edge.is_low_confidence;
  }

  function hiddenConnectionTooltipLines(sourceId, targetId, hiddenNodeIds, pathEdges = []) {
    const source = nodeById.get(sourceId);
    const target = nodeById.get(targetId);
    const hiddenNodes = hiddenNodeIds.map((id) => nodeById.get(id)).filter(Boolean);
    const viaText = hiddenNodes.length === 1 ? "1 hidden node" : `${hiddenNodes.length} hidden nodes`;
    const lines = [
      `<strong>${escapeHtml(source?.label || sourceId)}</strong> connects to <strong>${escapeHtml(target?.label || targetId)}</strong> through ${viaText}.`,
    ];
    if (hiddenNodes.length) {
      lines.push(`Hidden path: ${hiddenNodes.map((node) => `${escapeHtml(node.label)} <span class="dim">(${hiddenNodeTypeLabel(node)})</span>`).join(' <span class="dim">→</span> ')}`);
    }
    if (pathEdges.length) {
      lines.push("<strong>How the connection works:</strong>");
      pathEdges.forEach((edge) => lines.push(escapeHtml(hiddenConnectionStepLine(edge))));
    }
    return lines;
  }

  function edgeBetweenNodes(leftId, rightId) {
    return (edgesByNodeId.get(leftId) || []).find((edge) => (
      isBridgeTraversableEdge(edge)
      && ((edge.source === leftId && edge.target === rightId) || (edge.source === rightId && edge.target === leftId))
    )) || null;
  }

  function pathEdgesFromHiddenChain(sourceId, targetId, hiddenNodeIds) {
    const nodeIds = [sourceId, ...hiddenNodeIds, targetId];
    const pathEdges = [];
    for (let index = 0; index < nodeIds.length - 1; index += 1) {
      const edge = edgeBetweenNodes(nodeIds[index], nodeIds[index + 1]);
      if (edge) pathEdges.push(edge);
    }
    return pathEdges;
  }

  function setExpandedHiddenConnection(edge) {
    if (!edge || edge.kind !== "hidden_connection") return false;
    const hiddenNodeIds = Array.isArray(edge.hiddenNodeIds) ? edge.hiddenNodeIds.map((id) => String(id)) : [];
    const expansion = {
      source: String(edge.source),
      target: String(edge.target),
      hiddenNodeIds,
    };
    const nextKey = hiddenConnectionExpansionKey(expansion.source, expansion.target, expansion.hiddenNodeIds);
    if (viewerState.expandedHiddenConnections.some((item) => (
      hiddenConnectionExpansionKey(item.source, item.target, item.hiddenNodeIds) === nextKey
    ))) return false;
    viewerState.expandedHiddenConnections = [...viewerState.expandedHiddenConnections, expansion];
    return true;
  }

  function nodeRect(node, x = Number(node?.x || 0), y = Number(node?.y || 0)) {
    const width = Number(node?._pillWidth || pillWidth(node));
    const height = Number(node?._pillHeight || pillHeight(node));
    return {
      left: x - (width / 2),
      right: x + (width / 2),
      top: y - (height / 2),
      bottom: y + (height / 2),
    };
  }

  function rectsOverlap(left, right, margin = 12) {
    return !(
      (left.right + margin) < right.left
      || (right.right + margin) < left.left
      || (left.bottom + margin) < right.top
      || (right.bottom + margin) < left.top
    );
  }

  function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
  }

  function expandedHiddenLaneFor(node) {
    if (!node) return 2;
    const declared = Number(node.lane || 0);
    if (declared >= 1 && declared <= 4) return declared;
    if (node.kind === "address") return 3;
    if (node.kind === "person") return 4;
    if (node.kind === "seed" || node.kind === "seed_alias") return 1;
    return 2;
  }

  function computeExpandedHiddenLaneCenters(sceneNodes) {
    const buckets = new Map();
    sceneNodes.forEach((node) => {
      const lane = expandedHiddenLaneFor(node);
      if (!buckets.has(lane)) buckets.set(lane, []);
      buckets.get(lane).push(Number(node.y || 0));
    });
    const centers = new Map();
    buckets.forEach((ys, lane) => {
      if (!ys.length) return;
      const min = Math.min(...ys);
      const max = Math.max(...ys);
      centers.set(lane, (min + max) / 2);
    });
    return centers;
  }

  function placeExpandedHiddenNodesAlongConnection(hiddenNodes, sourceNode, targetNode, existingNodes, _bounds, options = {}) {
    if (!hiddenNodes.length) return;
    const expansionIndex = Number(options.expansionIndex || 0);
    const laneCenters = options.laneCenters || computeExpandedHiddenLaneCenters(existingNodes);
    let leftmostX = Number.POSITIVE_INFINITY;
    existingNodes.forEach((node) => {
      const halfWidth = Number(node._pillWidth || pillWidth(node)) / 2;
      const left = Number(node.x || 0) - halfWidth;
      if (left < leftmostX) leftmostX = left;
    });
    if (!Number.isFinite(leftmostX)) leftmostX = 0;
    const fallbackY = (Number(sourceNode?.y || 0) + Number(targetNode?.y || 0)) / 2;
    const widths = hiddenNodes.map((node) => pillWidth(node));
    const maxWidth = Math.max(...widths);
    const sideMargin = 80;
    const columnSpacing = 110;
    const columnX = leftmostX - sideMargin - (maxWidth / 2) - (expansionIndex * columnSpacing);
    const byLane = new Map();
    hiddenNodes.slice().reverse().forEach((node) => {
      node._pillWidth = pillWidth(node);
      node._pillHeight = pillHeight(node);
      node._focused = false;
      node._searchHit = false;
      const lane = expandedHiddenLaneFor(node);
      if (!byLane.has(lane)) byLane.set(lane, []);
      byLane.get(lane).push(node);
    });
    const rowGap = 22;
    byLane.forEach((nodes, lane) => {
      const baseY = laneCenters.has(lane) ? laneCenters.get(lane) : fallbackY;
      const totalHeight = nodes.reduce((sum, node) => sum + node._pillHeight, 0)
        + (rowGap * Math.max(0, nodes.length - 1));
      let cursor = baseY - (totalHeight / 2);
      nodes.forEach((node) => {
        node.x = columnX;
        node.y = cursor + (node._pillHeight / 2);
        cursor += node._pillHeight + rowGap;
        existingNodes.push(node);
      });
    });
  }

  function applyExpandedHiddenConnectionsToScene(scene, bounds) {
    if (!viewerState.expandedHiddenConnections.length) return scene;
    const sceneNodes = scene.nodes.slice();
    const sceneEdges = scene.edges.slice();
    const nodeLookup = new Map(sceneNodes.map((node) => [String(node.id), node]));
    const baseVisibleNodeIds = new Set(scene.nodes.map((node) => String(node.id)));
    const edgeKeysToHide = new Set();
    const sceneLaneCenters = computeExpandedHiddenLaneCenters(scene.nodes);
    viewerState.expandedHiddenConnections.forEach((expansion, expansionIndex) => {
      const sourceNode = nodeLookup.get(String(expansion.source));
      const targetNode = nodeLookup.get(String(expansion.target));
      if (!sourceNode || !targetNode) return;
      const hiddenIds = Array.isArray(expansion.hiddenNodeIds) ? expansion.hiddenNodeIds : [];
      const expansionKey = hiddenConnectionExpansionKey(expansion.source, expansion.target, hiddenIds);
      const pathEdges = pathEdgesFromHiddenChain(expansion.source, expansion.target, hiddenIds);
      const steps = [sourceNode, ...hiddenIds.map((id) => nodeById.get(id)).filter(Boolean), targetNode];
      if (steps.length < 2 || !pathEdges.length) return;
      edgeKeysToHide.add(expansionKey);
      const insertedHiddenNodes = [];
      const expandedNodeIds = new Map([
        [String(expansion.source), String(expansion.source)],
        [String(expansion.target), String(expansion.target)],
      ]);
      hiddenIds.forEach((hiddenId) => {
        const key = String(hiddenId);
        if (baseVisibleNodeIds.has(key)) {
          expandedNodeIds.set(key, key);
          return;
        }
        const hiddenNode = nodeById.get(key);
        if (!hiddenNode) return;
        const cloneId = expandedHiddenCloneId(expansionKey, key);
        const clone = { ...hiddenNode, id: cloneId, _expandedIndirectBaseId: key };
        clone._expandedIndirect = true;
        insertedHiddenNodes.push(clone);
        nodeLookup.set(cloneId, clone);
        expandedNodeIds.set(key, cloneId);
      });
      placeExpandedHiddenNodesAlongConnection(
        insertedHiddenNodes,
        sourceNode,
        targetNode,
        sceneNodes,
        bounds,
        {
          expansionIndex,
          laneCenters: sceneLaneCenters,
        },
      );
      sceneNodes.push(...insertedHiddenNodes.filter((node) => !sceneNodes.includes(node)));
      pathEdges.forEach((pathEdge) => {
        const remappedSource = expandedNodeIds.get(String(pathEdge.source)) || String(pathEdge.source);
        const remappedTarget = expandedNodeIds.get(String(pathEdge.target)) || String(pathEdge.target);
        const pathKey = `${pathEdge.kind}:${remappedSource}:${remappedTarget}:${String(pathEdge.tooltip || "")}:${String(pathEdge.role_type || "")}`;
        if (sceneEdges.some((edge) => (
          `${edge.kind}:${edge.source}:${edge.target}:${String(edge.tooltip || "")}:${String(edge.role_type || "")}` === pathKey
        ))) return;
        sceneEdges.push({
          ...pathEdge,
          source: remappedSource,
          target: remappedTarget,
          _expandedIndirect: true,
        });
      });
    });
    const expandedScene = {
      nodes: sceneNodes,
      edges: sceneEdges.filter((edge) => {
        if (edge.kind !== "hidden_connection") return true;
        const hiddenIds = Array.isArray(edge.hiddenNodeIds) ? edge.hiddenNodeIds.map((id) => String(id)) : [];
        return !edgeKeysToHide.has(hiddenConnectionExpansionKey(edge.source, edge.target, hiddenIds));
      }),
      rootIds: scene.rootIds,
    };
    ensureSceneMetadata(expandedScene.nodes, expandedScene.edges);
    return expandedScene;
  }

  function findBridgeConnections(startId) {
    const startNode = nodeById.get(startId);
    if (!isBridgeStartNode(startNode)) return [];
    const connections = new Map();
    const hiddenQueue = [];
    const visited = new Set([startId]);
    (edgesByNodeId.get(startId) || []).forEach((edge) => {
      if (!isBridgeTraversableEdge(edge)) return;
      const nextId = edge.source === startId ? edge.target : edge.source;
      if (visited.has(nextId)) return;
      visited.add(nextId);
      const nextNode = nodeById.get(nextId);
      if (nextNode && isBridgeTargetNode(nextNode)) {
        if (!directEdgePairs.has(edgePairKey(startId, nextId))) {
          connections.set(nextId, {
            source: startId,
            target: nextId,
            kind: "hidden_connection",
            hops: 1,
            hiddenNodeIds: [nextId],
            pathEdges: [edge],
            tooltip_lines: hiddenConnectionTooltipLines(startId, nextId, [nextId], [edge]),
          });
        }
        return;
      }
      if (!isBridgeStartNode(nextNode)) return;
      hiddenQueue.push({ id: nextId, hops: 1, hiddenNodeIds: [nextId], pathEdges: [edge] });
    });
    while (hiddenQueue.length) {
      const current = hiddenQueue.shift();
      (edgesByNodeId.get(current.id) || []).forEach((edge) => {
        if (!isBridgeTraversableEdge(edge)) return;
        const nextId = edge.source === current.id ? edge.target : edge.source;
        if (visited.has(nextId)) return;
        visited.add(nextId);
        const nextNode = nodeById.get(nextId);
        if (nextNode && isBridgeTargetNode(nextNode)) {
          const existing = connections.get(nextId);
          if ((!existing || current.hops + 1 < existing.hops) && !directEdgePairs.has(edgePairKey(startId, nextId))) {
            connections.set(nextId, {
              source: startId,
              target: nextId,
              kind: "hidden_connection",
              hops: current.hops + 1,
              hiddenNodeIds: [...current.hiddenNodeIds, nextId],
              pathEdges: [...current.pathEdges, edge],
              tooltip_lines: hiddenConnectionTooltipLines(startId, nextId, current.hiddenNodeIds, [...current.pathEdges, edge]),
            });
          }
          return;
        }
        if (!isBridgeStartNode(nextNode)) return;
        hiddenQueue.push({ id: nextId, hops: current.hops + 1, hiddenNodeIds: [...current.hiddenNodeIds, nextId], pathEdges: [...current.pathEdges, edge] });
      });
    }
    return [...connections.values()];
  }

  function deriveVisibleBridgeEdges(visibleIds) {
    const hiddenConnections = new Map();
    [...visibleIds].forEach((startId) => {
      if (!isBridgeStartNode(nodeById.get(startId))) return;
      findBridgeConnections(startId).forEach((connection) => {
        if (!visibleIds.has(connection.target)) return;
        const pairKey = edgePairKey(connection.source, connection.target);
        const existing = hiddenConnections.get(pairKey);
        if (!existing || connection.hops < existing.hops) hiddenConnections.set(pairKey, connection);
      });
    });
    return [...hiddenConnections.values()];
  }

  function applyTypeFilters(visibleIds, rootIds, options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    if (!visibleIds.size) return new Set();
    const filteredIds = new Set(
      [...visibleIds].filter((id) => {
        const node = nodeById.get(id);
        if (!node) return false;
        if (node.kind === "seed" && !includeLowConfidence && !rootIds.has(id)) return false;
        if (node.is_low_confidence && !includeLowConfidence) return false;
        if (node.is_low_confidence) return true;
        const typeKey = nodeTypeKey(node);
        if (!isFilterableType(typeKey)) return true;
        return !viewerState.hiddenTypes.has(typeKey);
      }),
    );
    [...filteredIds].forEach((id) => {
      const node = nodeById.get(id);
      if (!node || node.kind !== "seed") return;
      const linkedIdentityVisible = (edgesByNodeId.get(id) || []).some((edge) => {
        if (edge.kind !== "alias") return false;
        const otherId = edge.source === id ? edge.target : edge.source;
        return filteredIds.has(otherId) && isIdentityNode(nodeById.get(otherId));
      });
      if (linkedIdentityVisible) filteredIds.delete(id);
    });
    if (!rootIds.size || viewerState.showIndirectOnly) return filteredIds;
    let changed = true;
    while (changed) {
      changed = false;
      const degree = new Map();
      filteredIds.forEach((id) => degree.set(id, 0));
      allEdges.forEach((edge) => {
        if (!includeLowConfidence && edge.is_low_confidence) return;
        if (!filteredIds.has(edge.source) || !filteredIds.has(edge.target)) return;
        degree.set(edge.source, (degree.get(edge.source) || 0) + 1);
        degree.set(edge.target, (degree.get(edge.target) || 0) + 1);
      });
      [...filteredIds].forEach((id) => {
        if (rootIds.has(id)) return;
        if (options.keepDisconnectedIdentities && nodeById.get(id)?.lane === 1) return;
        if ((degree.get(id) || 0) > 0) return;
        filteredIds.delete(id);
        changed = true;
      });
    }
    return filteredIds;
  }

  function expandRelatedAddresses(visibleIds) {
    if (viewerState.hiddenTypes.has("address")) return new Set(visibleIds);
    const expandedIds = new Set(visibleIds);
    [...visibleIds].forEach((id) => {
      const node = nodeById.get(id);
      if (!node || node.kind !== "organisation") return;
      (orgAddressIds.get(id) || new Set()).forEach((addressId) => expandedIds.add(addressId));
    });
    return expandedIds;
  }

  function buildSearchProjection(matchedIds, options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    const visibleIds = new Set();
    matchedIds.forEach((id) => visibleIds.add(id));
    function walkLane(nodeId, visited, directionFn) {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);
      const node = nodeById.get(nodeId);
      if (!node || node.kind === "seed") return;
      visibleIds.add(nodeId);
      const nodeLane = node.lane ?? 0;
      (edgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const otherId = edge.source === nodeId ? edge.target : edge.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind === "seed") return;
        const otherLane = otherNode.lane ?? 0;
        if (directionFn(otherLane, nodeLane)) walkLane(otherId, visited, directionFn);
      });
    }
    const peopleOnlySearch = matchedIds.size > 0 && [...matchedIds].every((id) => nodeById.get(id)?.lane === 4);
    const upstreamVisited = new Set();
    const focusOrgIds = new Set();
    if (peopleOnlySearch) {
      matchedIds.forEach((id) => {
        (edgesByNodeId.get(id) || []).forEach((edge) => {
          const otherId = edge.source === id ? edge.target : edge.source;
          const otherNode = nodeById.get(otherId);
          if (!otherNode || otherNode.kind !== "organisation") return;
          focusOrgIds.add(otherId);
          visibleIds.add(otherId);
          (edgesByNodeId.get(otherId) || []).forEach((orgEdge) => {
            if (orgEdge.kind !== "role") return;
            const nextId = orgEdge.source === otherId ? orgEdge.target : orgEdge.source;
            if (nodeById.get(nextId)?.lane === 1) visibleIds.add(nextId);
          });
        });
      });
    } else {
      matchedIds.forEach((id) => walkLane(id, upstreamVisited, (other, self) => other < self));
    }
    const bridgeStartIds = peopleOnlySearch ? [...focusOrgIds] : [...matchedIds];
    bridgeStartIds.forEach((startId) => {
      findBridgeConnections(startId).forEach((connection) => {
        const node = nodeById.get(connection.target);
        if (!node) return;
        if (peopleOnlySearch && node.lane === 4) return;
        visibleIds.add(node.id);
      });
    });
    const downstreamVisited = new Set();
    matchedIds.forEach((id) => walkLane(id, downstreamVisited, (other, self) => other > self));
    expandLowConfidenceSearchContext(matchedIds, visibleIds, { includeLowConfidence });
    expandOpenLetterUpstreamContext(visibleIds, upstreamVisited);
    [...visibleIds]
      .map((id) => nodeById.get(id))
      .filter((node) => node?.kind === "organisation")
      .forEach((node) => {
        if (!peopleOnlySearch) {
          walkLane(node.id, downstreamVisited, (other, self) => other > self);
          return;
        }
        (edgesByNodeId.get(node.id) || []).forEach((edge) => {
          const otherId = edge.source === node.id ? edge.target : edge.source;
          if (nodeById.get(otherId)?.kind === "address") visibleIds.add(otherId);
        });
      });
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), matchedIds, { keepDisconnectedIdentities: true, includeLowConfidence });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
    return {
      projectionType: "search",
      includeLowConfidence,
      seedIds: [...matchedIds],
      rootIds: [...matchedIds],
      visibleIds: filteredVisibleIds,
      edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
    };
  }

  function buildIndirectOrgProjection(options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    const qualifyingOrgIds = new Set();
    indirectIdentityIdsByOrg.forEach((identityIds, orgId) => {
      if (identityIds.size >= 2) qualifyingOrgIds.add(orgId);
    });
    const visibleIds = new Set(qualifyingOrgIds);
    qualifyingOrgIds.forEach((orgId) => {
      (edgesByNodeId.get(orgId) || []).forEach((edge) => {
        if (edge.kind !== "role") return;
        const otherId = edge.source === orgId ? edge.target : edge.source;
        if (nodeById.get(otherId)?.lane === 1) visibleIds.add(otherId);
      });
      (indirectIdentityIdsByOrg.get(orgId) || new Set()).forEach((identityId) => visibleIds.add(identityId));
    });
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), qualifyingOrgIds, { keepDisconnectedIdentities: true, includeLowConfidence });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
    return {
      projectionType: "indirect",
      includeLowConfidence,
      rootIds: [...qualifyingOrgIds],
      visibleIds: filteredVisibleIds,
      edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
    };
  }

  function buildSanctionedProjection(options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    const sanctionedIds = new Set(
      allNodes
        .filter((node) => node.kind !== "seed" && node.sanctioned)
        .map((node) => node.id),
    );
    const visibleIds = new Set(sanctionedIds);
    sanctionedIds.forEach((nodeId) => {
      (edgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const otherId = edge.source === nodeId ? edge.target : edge.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind === "seed") return;
        visibleIds.add(otherId);
      });
    });
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), sanctionedIds, { keepDisconnectedIdentities: true, includeLowConfidence });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
    return {
      projectionType: "sanctioned",
      includeLowConfidence,
      rootIds: [...sanctionedIds],
      visibleIds: filteredVisibleIds,
      edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
    };
  }

  function buildNegativeNewsProjection(options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? (viewerState.showLowConfidence || viewerState.showLowConfidenceNodes);
    const adverseMediaIds = new Set(
      allNodes
        .filter((node) => node.kind !== "seed" && node.adverse_media_hit)
        .map((node) => node.id),
    );
    const visibleIds = new Set(adverseMediaIds);
    adverseMediaIds.forEach((nodeId) => {
      (edgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const otherId = edge.source === nodeId ? edge.target : edge.source;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind === "seed") return;
        visibleIds.add(otherId);
      });
    });
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), adverseMediaIds, { keepDisconnectedIdentities: true, includeLowConfidence });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
    return {
      projectionType: "negative_news",
      includeLowConfidence,
      rootIds: [...adverseMediaIds],
      visibleIds: filteredVisibleIds,
      edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)),
    };
  }

  function buildFocusedProjection(rootIds, options = {}) {
    const includeLowConfidence = options.includeLowConfidence ?? viewerState.showLowConfidence;
    if (!rootIds.size) {
      const visibleIds = applyTypeFilters(
        expandRelatedAddresses(new Set(allNodes.filter((node) => node.kind !== "seed").map((node) => node.id))),
        new Set(),
        { includeLowConfidence },
      );
      expandOpenLetterUpstreamContext(visibleIds, new Set());
      const edgeIds = allEdges.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
      return { projectionType: "focused", includeLowConfidence, rootIds: [], visibleIds, edgeIds };
    }
    const subgraph = collectConnectedSubgraph(rootIds);
    const visibleIds = applyTypeFilters(expandRelatedAddresses(new Set(subgraph.reachableIds)), rootIds, { includeLowConfidence });
    expandOpenLetterUpstreamContext(visibleIds, new Set());
    const edgeIds = allEdges.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target) && (includeLowConfidence || !edge.is_low_confidence));
    return { projectionType: "focused", includeLowConfidence, rootIds: [...rootIds], visibleIds, edgeIds };
  }

  function applyHighlightOnlyFilters(projection, options = {}) {
    const showSanctionedOnly = options.showSanctionedOnly ?? viewerState.showSanctionedOnly;
    const showNegativeNewsOnly = options.showNegativeNewsOnly ?? viewerState.showNegativeNewsOnly;
    const baseProjection = {
      ...projection,
      showSanctionedOnly,
      showNegativeNewsOnly,
    };
    if (!showSanctionedOnly && !showNegativeNewsOnly) return baseProjection;
    const projectionVisibleIds = new Set(baseProjection.visibleIds || []);
    const focusIds = new Set(
      [...projectionVisibleIds].filter((nodeId) => {
        const node = nodeById.get(nodeId);
        if (!node || node.kind === "seed") return false;
        return (showSanctionedOnly && !!node.sanctioned)
          || (showNegativeNewsOnly && !!node.adverse_media_hit);
      }),
    );
    if (!focusIds.size) {
      return {
        ...baseProjection,
        rootIds: [],
        visibleIds: new Set(),
        edgeIds: [],
      };
    }
    const filteredVisibleIds = new Set(focusIds);
    focusIds.forEach((nodeId) => {
      (edgesByNodeId.get(nodeId) || []).forEach((edge) => {
        const otherId = edge.source === nodeId ? edge.target : edge.source;
        if (!projectionVisibleIds.has(otherId)) return;
        const otherNode = nodeById.get(otherId);
        if (!otherNode || otherNode.kind === "seed") return;
        filteredVisibleIds.add(otherId);
      });
    });
    const restrictedVisibleIds = new Set(
      [...expandRelatedAddresses(filteredVisibleIds)].filter((nodeId) => projectionVisibleIds.has(nodeId)),
    );
    const edgeIds = (baseProjection.edgeIds || [])
      .filter((edge) => edge.kind !== "hidden_connection")
      .filter((edge) => restrictedVisibleIds.has(edge.source) && restrictedVisibleIds.has(edge.target));
    return {
      ...baseProjection,
      rootIds: [...focusIds],
      visibleIds: restrictedVisibleIds,
      edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(restrictedVisibleIds)),
    };
  }

  function lowConfidenceOnlyVisibleNodeIdsForProjection(projection) {
    if (!projection?.includeLowConfidence) return new Set();
    let baseline = null;
    if (projection.projectionType === "search") {
      baseline = buildSearchProjection(new Set(projection.seedIds || []), { includeLowConfidence: false });
    } else if (projection.projectionType === "indirect") {
      baseline = buildIndirectOrgProjection({ includeLowConfidence: false });
    } else if (projection.projectionType === "sanctioned") {
      baseline = buildSanctionedProjection({ includeLowConfidence: false });
    } else if (projection.projectionType === "negative_news") {
      baseline = buildNegativeNewsProjection({ includeLowConfidence: false });
    } else {
      baseline = buildFocusedProjection(new Set(projection.rootIds || []), { includeLowConfidence: false });
    }
    baseline = applyHighlightOnlyFilters(baseline, {
      showSanctionedOnly: !!projection.showSanctionedOnly,
      showNegativeNewsOnly: !!projection.showNegativeNewsOnly,
    });
    return new Set(
      [...projection.visibleIds].filter((nodeId) => {
        if (baseline.visibleIds.has(nodeId)) return false;
        const node = nodeById.get(nodeId);
        return !!node && !node.is_low_confidence && (node.kind === "person" || node.kind === "organisation");
      }),
    );
  }

  function projectVisibleGraph() {
    const matchedIds = getMatchedNodeIds(viewerState.searchQuery);
    const rootIds = matchedIds.size ? matchedIds : new Set(viewerState.focusedNodeIds);
    let projection;
    if (matchedIds.size) projection = buildSearchProjection(matchedIds);
    else if (viewerState.showIndirectOnly) projection = buildIndirectOrgProjection();
    else projection = buildFocusedProjection(rootIds);
    return applyHighlightOnlyFilters(projection);
  }

  function textWidth(text, bold = false) {
    measureCtx.font = `${bold ? 700 : 700} ${bold ? 14 : 13}px "Segoe UI", system-ui, sans-serif`;
    return measureCtx.measureText(String(text || "")).width;
  }

  function fontSize(node) {
    return node.kind === "seed_alias" ? 14 : 13;
  }

  function pillHeight(node) {
    return fontSize(node) + 16;
  }

  function badgeWidth(node) {
    const registryType = String(node?.registry_type || "").toLowerCase();
    return node?.kind === "organisation" && (registryType === "company" || registryType === "charity") ? 18 : 0;
  }

  function focusButtonWidth(node) {
    return node.kind === "seed" ? 0 : 26;
  }

  function pillWidth(node) {
    const labelWidth = textWidth(node.label || "", node.kind === "seed_alias");
    return badgeWidth(node) + labelWidth + 28 + focusButtonWidth(node);
  }

  function buildEdgeAdjacency(nodes, edges) {
    const edgeAdjacency = new Map(nodes.map((node) => [node.id, []]));
    edges.forEach((edge) => {
      if (!edgeAdjacency.has(edge.source) || !edgeAdjacency.has(edge.target)) return;
      edgeAdjacency.get(edge.source).push(edge);
      edgeAdjacency.get(edge.target).push(edge);
    });
    return edgeAdjacency;
  }

  function avgNeighborX(node, edgeAdjacency, nodeLookup, fallbackCenter = (container.clientWidth || window.innerWidth) / 2) {
    const xs = [];
    (edgeAdjacency.get(node.id) || []).forEach((edge) => {
      const other = nodeLookup.get(edge.source === node.id ? edge.target : edge.source);
      if (other && other._visible && other.x != null && other.lane !== node.lane) xs.push(other.x);
    });
    if (!xs.length) return fallbackCenter;
    return xs.reduce((sum, value) => sum + value, 0) / xs.length;
  }

  function lowConfidenceLaneAnchorX(node, edgeAdjacency, nodeLookup) {
    if (!node || Number(node.lane || 0) !== 2) return null;
    const xs = [];
    const addAnchor = (anchorNode) => {
      if (anchorNode && anchorNode._visible && anchorNode.x != null) xs.push(anchorNode.x);
    };
    (edgeAdjacency.get(node.id) || []).forEach((edge) => {
      if (!edge?.is_low_confidence) return;
      const other = nodeLookup.get(edge.source === node.id ? edge.target : edge.source);
      if (!other) return;
      if (isPersonAnchorNode(other)) {
        addAnchor(other);
        return;
      }
      if (!isLowConfidenceDocumentNode(other)) return;
      (edgeAdjacency.get(other.id) || []).forEach((docEdge) => {
        if (!docEdge?.is_low_confidence) return;
        const docOther = nodeLookup.get(docEdge.source === other.id ? docEdge.target : docEdge.source);
        if (isPersonAnchorNode(docOther)) addAnchor(docOther);
      });
    });
    if (!xs.length) return null;
    return xs.reduce((sum, value) => sum + value, 0) / xs.length;
  }

  function nodeConnectionOrderScore(node) {
    return (Number(node.degree || 0) * 1000)
      + (Number(node.org_count || 0) * 10)
      + Number(node.role_count || 0);
  }

  function layoutNodesInBounds(nodes, edges, rootIds, bounds) {
    const nodeLookup = new Map(nodes.map((node) => [node.id, node]));
    const edgeAdjacency = buildEdgeAdjacency(nodes, edges);
    const fallbackCenter = bounds.left + ((bounds.right - bounds.left) / 2);
    let curY = bounds.top + 72;
    [1, 2, 3, 4].forEach((lane) => {
      const laneNodes = nodes.filter((node) => Number(node.lane || 0) === lane);
      laneNodes.sort((left, right) => {
        if (lane === 2) {
          const leftAnchor = lowConfidenceLaneAnchorX(left, edgeAdjacency, nodeLookup);
          const rightAnchor = lowConfidenceLaneAnchorX(right, edgeAdjacency, nodeLookup);
          if (leftAnchor != null || rightAnchor != null) {
            if (leftAnchor == null) return 1;
            if (rightAnchor == null) return -1;
            const anchorDiff = leftAnchor - rightAnchor;
            if (anchorDiff !== 0) return anchorDiff;
          }
        }
        const connectionDiff = nodeConnectionOrderScore(right) - nodeConnectionOrderScore(left);
        if (connectionDiff !== 0) return connectionDiff;
        const neighborDiff = avgNeighborX(left, edgeAdjacency, nodeLookup, fallbackCenter) - avgNeighborX(right, edgeAdjacency, nodeLookup, fallbackCenter);
        if (neighborDiff !== 0) return neighborDiff;
        return String(left.label || "").localeCompare(String(right.label || ""));
      });
      const spacing = 16;
      const rowGap = 18;
      const pad = 18;
      const usableMin = bounds.left + pad;
      const usableMax = bounds.right - pad;
      const maxRowW = Math.max(120, usableMax - usableMin);
      const rows = [];
      let currentRow = [];
      let currentWidth = 0;
      laneNodes.forEach((node) => {
        const nodeWidth = pillWidth(node);
        const nextWidth = currentRow.length ? currentWidth + spacing + nodeWidth : nodeWidth;
        if (currentRow.length && nextWidth > maxRowW) {
          rows.push(currentRow);
          currentRow = [node];
          currentWidth = nodeWidth;
        } else {
          currentRow.push(node);
          currentWidth = nextWidth;
        }
      });
      if (currentRow.length) rows.push(currentRow);
      const rowStep = rows.length ? Math.max(...rows.flat().map((node) => pillHeight(node))) + rowGap : 0;
      rows.forEach((row, rowIndex) => {
        const rowW = row.reduce((sum, node) => sum + pillWidth(node), 0) + (spacing * Math.max(0, row.length - 1));
        let cx = usableMin + Math.max(0, (maxRowW - rowW) / 2);
        const rowY = curY + (rowIndex * rowStep);
        row.forEach((node) => {
          const widthForNode = pillWidth(node);
          node.x = cx + (widthForNode / 2);
          node.y = rowY;
          node._pillWidth = widthForNode;
          node._pillHeight = pillHeight(node);
          node._focused = rootIds.has(node.id);
          node._searchHit = viewerState.searchQuery && String(node.label || "").toLowerCase().includes(viewerState.searchQuery.toLowerCase());
          node._rankScore = nodeRankScore(node);
          cx += widthForNode + spacing;
        });
      });
      const laneHeight = rows.length * rowStep;
      curY += Math.max(laneHeight, 30) + 50;
    });
    return curY;
  }

  function layoutVisibleNodes(rootIds) {
    const width = container.clientWidth || window.innerWidth;
    layoutNodesInBounds(visibleNodes, visibleEdges, rootIds, { left: 0, right: width, top: 0 });
  }

  function ensureSceneMetadata(nodes = visibleNodes, edges = visibleEdges) {
    const nodeLookup = new Map(nodes.map((node) => [node.id, node]));
    nodes.forEach((node) => {
      node._colorValue = nodeColorValue(node);
      node._rankScore = nodeRankScore(node);
      node._fontSize = fontSize(node);
    });
    edges.forEach((edge) => {
      edge._sourceNode = nodeLookup.get(edge.source) || nodeById.get(edge.source);
      edge._targetNode = nodeLookup.get(edge.target) || nodeById.get(edge.target);
      edge._colorValue = edgeColorValue(edge);
    });
  }

  function buildSceneForProjection(projection, bounds) {
    const rootIds = new Set(projection.rootIds || []);
    const lowConfidenceOnlyIds = lowConfidenceOnlyVisibleNodeIdsForProjection(projection);
    const sceneNodes = allNodes
      .filter((node) => projection.visibleIds.has(node.id))
      .map((node) => ({ ...node, _visible: true, _lowConfidenceOnlyVisible: lowConfidenceOnlyIds.has(node.id) }));
    const sceneEdges = projection.edgeIds
      .filter((edge) => projection.visibleIds.has(edge.source) && projection.visibleIds.has(edge.target))
      .map((edge) => ({ ...edge }));
    layoutNodesInBounds(sceneNodes, sceneEdges, rootIds, bounds);
    ensureSceneMetadata(sceneNodes, sceneEdges);
    return { nodes: sceneNodes, edges: sceneEdges, rootIds: [...rootIds] };
  }

  function buildCombinedScene(baseProjection) {
    const width = container.clientWidth || window.innerWidth;
    const scenes = [{ projection: baseProjection }];
    viewerState.extraRootIds.forEach((nodeId) => {
      scenes.push({ projection: applyHighlightOnlyFilters(buildSearchProjection(new Set([nodeId]))) });
    });
    if (scenes.length === 1) {
      const fullBounds = { left: 0, right: width, top: 0 };
      const fullScene = buildSceneForProjection(baseProjection, fullBounds);
      return applyExpandedHiddenConnectionsToScene({
        nodes: fullScene.nodes,
        edges: fullScene.edges,
        rootIds: fullScene.rootIds,
      }, fullBounds);
    }
    const columns = scenes.length === 2 ? 2 : Math.min(scenes.length, 3);
    const outerPad = 18;
    const gutter = 18;
    const usableWidth = Math.max(240, width - (outerPad * 2) - (gutter * Math.max(0, columns - 1)));
    const columnWidth = usableWidth / columns;
    let rowTop = 0;
    const combinedNodes = [];
    const combinedEdges = [];
    const combinedRootIds = [];
    for (let start = 0; start < scenes.length; start += columns) {
      const rowScenes = scenes.slice(start, start + columns);
      let rowBottom = rowTop;
      rowScenes.forEach((entry, offset) => {
        const left = outerPad + (offset * (columnWidth + gutter));
        const bounds = { left, right: left + columnWidth, top: rowTop };
        const scene = applyExpandedHiddenConnectionsToScene(
          buildSceneForProjection(entry.projection, bounds),
          bounds,
        );
        combinedNodes.push(...scene.nodes);
        combinedEdges.push(...scene.edges);
        combinedRootIds.push(...scene.rootIds);
        rowBottom = Math.max(
          rowBottom,
          ...scene.nodes.map((node) => Number(node.y || rowTop) + (Number(node._pillHeight || 0) / 2)),
        );
      });
      rowTop = rowBottom + 120;
    }
    return { nodes: combinedNodes, edges: combinedEdges, rootIds: combinedRootIds };
  }

  function showTooltip(event, lines) {
    if (!lines?.length) return;
    tooltip.innerHTML = lines.join("<br>");
    tooltip.style.display = "block";
    positionTooltip(event);
  }

  function positionTooltip(event) {
    if (!event) return;
    const pad = 14;
    const width = tooltip.offsetWidth;
    const height = tooltip.offsetHeight;
    let x = event.clientX + pad;
    let y = event.clientY - 10;
    if (x + width > window.innerWidth - 10) x = event.clientX - width - pad;
    if (y + height > window.innerHeight - 10) y = window.innerHeight - height - 10;
    tooltip.style.left = `${x}px`;
    tooltip.style.top = `${y}px`;
  }

  function hideTooltip() {
    tooltip.style.display = "none";
  }

  function tooltipLinesForNode(node) {
    const egyptJudgmentCount = Number(node?.egypt_judgment_count || 0);
    const egyptJudgmentSummary = egyptJudgmentCount > 0
      ? `Egypt judgments: ${egyptJudgmentCount} match${egyptJudgmentCount === 1 ? "" : "es"}`
      : "";
    if (!node?.is_low_confidence) {
      const lines = Array.isArray(node?.tooltip_lines) ? node.tooltip_lines.slice() : [node?.label || "Node"];
      if (egyptJudgmentSummary) lines.push(egyptJudgmentSummary);
      return lines;
    }
    const linkedEdges = lowConfidenceEdgesByNodeId.get(node?.id) || [];
    let summary = "";
    for (const edge of linkedEdges) {
      const evidenceItems = Array.isArray(edge?.evidence_items) ? edge.evidence_items : [];
      for (const item of evidenceItems) {
        const notes = String(item?.notes || "").trim();
        if (notes) {
          summary = notes;
          break;
        }
      }
      if (!summary) summary = String(edge?.evidence?.notes || "").trim();
      if (summary) break;
    }
    const lines = [`<strong>${escapeHtml(node?.label || "Node")}</strong>`];
    if (summary) lines.push(escapeHtml(summary));
    else if (node?.label) lines.push(escapeHtml(String(node.label)));
    if (egyptJudgmentSummary) lines.push(escapeHtml(egyptJudgmentSummary));
    return lines;
  }

  function tooltipLinesForEdge(edge) {
    if (!edge?.is_low_confidence) {
      const explicitTooltipLines = Array.isArray(edge?.tooltip_lines)
        ? edge.tooltip_lines.filter((value) => String(value || "").trim())
        : [];
      if (explicitTooltipLines.length) return explicitTooltipLines.slice();
      const explicitTooltip = String(edge?.tooltip || "").trim();
      if (explicitTooltip) return [explicitTooltip];
      const displayPersonLabels = Array.isArray(edge?.display_person_labels)
        ? edge.display_person_labels.map((value) => String(value || "").trim()).filter(Boolean)
        : [];
      if (edge?.kind === "role" && displayPersonLabels.length) {
        const sourceNode = displayNodeForEdgeId(edge?.source, edge?._sourceNode) || null;
        const targetNode = displayNodeForEdgeId(edge?.target, edge?._targetNode) || null;
        const sourceKind = String(sourceNode?.kind || "");
        const targetKind = String(targetNode?.kind || "");
        const orgLabel = sourceKind === "organisation"
          ? String(sourceNode?.label || edge?.source || "Organisation")
          : String(targetNode?.label || edge?.target || "Organisation");
        const phrase = String(edge?.phrase || "").trim() || "is linked to";
        const personLabel = displayPersonLabels.length === 1
          ? displayPersonLabels[0]
          : summarizeLabelList(displayPersonLabels);
        return [`${escapeHtml(personLabel)} ${escapeHtml(phrase)} ${escapeHtml(orgLabel)}.`];
      }
      return ["link"];
    }
    const sourceLabel = displayNodeLabelForEdgeId(edge?.source, edge?._sourceNode, "Source");
    const targetLabel = displayNodeLabelForEdgeId(edge?.target, edge?._targetNode, "Target");
    const rawType = String(edge?.role_label || edge?.role_type || "").trim();
    const baseType = rawType.replace(/\s*\([^)]*\)\s*$/, "").toLowerCase();
    const titleMatch = rawType.match(/\(([^)]+)\)\s*$/);
    const title = String(titleMatch?.[1] || "").trim();
    const representedOrganisations = Array.isArray(edge?.represented_organisation_labels) ? edge.represented_organisation_labels : [];
    const representedSigners = Array.isArray(edge?.represented_signer_labels) ? edge.represented_signer_labels : [];
    const subject = title && !sourceLabel.toLowerCase().startsWith(`${title.toLowerCase()} `)
      ? `${title} ${sourceLabel}`
      : sourceLabel;
    if (baseType.includes("signatory") && representedOrganisations.length) {
      return [`${escapeHtml(subject)} signed ${escapeHtml(targetLabel)} representing ${escapeHtml(summarizeLabelList(representedOrganisations))}.`];
    }
    if (edge?.kind === "mapping_document_affiliation" && representedSigners.length) {
      return [`${escapeHtml(summarizeLabelList(representedSigners))} signed ${escapeHtml(sourceLabel)} representing ${escapeHtml(targetLabel)}.`];
    }
    if (baseType.includes("signatory")) return [`${escapeHtml(subject)} is listed as a signatory for ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("affiliate")) return [`${escapeHtml(subject)} is affiliated with ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("partner")) return [`${escapeHtml(subject)} is a partner of ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("sponsor")) return [`${escapeHtml(subject)} sponsors ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("member_of") || baseType.includes("member of")) return [`${escapeHtml(subject)} is a member of ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("hosted_by") || baseType.includes("hosted by")) return [`${escapeHtml(subject)} is hosted by ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("funded_by") || baseType.includes("funded by")) return [`${escapeHtml(subject)} is funded by ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("parent")) return [`${escapeHtml(subject)} is a parent organisation of ${escapeHtml(targetLabel)}.`];
    if (baseType.includes("subsidiary")) return [`${escapeHtml(subject)} is a subsidiary of ${escapeHtml(targetLabel)}.`];
    if (edge?.phrase) return [`${escapeHtml(subject)} ${escapeHtml(String(edge.phrase).trim())} ${escapeHtml(targetLabel)}.`];
    if (rawType) return [`${escapeHtml(subject)} is linked to ${escapeHtml(targetLabel)} as ${escapeHtml(rawType.replace(/\s*\([^)]*\)\s*$/, ""))}.`];
    return [`${escapeHtml(subject)} is linked to ${escapeHtml(targetLabel)}.`];
  }

  function setSidebarTab(tabName) {
    sidebarTabEls.forEach((element) => element.classList.toggle("active", element.dataset.tab === tabName));
    sidebarPaneEls.forEach((element) => element.classList.toggle("active", element.dataset.pane === tabName));
    if (tabName === "map" && addressMap) {
      setTimeout(() => addressMap.invalidateSize(), 0);
    }
  }

  function toggleSidebar(forceOpen = null) {
    const nextOpen = forceOpen == null ? !sidebarEl.classList.contains("open") : !!forceOpen;
    sidebarEl.classList.toggle("open", nextOpen);
    toggleSidebarButton.classList.toggle("open", nextOpen);
    renderExtraTreeSummary();
  }

  function rankedCategoryForNode(node) {
    if (!node) return "";
    if (node.kind === "address") return "addresses";
    if (node.kind === "organisation") return "orgs";
    if (node.kind === "seed_alias" || node.kind === "person") return "people";
    return "";
  }

  function rankedEdgeCounts() {
    const counts = new Map();
    visibleEdges.forEach((edge) => {
      counts.set(edge.source, Number(counts.get(edge.source) || 0) + 1);
      counts.set(edge.target, Number(counts.get(edge.target) || 0) + 1);
    });
    return counts;
  }

  function rankedNodeScore(node, category, edgeCounts) {
    if (category === "people") return nodeRankScore(node);
    const visibleLinks = Number(edgeCounts.get(node.id) || 0);
    const seedRefs = Array.isArray(node.seed_names) ? node.seed_names.length : 0;
    if (category === "orgs") {
      return (Number(node.people_count || 0) * 3.2)
        + (visibleLinks * 1.4)
        + (seedRefs * 0.8)
        + (node.shared ? 1.5 : 0);
    }
    if (category === "addresses") {
      return (visibleLinks * 2)
        + (seedRefs * 0.8)
        + (node.shared ? 1.2 : 0);
    }
    return 0;
  }

  function rankedNodeMeta(node, category, edgeCounts) {
    const visibleLinks = Number(edgeCounts.get(node.id) || 0);
    if (category === "people") {
      return `${Number(node.org_count || 0)} orgs, ${Number(node.role_count || 0)} roles`;
    }
    if (category === "orgs") {
      const seedRefs = Array.isArray(node.seed_names) ? node.seed_names.length : 0;
      return `${Number(node.people_count || 0)} people, ${visibleLinks} visible links${seedRefs ? `, ${seedRefs} seeds` : ""}`;
    }
    return [
      `${visibleLinks} visible links`,
      String(node.postcode || "").trim(),
      String(node.country || "").trim(),
    ].filter(Boolean).join(", ");
  }

  function renderScorePanel() {
    const category = ["people", "orgs", "addresses"].includes(viewerState.rankedCategory)
      ? viewerState.rankedCategory
      : "people";
    const edgeCounts = rankedEdgeCounts();
    const tabButtons = `
      <div class="score-type-tabs">
        <button type="button" class="score-type-tab ${category === "people" ? "active" : ""}" data-ranked-type="people">People</button>
        <button type="button" class="score-type-tab ${category === "orgs" ? "active" : ""}" data-ranked-type="orgs">Orgs</button>
        <button type="button" class="score-type-tab ${category === "addresses" ? "active" : ""}" data-ranked-type="addresses">Addresses</button>
      </div>
    `;
    const rankedNodes = visibleNodes
      .filter((node) => rankedCategoryForNode(node) === category)
      .sort((left, right) => {
        const scoreDiff = rankedNodeScore(right, category, edgeCounts) - rankedNodeScore(left, category, edgeCounts);
        if (scoreDiff !== 0) return scoreDiff;
        return String(left.label || "").localeCompare(String(right.label || ""));
      })
      .slice(0, 12);
    scorePanelEl.innerHTML = rankedNodes.length
      ? `
        ${tabButtons}
        <h2>Top ranked on screen</h2>
        <div class="score-list">
          ${rankedNodes.map((node) => `
            <div class="score-item">
              <div class="score-item-title">
                <strong>${escapeHtml(node.label || "Unknown")}</strong>
                <span>${rankedNodeScore(node, category, edgeCounts).toFixed(2)}</span>
              </div>
              <div class="score-item-meta">${escapeHtml(rankedNodeMeta(node, category, edgeCounts))}</div>
            </div>
          `).join("")}
        </div>
      `
      : `${tabButtons}<div class="score-empty">No visible ${category === "orgs" ? "organisations" : category} are currently on screen.</div>`;
  }

  async function ensureLowConfidenceLoaded() {
    if (lowConfidenceLoaded) return true;
    if (lowConfidenceLoadingPromise) return lowConfidenceLoadingPromise;
    lowConfidenceLoadingPromise = fetch(LOW_CONFIDENCE_DATA_URL)
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load low-confidence graph (${response.status})`);
        return response.json();
      })
      .then((payload) => {
        lowConfidenceNodes = Array.isArray(payload.nodes) ? payload.nodes : [];
        lowConfidenceEdges = Array.isArray(payload.edges) ? payload.edges : [];
        rebuildLowConfidenceIndexes();
        lowConfidenceLoaded = true;
        return true;
      })
      .catch((error) => {
        console.error(error);
        lowConfidenceNodes = [];
        lowConfidenceEdges = [];
        rebuildLowConfidenceIndexes();
        lowConfidenceLoaded = false;
        return false;
      })
      .finally(() => {
        lowConfidenceLoadingPromise = null;
      });
    return lowConfidenceLoadingPromise;
  }

  async function ensureLowConfidenceOrgLoaded() {
    if (lowConfidenceOrgLoaded) return true;
    if (lowConfidenceOrgLoadingPromise) return lowConfidenceOrgLoadingPromise;
    lowConfidenceOrgLoadingPromise = fetch(LOW_CONFIDENCE_NODES_DATA_URL)
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load low-confidence nodes (${response.status})`);
        return response.json();
      })
      .then((payload) => {
        lowConfidenceOrgNodes = Array.isArray(payload.nodes) ? payload.nodes : [];
        lowConfidenceOrgEdges = Array.isArray(payload.edges) ? payload.edges : [];
        rebuildLowConfidenceOrgIndexes();
        lowConfidenceOrgLoaded = true;
        return true;
      })
      .catch((error) => {
        console.error(error);
        lowConfidenceOrgNodes = [];
        lowConfidenceOrgEdges = [];
        rebuildLowConfidenceOrgIndexes();
        lowConfidenceOrgLoaded = false;
        return false;
      })
      .finally(() => {
        lowConfidenceOrgLoadingPromise = null;
      });
    return lowConfidenceOrgLoadingPromise;
  }

  function isCompaniesHouseDocumentUrl(value) {
    try {
      const url = new URL(String(value || "").trim(), window.location.origin);
      return /(^|\.)document-api\.company-information\.service\.gov\.uk$/i.test(url.hostname)
        && /^\/document\/.+/.test(url.pathname);
    } catch (_error) {
      return false;
    }
  }

  function evidenceActionUrl(evidence) {
    const documentUrl = String(evidence?.document_url || "").trim();
    if (!documentUrl) return "";
    const pageNumber = Number(evidence?.page_number || 0);
    if (isCompaniesHouseDocumentUrl(documentUrl)) {
      const params = new URLSearchParams({ url: documentUrl });
      if (pageNumber) params.set("page", String(pageNumber));
      return `${EVIDENCE_FILE_URL}?${params.toString()}`;
    }
    if (!pageNumber || documentUrl.includes("#") || !/\.pdf($|[?#])/i.test(documentUrl)) return documentUrl;
    return `${documentUrl}#page=${pageNumber}`;
  }

  function evidenceDisplayTitle(evidence, fallback = "Evidence") {
    const rawTitle = String(evidence?.title || "").trim();
    if (rawTitle && !rawTitle.includes("$")) return rawTitle;
    const pathValue = String(evidence?.path || "").trim();
    if (pathValue) {
      const parts = pathValue.split(/[\\/]/).filter(Boolean);
      const lastPart = parts.length ? parts[parts.length - 1] : "";
      if (lastPart && !lastPart.includes("$")) return lastPart;
    }
    const urlValue = String(evidenceActionUrl(evidence) || evidence?.document_url || "").trim();
    if (urlValue) {
      try {
        const url = new URL(urlValue, window.location.origin);
        const fileName = decodeURIComponent((url.pathname.split("/").filter(Boolean).pop() || "").trim());
        if (fileName && fileName !== "evidence-file" && !fileName.includes("$")) return fileName;
        const host = url.hostname.replace(/^www\./i, "").trim();
        if (host) return host;
      } catch (_error) {
        if (!urlValue.includes("$")) return urlValue;
      }
    }
    return fallback;
  }

  function edgeSubtitle(edge) {
    if (edge?.kind === "hidden_connection") return "Indirect connection";
    return String(edge?.phrase || edge?.role_label || edge?.role_type || edge?.kind || "link")
      .replaceAll("_", " ")
      .trim();
  }

  function plainText(value) {
    return String(value || "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function uniqueValues(values) {
    const seen = new Set();
    const result = [];
    values.forEach((value) => {
      const text = String(value || "").trim();
      if (!text || seen.has(text)) return;
      seen.add(text);
      result.push(text);
    });
    return result;
  }

  function normalizeMergeOverrideRows(rows) {
    const seen = new Set();
    return (Array.isArray(rows) ? rows : []).map((row) => ({
      sourceId: String(row?.sourceId || ""),
      targetId: String(row?.targetId || ""),
      leaderId: String(row?.leaderId || ""),
    })).filter((row) => {
      if (!row.sourceId || !row.targetId || row.sourceId === row.targetId) return false;
      const key = `${row.sourceId}||${row.targetId}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function normalizeHiddenOverrideRows(rows) {
    const seen = new Set();
    return (Array.isArray(rows) ? rows : []).map((row) => ({
      nodeId: String(row?.nodeId || ""),
      label: String(row?.label || ""),
    })).filter((row) => {
      if (!row.nodeId) return false;
      if (seen.has(row.nodeId)) return false;
      seen.add(row.nodeId);
      return true;
    });
  }

  function mergeKindForNode(node) {
    if (!node) return null;
    if (node.is_low_confidence && String(node.low_confidence_category || "") !== "unresolved_org") return null;
    if (node.kind === "address") return "address";
    if (node.kind === "person" || node.kind === "seed_alias" || node.lane === 1) return "name";
    if (node.kind === "organisation") return "organisation";
    return null;
  }

  function nodeMergeStableKeys(node) {
    const kind = mergeKindForNode(node);
    if (!kind) return [];
    if (kind === "address") {
      return uniqueValues(
        ([
          ...(Array.isArray(node.normalized_keys) ? node.normalized_keys : []),
          node.normalized_key,
        ]
          .map((value) => String(value || "").trim())
          .filter(Boolean)
          .map((value) => `address:${value}`)),
      );
    }
    if (kind === "organisation") {
      const registryType = String(node.registry_type || "").trim().toLowerCase();
      const registryNumber = String(node.registry_number || "").trim();
      const suffix = Number(node.suffix || 0);
      return uniqueValues([
        registryType && registryNumber ? `org:${registryType}:${registryNumber}:${suffix}` : "",
        registryType && registryNumber ? `org:${registryType}:${registryNumber}` : "",
        ...(Array.isArray(node.organisation_merge_keys) ? node.organisation_merge_keys.map((value) => String(value || "").trim()) : []),
        node.unresolved_org_key ? String(node.unresolved_org_key) : "",
        `node-id:${String(node.id)}`,
        String(node.label || "").trim() ? `label:${String(node.label).trim().toLowerCase()}` : "",
      ]);
    }
    return uniqueValues([
      `node-id:${String(node.id)}`,
      ...(Array.isArray(node.person_ids) ? node.person_ids.map((value) => `person:${String(value)}`) : []),
      ...(Array.isArray(node.identity_keys) ? node.identity_keys.map((value) => `identity:${String(value || "").trim()}`) : []),
      node.individual_key ? `individual:${String(node.individual_key)}` : "",
      String(node.label || "").trim() ? `label:${String(node.label).trim().toLowerCase()}` : "",
    ]);
  }

  function nodeMergePrimaryKey(node) {
    return nodeMergeStableKeys(node)[0] || "";
  }

  function nodeHideStableKeys(node) {
    if (!node || node.is_low_confidence || node.kind === "seed") return [];
    if (node.kind === "address") {
      return uniqueValues(
        [
          ...(Array.isArray(node.normalized_keys) ? node.normalized_keys : []),
          node.normalized_key,
        ]
          .map((value) => String(value || "").trim())
          .filter(Boolean)
          .map((value) => `address:${value}`),
      );
    }
    if (node.kind === "organisation") {
      const registryType = String(node.registry_type || "").trim().toLowerCase();
      const registryNumber = String(node.registry_number || "").trim();
      const suffix = Number(node.suffix || 0);
      return uniqueValues([
        registryType && registryNumber ? `org:${registryType}:${registryNumber}:${suffix}` : "",
        registryType && registryNumber ? `org:${registryType}:${registryNumber}` : "",
        `node-id:${String(node.id || "")}`,
        String(node.label || "").trim() ? `label:${String(node.label).trim().toLowerCase()}` : "",
      ]);
    }
    return uniqueValues([
      ...(Array.isArray(node.person_ids) ? node.person_ids.map((value) => `person:${String(value)}`) : []),
      ...(Array.isArray(node.identity_keys) ? node.identity_keys.map((value) => `identity:${String(value || "").trim()}`) : []),
      node.individual_key ? `individual:${String(node.individual_key)}` : "",
      `node-id:${String(node.id || "")}`,
      String(node.label || "").trim() ? `label:${String(node.label).trim().toLowerCase()}` : "",
    ]);
  }

  function nodeHidePrimaryKey(node) {
    return nodeHideStableKeys(node)[0] || "";
  }

  function nodeIsPersistentlyHidden(node, hiddenKeySet) {
    return nodeHideStableKeys(node).some((key) => hiddenKeySet.has(key));
  }

  function cloneNodeForMerge(node) {
    return {
      ...node,
      aliases: Array.isArray(node.aliases) ? node.aliases.slice() : [],
      identity_keys: Array.isArray(node.identity_keys) ? node.identity_keys.slice() : [],
      person_ids: Array.isArray(node.person_ids) ? node.person_ids.slice() : [],
      normalized_keys: Array.isArray(node.normalized_keys) ? node.normalized_keys.slice() : [],
      organisation_merge_keys: Array.isArray(node.organisation_merge_keys) ? node.organisation_merge_keys.slice() : [],
      seed_names: Array.isArray(node.seed_names) ? node.seed_names.slice() : [],
      tooltip_lines: Array.isArray(node.tooltip_lines) ? node.tooltip_lines.slice() : [],
      appears_under_identities: Array.isArray(node.appears_under_identities)
        ? node.appears_under_identities.map((item) => ({ ...item }))
        : [],
      manual_merge_rows: Array.isArray(node.manual_merge_rows)
        ? node.manual_merge_rows.map((row) => ({ ...row }))
        : [],
    };
  }

  function nodeHasStableKey(node, stableKey) {
    return !!stableKey && nodeMergeStableKeys(node).includes(String(stableKey || ""));
  }

  function mergeNodeData(target, source, row = null) {
    const preferredLeaderKey = String(row?.leaderId || "");
    if (preferredLeaderKey) {
      if (nodeHasStableKey(source, preferredLeaderKey)) {
        target.label = source.label;
      }
    } else if (String(source.label || "").length > String(target.label || "").length) {
      target.label = source.label;
    }
    target.aliases = uniqueValues([
      ...(Array.isArray(target.aliases) ? target.aliases : []),
      ...(Array.isArray(source.aliases) ? source.aliases : []),
      source.label,
    ]).filter((value) => value !== String(target.label || ""));
    target.identity_keys = uniqueValues([
      ...(Array.isArray(target.identity_keys) ? target.identity_keys : []),
      ...(Array.isArray(source.identity_keys) ? source.identity_keys : []),
    ]);
    target.person_ids = uniqueValues([
      ...(Array.isArray(target.person_ids) ? target.person_ids : []),
      ...(Array.isArray(source.person_ids) ? source.person_ids : []),
    ]);
    target.normalized_keys = uniqueValues([
      ...(Array.isArray(target.normalized_keys) ? target.normalized_keys : []),
      ...(Array.isArray(source.normalized_keys) ? source.normalized_keys : []),
    ]);
    target.seed_names = uniqueValues([
      ...(Array.isArray(target.seed_names) ? target.seed_names : []),
      ...(Array.isArray(source.seed_names) ? source.seed_names : []),
    ]);
    target.tooltip_lines = uniqueValues([
      ...(Array.isArray(target.tooltip_lines) ? target.tooltip_lines : []),
      ...(Array.isArray(source.tooltip_lines) ? source.tooltip_lines : []),
      `Merged with ${String(source.label || source.id || "node")}`,
    ]);
    target.appears_under_identities = [
      ...(Array.isArray(target.appears_under_identities) ? target.appears_under_identities : []),
      ...(Array.isArray(source.appears_under_identities) ? source.appears_under_identities : []),
    ];
    target.org_count = Math.max(Number(target.org_count || 0), Number(source.org_count || 0));
    target.role_count = Math.max(Number(target.role_count || 0), Number(source.role_count || 0));
    target.score = Math.max(Number(target.score || 0), Number(source.score || 0));
    target.shared = !!target.shared || !!source.shared;
    if (row?.sourceId && row?.targetId) {
      const entry = {
        kind: String(row.kind || mergeKindForNode(target) || ""),
        sourceId: String(row.sourceId || ""),
        targetId: String(row.targetId || ""),
        leaderId: String(row.leaderId || ""),
        sourceLabel: String(row.sourceLabel || source.label || source.id || "node"),
        targetLabel: String(row.targetLabel || target.label || target.id || "node"),
        leaderLabel: String(
          row.leaderLabel
          || (nodeHasStableKey(source, preferredLeaderKey) ? source.label : target.label)
          || target.id
          || "node"
        ),
      };
      const entryKey = `${entry.sourceId}||${entry.targetId}`;
      target.manual_merge_rows = [
        ...(Array.isArray(target.manual_merge_rows) ? target.manual_merge_rows.filter((item) => `${item.sourceId}||${item.targetId}` !== entryKey) : []),
        entry,
      ];
    }
  }

  function dedupeMergedEdges(edges) {
    const seen = new Map();
    const result = [];
    edges.forEach((edge) => {
      if (!edge || edge.source === edge.target) return;
      const key = [
        edge.source,
        edge.target,
        edge.kind || "",
        edge.phrase || "",
        edge.role_type || "",
        edge.role_label || "",
        edge.tooltip || "",
      ].join("||");
      const existing = seen.get(key);
      if (existing) {
        if (!existing.evidence && edge.evidence) existing.evidence = edge.evidence;
        if ((!existing.evidence_items || !existing.evidence_items.length) && edge.evidence_items?.length) {
          existing.evidence_items = edge.evidence_items;
        }
        if ((!existing.tooltip_lines || !existing.tooltip_lines.length) && edge.tooltip_lines?.length) {
          existing.tooltip_lines = edge.tooltip_lines;
        }
        existing.display_person_labels = uniqueValues([
          ...(Array.isArray(existing.display_person_labels) ? existing.display_person_labels : []),
          ...(Array.isArray(edge.display_person_labels) ? edge.display_person_labels : []),
        ]);
        return;
      }
      seen.set(key, edge);
      result.push(edge);
    });
    return result;
  }

  function applyMergeOverrides(nodes, edges, overrides) {
    const nextNodes = nodes.map(cloneNodeForMerge);
    const nextEdges = edges.map((edge) => ({ ...edge }));
    const nodeByMergeId = new Map(nextNodes.map((node) => [node.id, node]));
    const stableLookupByKind = new Map();
    const stableLabelLookupByKind = new Map();
    ["address", "name", "organisation"].forEach((kind) => {
      const lookup = new Map();
      const labelLookup = new Map();
      nextNodes.forEach((node) => {
        if (mergeKindForNode(node) !== kind) return;
        nodeMergeStableKeys(node).forEach((key) => {
          if (!lookup.has(key)) lookup.set(key, node.id);
          if (!labelLookup.has(key)) labelLookup.set(key, String(node.label || node.id || ""));
        });
      });
      stableLookupByKind.set(kind, lookup);
      stableLabelLookupByKind.set(kind, labelLookup);
    });

    const redirects = new Map();
    const appliedRows = [];
    ["address", "name", "organisation"].forEach((kind) => {
      const lookup = stableLookupByKind.get(kind) || new Map();
      const labelLookup = stableLabelLookupByKind.get(kind) || new Map();
      normalizeMergeOverrideRows(overrides?.[kind]).forEach((row) => {
        const sourceNodeId = lookup.get(String(row?.sourceId || ""));
        const targetNodeId = lookup.get(String(row?.targetId || ""));
        if (!sourceNodeId || !targetNodeId || sourceNodeId === targetNodeId) return;
        redirects.set(sourceNodeId, targetNodeId);
        const leaderId = labelLookup.has(String(row?.leaderId || ""))
          ? String(row?.leaderId || "")
          : String(row?.targetId || "");
        appliedRows.push({
          kind,
          sourceId: String(row.sourceId || ""),
          targetId: String(row.targetId || ""),
          leaderId,
          sourceNodeId,
          targetNodeId,
          sourceLabel: String(labelLookup.get(String(row.sourceId || "")) || sourceNodeId),
          targetLabel: String(labelLookup.get(String(row.targetId || "")) || targetNodeId),
          leaderLabel: String(labelLookup.get(leaderId) || labelLookup.get(String(row.targetId || "")) || targetNodeId),
        });
      });
    });

    function resolveNodeId(nodeId) {
      let currentId = nodeId;
      const seen = new Set();
      while (redirects.has(currentId) && !seen.has(currentId)) {
        seen.add(currentId);
        currentId = redirects.get(currentId);
      }
      return currentId;
    }

    const rowBySourceNodeId = new Map();
    appliedRows.forEach((row) => {
      row.resolvedTargetNodeId = resolveNodeId(row.targetNodeId);
      rowBySourceNodeId.set(row.sourceNodeId, row);
    });

    nextNodes.forEach((node) => {
      const targetNodeId = resolveNodeId(node.id);
      if (targetNodeId === node.id) return;
      const targetNode = nodeByMergeId.get(targetNodeId);
      if (!targetNode) return;
      mergeNodeData(targetNode, node, rowBySourceNodeId.get(node.id) || null);
    });

    const keptNodes = nextNodes.filter((node) => resolveNodeId(node.id) === node.id);
    const keptIds = new Set(keptNodes.map((node) => node.id));
    const rewrittenEdges = nextEdges
      .map((edge) => ({
        ...edge,
        source: resolveNodeId(edge.source),
        target: resolveNodeId(edge.target),
      }))
      .filter((edge) => keptIds.has(edge.source) && keptIds.has(edge.target));

    return {
      nodes: keptNodes,
      edges: dedupeMergedEdges(rewrittenEdges),
    };
  }

  function applyHiddenOverrides(nodes, edges, overrides) {
    const hiddenRows = normalizeHiddenOverrideRows(overrides?.hidden);
    if (!hiddenRows.length) {
      return {
        nodes: nodes.slice(),
        edges: edges.slice(),
      };
    }
    const hiddenKeySet = new Set(hiddenRows.map((row) => row.nodeId));
    const keptNodes = nodes.filter((node) => !nodeIsPersistentlyHidden(node, hiddenKeySet));
    const keptIds = new Set(keptNodes.map((node) => node.id));
    return {
      nodes: keptNodes,
      edges: edges.filter((edge) => keptIds.has(edge.source) && keptIds.has(edge.target)),
    };
  }

  function rebuildBaseGraph() {
    const merged = applyMergeOverrides(rawMainNodes, rawMainEdges, mergeOverrides);
    const filtered = applyHiddenOverrides(merged.nodes, merged.edges, mergeOverrides);
    baseNodes = filtered.nodes.slice();
    baseEdges = filtered.edges.slice();
    baseNodeById = new Map(baseNodes.map((node) => [node.id, node]));
    baseEdgesByNodeId = new Map();
    baseEdges.forEach((edge) => {
      if (!baseEdgesByNodeId.has(edge.source)) baseEdgesByNodeId.set(edge.source, []);
      if (!baseEdgesByNodeId.has(edge.target)) baseEdgesByNodeId.set(edge.target, []);
      baseEdgesByNodeId.get(edge.source).push(edge);
      baseEdgesByNodeId.get(edge.target).push(edge);
    });
  }

  function evidenceLabelForEdge(edge) {
    const firstLine = tooltipLinesForEdge(edge)[0];
    const summary = plainText(firstLine);
    return summary ? `Evidence for: ${summary}` : "Evidence";
  }

  function nodeAttributionEdges(node) {
    return (edgesByNodeId.get(node?.id) || [])
      .filter((edge) => edge && edge.kind !== "hidden_connection" && edge.kind !== "shared_org" && edge.kind !== "cross_seed")
      .sort((left, right) => {
        const leftEvidence = evidenceActionsForEdge(left).length;
        const rightEvidence = evidenceActionsForEdge(right).length;
        if (rightEvidence !== leftEvidence) return rightEvidence - leftEvidence;
        return edgeSubtitle(left).localeCompare(edgeSubtitle(right));
      });
  }

  function summaryLinesForNodeAttribution(node, edges = []) {
    const lines = tooltipLinesForNode(node)
      .filter((line) => !(node?.kind === "person" && /^\s{2}/.test(String(line || "")) && String(line || "").includes("<em>")))
      .map((line) => plainText(line))
      .filter(Boolean);
    if (!edges.length) return lines;
    return lines;
  }

  function adverseMediaCategoryLabel(category) {
    if (category === "explicit_mb_connection") return "Explicit MB connection";
    if (category === "writes_for_mb_outlet") return "Writes for MB outlet";
    if (category === "other_mb_alignment") return "Other MB alignment";
    return "Adverse media";
  }

  function sanctionSourceLabel(source) {
    const value = String(source || "").trim();
    if (value === "Direction Generale du Tresor") return "France Treasury";
    if (value === "Germany Finanzsanktionsliste") return "Germany Sanctions List";
    return value || "Sanctions list";
  }

  function renderSanctionsHtml(node) {
    const matches = Array.isArray(node?.sanction_matches) ? node.sanction_matches : [];
    if (!matches.length) return "";
    const summarySources = Array.isArray(node?.sanction_sources)
      ? node.sanction_sources.map((value) => String(value || "").trim()).filter(Boolean)
      : [];
    const summaryText = summarySources.length ? summarySources.join(" · ") : "";
    return `
      <div class="analysis-section">
        <div class="analysis-section-title">Potential sanctions match</div>
        ${summaryText ? `<div class="analysis-text">${escapeHtml(summaryText)}</div>` : ""}
      </div>
    `;
  }

  function renderAdverseMediaHtml(node) {
    const claims = Array.isArray(node?.adverse_media_claims) ? node.adverse_media_claims : [];
    if (!claims.length) return "";
    return `
      <div class="analysis-section">
        <div class="analysis-section-title">Adverse media</div>
        <div class="analysis-claims">
          ${claims.map((claim, index) => {
            const title = String(claim?.translated_title || claim?.title || "").trim();
            const category = adverseMediaCategoryLabel(String(claim?.category || "").trim());
            const confidence = Number(claim?.confidence || 0);
            const rationale = String(claim?.short_rationale || "").trim();
            const quote = String(claim?.evidence_quote || "").trim();
            const url = String(claim?.url || "").trim();
            const confidenceText = Number.isFinite(confidence) && confidence > 0 ? `Confidence ${confidence.toFixed(2)}` : "";
            const metaBits = [category, confidenceText].filter(Boolean).join(" · ");
            return `
              <div class="analysis-claim adverse-media-claim">
                <div class="analysis-claim-header">
                  <div class="analysis-claim-index">${index + 1}</div>
                  <div class="analysis-claim-text">${escapeHtml(title || category)}</div>
                </div>
                ${metaBits ? `<div class="analysis-claim-meta">${escapeHtml(metaBits)}</div>` : ""}
                ${rationale ? `<div class="analysis-claim-note">${escapeHtml(rationale)}</div>` : ""}
                ${quote ? `<div class="analysis-claim-quote">${escapeHtml(quote)}</div>` : ""}
                <div class="analysis-claim-evidence">
                  <span class="analysis-claim-evidence-label">Article</span>
                  ${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(title || "Open article")}</a>` : '<span class="dim">No linked article.</span>'}
                </div>
              </div>
            `;
          }).join("")}
        </div>
      </div>
    `;
  }

  function renderEgyptJudgmentsHtml(node) {
    const matches = Array.isArray(node?.egypt_judgment_matches) ? node.egypt_judgment_matches : [];
    if (!matches.length) return "";
    return `
      <div class="analysis-section">
        <div class="analysis-section-title">Egypt judgments screen</div>
        <div class="analysis-claims">
          ${matches.map((match, index) => {
            const canonicalName = String(match?.canonical_name || "").trim();
            const matchedName = String(match?.matched_name || "").trim();
            const matchedAlias = String(match?.matched_alias || "").trim();
            const sourceType = String(match?.source_type || "").trim().replaceAll("_", " ");
            const sourceLabel = String(match?.source_label || "").trim();
            const listName = String(match?.list_name || "").trim();
            const sourceUrl = String(match?.source_url || "").trim();
            const metaBits = [sourceLabel, sourceType].filter(Boolean).join(" · ");
            const noteBits = [];
            if (matchedName && matchedName !== canonicalName) noteBits.push(`Node matched as ${matchedName}`);
            if (matchedAlias && matchedAlias !== canonicalName) noteBits.push(`Dataset alias ${matchedAlias}`);
            return `
              <div class="analysis-claim egypt-judgment-claim">
                <div class="analysis-claim-header">
                  <div class="analysis-claim-index">${index + 1}</div>
                  <div class="analysis-claim-text">${escapeHtml(canonicalName || matchedName || "Egypt judgment match")}</div>
                </div>
                ${metaBits ? `<div class="analysis-claim-meta">${escapeHtml(metaBits)}</div>` : ""}
                ${noteBits.length ? `<div class="analysis-claim-note">${escapeHtml(noteBits.join(" · "))}</div>` : ""}
                <div class="analysis-claim-evidence">
                  <span class="analysis-claim-evidence-label">Source</span>
                  ${sourceUrl ? `<a href="${escapeHtml(sourceUrl)}" target="_blank" rel="noreferrer">${escapeHtml(listName || sourceLabel || "Open source")}</a>` : `<span>${escapeHtml(listName || sourceLabel || "No linked source")}</span>`}
                </div>
              </div>
            `;
          }).join("")}
        </div>
      </div>
    `;
  }

  function renderNodeAttributionHtml(node) {
    const edges = nodeAttributionEdges(node);
    const summary = summaryLinesForNodeAttribution(node, edges);
    const sanctionsHtml = renderSanctionsHtml(node);
    const egyptJudgmentsHtml = renderEgyptJudgmentsHtml(node);
    const adverseMediaHtml = renderAdverseMediaHtml(node);
    return `
      <div class="analysis-viewer">
        <div class="analysis-selection">${escapeHtml(node.label || node.id || "Node")}</div>
        ${summary.length ? `<div class="analysis-text">${summary.map((line) => escapeHtml(line)).join("<br>")}</div>` : ""}
        ${sanctionsHtml}
        ${egyptJudgmentsHtml}
        ${adverseMediaHtml}
        <div class="analysis-section">
          ${edges.length ? '<div class="analysis-section-title">Graph claims</div>' : ""}
          <div class="analysis-claims">
          ${edges.length ? edges.map((edge, index) => {
            const links = evidenceActionsForEdge(edge)
              .map((action) => `<a href="${escapeHtml(action.url)}" target="_blank" rel="noreferrer">${escapeHtml(action.label)}</a>`)
              .join("");
            return `
              <div class="analysis-claim">
                <div class="analysis-claim-header">
                  <div class="analysis-claim-index">${index + 1}</div>
                  <div class="analysis-claim-text">${escapeHtml(plainText(tooltipLinesForEdge(edge)[0] || ""))}</div>
                </div>
                <div class="analysis-claim-evidence">
                  <span class="analysis-claim-evidence-label">Evidence</span>
                  ${links || '<span class="dim">No linked evidence.</span>'}
                </div>
              </div>
            `;
          }).join("") : ((egyptJudgmentsHtml || adverseMediaHtml) ? '<div class="analysis-empty">No direct graph claims are attached to this node.</div>' : '<div class="analysis-empty">No direct claims or attributions are attached to this node in the current graph.</div>')}
          </div>
        </div>
      </div>
    `;
  }

  function openDetailsModal({ title, status = "", bodyHtml = "" }) {
    detailsModalTitleEl.textContent = title || "Details";
    detailsModalStatusEl.textContent = status || "";
    detailsModalBodyEl.innerHTML = bodyHtml || '<div class="analysis-empty">No details available.</div>';
    detailsModalEl.classList.add("open");
    detailsModalEl.setAttribute("aria-hidden", "false");
  }

  function closeDetailsModal() {
    detailsModalEl.classList.remove("open");
    detailsModalEl.setAttribute("aria-hidden", "true");
  }

  function openNodeAttributionView(node) {
    openDetailsModal({
      title: node?.label || "Details",
      status: "Claims and attribution",
      bodyHtml: renderNodeAttributionHtml(node),
    });
  }

  async function ensureMergeOverridesLoaded() {
    if (mergeOverridesLoadingPromise) return mergeOverridesLoadingPromise;
    mergeOverridesLoadingPromise = fetch(graphFunctionUrl(MERGE_OVERRIDES_URL))
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load merge overrides (${response.status})`);
        return response.json();
      })
      .then((payload) => {
        const overrides = payload?.overrides || {};
        mergeOverrides = {
          address: normalizeMergeOverrideRows(overrides.address),
          name: normalizeMergeOverrideRows(overrides.name),
          organisation: normalizeMergeOverrideRows(overrides.organisation),
          hidden: normalizeHiddenOverrideRows(overrides.hidden),
        };
        rebuildBaseGraph();
        return true;
      })
      .catch((error) => {
        console.warn(error);
        mergeOverrides = { address: [], name: [], organisation: [], hidden: [] };
        rebuildBaseGraph();
        return false;
      })
      .finally(() => {
        mergeOverridesLoadingPromise = null;
      });
    return mergeOverridesLoadingPromise;
  }

  async function persistMergeOverride(action) {
    const response = await fetch(graphFunctionUrl(MERGE_OVERRIDES_URL), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        graph: currentGraphKey,
        operation: String(action.operation || "add"),
        kind: action.kind,
        sourceId: action.sourceKey,
        targetId: action.targetKey,
        leaderId: action.leaderKey,
      }),
    });
    if (!response.ok) {
      throw new Error(`Merge persistence failed (${response.status})`);
    }
    const payload = await response.json();
    const overrides = payload?.overrides || {};
    mergeOverrides = {
      address: normalizeMergeOverrideRows(overrides.address),
      name: normalizeMergeOverrideRows(overrides.name),
      organisation: normalizeMergeOverrideRows(overrides.organisation),
      hidden: normalizeHiddenOverrideRows(overrides.hidden),
    };
    viewerState.pendingMergeNodeId = "";
    rebuildBaseGraph();
    await applyViewerState();
  }

  async function persistHiddenOverride(action, options = {}) {
    const response = await fetch(graphFunctionUrl(MERGE_OVERRIDES_URL), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        graph: currentGraphKey,
        operation: String(action.operation || "add"),
        kind: "hidden",
        nodeId: action.nodeKey,
        label: action.nodeLabel,
      }),
    });
    if (!response.ok) {
      throw new Error(`Hidden-node persistence failed (${response.status})`);
    }
    const payload = await response.json();
    const overrides = payload?.overrides || {};
    mergeOverrides = {
      address: normalizeMergeOverrideRows(overrides.address),
      name: normalizeMergeOverrideRows(overrides.name),
      organisation: normalizeMergeOverrideRows(overrides.organisation),
      hidden: normalizeHiddenOverrideRows(overrides.hidden),
    };
    if (options.refresh !== false) {
      rebuildBaseGraph();
      await applyViewerState();
    }
  }

  function promptForMergeLeader(action) {
    const choice = window.prompt(
      `Choose which label should lead this merge:\n1. ${action.sourceLabel}\n2. ${action.targetLabel}\n\nEnter 1 or 2.`,
      "2",
    );
    if (choice === null) return "";
    const trimmed = String(choice || "").trim();
    if (trimmed === "1") return action.sourceKey;
    if (trimmed === "2") return action.targetKey;
    window.alert("Please enter 1 or 2.");
    return "";
  }

  function registryActionForNode(node) {
    const registryType = String(node?.registry_type || "").toLowerCase();
    const registryNumber = String(node?.registry_number || node?.registry_id || node?.external_id || "").trim();
    if (!registryType || !registryNumber) return null;
    if (registryType === "company") {
      return {
        type: "open_url",
        label: "Open Companies House page",
        url: `https://find-and-update.company-information.service.gov.uk/company/${encodeURIComponent(registryNumber)}`,
      };
    }
    if (registryType === "charity") {
      return {
        type: "open_url",
        label: "Open Charity Commission page",
        url: `https://register-of-charities.charitycommission.gov.uk/charity-details/?regid=${encodeURIComponent(registryNumber)}&subid=0`,
      };
    }
    return null;
  }

  function openContextMenu(node, event) {
    event.preventDefault();
    event.stopPropagation();
    hideTooltip();
    const expandableLowConfidenceNode = isLowConfidenceDocumentNode(node);
    const mergeKind = mergeKindForNode(node);
    const mergePrimaryKey = nodeMergePrimaryKey(node);
    const hidePrimaryKey = nodeHidePrimaryKey(node);
    const pendingMergeNode = nodeById.get(viewerState.pendingMergeNodeId) || null;
    const compatiblePendingMergeNode = pendingMergeNode
      && pendingMergeNode.id !== node.id
      && mergeKind
      && mergeKindForNode(pendingMergeNode) === mergeKind
      && nodeMergePrimaryKey(pendingMergeNode)
      ? pendingMergeNode
      : null;
    const undoActions = (Array.isArray(node.manual_merge_rows) ? node.manual_merge_rows : []).map((row) => ({
      label: `Undo merge with ${row.sourceLabel || row.sourceId}`,
      type: "merge_remove",
      kind: String(row.kind || mergeKind || ""),
      sourceLabel: String(row.sourceLabel || row.sourceId || "node"),
      targetLabel: String(row.targetLabel || node.label || node.id || "node"),
      sourceKey: String(row.sourceId || ""),
      targetKey: String(row.targetId || ""),
    })).filter((row) => row.kind && row.sourceKey && row.targetKey);
    const actions = [
      { label: "Explain claims and attribution", type: "node_claims", nodeId: node.id },
      expandableLowConfidenceNode
        ? {
            label: viewerState.expandedLowConfidenceNodeIds.has(node.id)
              ? "Collapse connected names and organisations"
              : "Expand connected names and organisations",
            type: viewerState.expandedLowConfidenceNodeIds.has(node.id)
              ? "low_confidence_collapse"
              : "low_confidence_expand",
            nodeId: node.id,
          }
        : null,
      registryActionForNode(node),
      hidePrimaryKey
        ? {
            label: "Hide",
            type: "hide_add",
            nodeId: node.id,
            nodeKey: hidePrimaryKey,
            nodeLabel: String(node.label || node.id || "node"),
          }
        : null,
      mergeKind && mergePrimaryKey && compatiblePendingMergeNode
        ? {
            label: `Merge ${compatiblePendingMergeNode.label} into this node`,
            type: "merge_persist",
            kind: mergeKind,
            sourceLabel: compatiblePendingMergeNode.label,
            targetLabel: node.label,
            sourceKey: nodeMergePrimaryKey(compatiblePendingMergeNode),
            targetKey: mergePrimaryKey,
            leaderKey: "",
          }
        : null,
      ...undoActions,
      mergeKind && mergePrimaryKey && viewerState.pendingMergeNodeId === node.id
        ? { label: "Cancel merge", type: "merge_cancel" }
        : mergeKind && mergePrimaryKey
          ? { label: "Start merge", type: "merge_start", nodeId: node.id }
          : null,
      viewerState.focusedNodeIds.has(node.id) ? { label: "Clear focus", type: "focus_clear" } : null,
    ].filter(Boolean);
    contextMenuEl.innerHTML = `
      <div class="context-menu-title">${escapeHtml(node.label || node.id || "Node actions")}</div>
      <div class="context-menu-actions">
        ${actions.map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${index}">${escapeHtml(action.label)}</button>`).join("")}
      </div>
    `;
    contextMenuEl._actions = actions;
    contextMenuEl.style.display = "block";
    contextMenuEl.style.left = `${Math.min(event.clientX, window.innerWidth - 260)}px`;
    contextMenuEl.style.top = `${Math.min(event.clientY, window.innerHeight - 220)}px`;
  }

  function openCanvasContextMenu(event) {
    event.preventDefault();
    event.stopPropagation();
    hideTooltip();
    hideCanvasSearchPopover();
    const hiddenRows = normalizeHiddenOverrideRows(mergeOverrides.hidden);
    const actions = [
      { label: "Add tree...", type: "canvas_add_prompt" },
      ...(hiddenRows.length > 1
        ? [{
            label: `Restore all hidden nodes (${hiddenRows.length})`,
            type: "hide_restore_all",
            rows: hiddenRows,
          }]
        : []),
      ...hiddenRows.map((row) => ({
        label: `Restore ${row.label || row.nodeId}`,
        type: "hide_remove",
        nodeKey: row.nodeId,
        nodeLabel: row.label || row.nodeId,
      })),
      ...viewerState.extraRootIds.map((nodeId) => {
        const node = nodeById.get(nodeId);
        return node ? { label: `Remove ${node.label || node.id}`, type: "canvas_remove_tree", nodeId } : null;
      }).filter(Boolean),
      viewerState.extraRootIds.length ? { label: "Clear added trees", type: "canvas_clear_trees" } : null,
    ].filter(Boolean);
    contextMenuEl.innerHTML = `
      <div class="context-menu-title">Canvas</div>
      <div class="context-menu-actions">
        ${actions.map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${index}">${escapeHtml(action.label)}</button>`).join("")}
      </div>
    `;
    contextMenuEl._actions = actions;
    contextMenuEl.style.display = "block";
    contextMenuEl.style.left = `${Math.min(event.clientX, window.innerWidth - 280)}px`;
    contextMenuEl.style.top = `${Math.min(event.clientY, window.innerHeight - 260)}px`;
    canvasSearchAnchor = { x: event.clientX, y: event.clientY };
  }

  function evidenceActionsForEdge(edge) {
    const evidenceItems = [];
    const seen = new Set();
    const pushEvidence = (evidence, sourceEdge = edge) => {
      if (!evidence || typeof evidence !== "object") return;
      const url = String(evidenceActionUrl(evidence) || evidence.document_url || "").trim();
      const page = String(evidence.page_hint || evidence.page_number || "").trim();
      const key = `${url}||${page}`;
      if (!url || seen.has(key)) return;
      seen.add(key);
      evidenceItems.push({ evidence, sourceEdge });
    };
    (Array.isArray(edge?.evidence_items) ? edge.evidence_items : []).forEach((item) => pushEvidence(item, edge));
    if (edge?.evidence) pushEvidence(edge.evidence, edge);
    (Array.isArray(edge?.pathEdges) ? edge.pathEdges : []).forEach((pathEdge) => {
      (Array.isArray(pathEdge?.evidence_items) ? pathEdge.evidence_items : []).forEach((item) => pushEvidence(item, pathEdge));
      if (pathEdge?.evidence) pushEvidence(pathEdge.evidence, pathEdge);
    });
    return evidenceItems
      .map(({ evidence, sourceEdge }) => {
        const url = evidenceActionUrl(evidence);
        if (!url) return null;
        return {
          type: "open_url",
          label: evidenceLabelForEdge(sourceEdge || edge),
          url,
        };
      })
      .filter(Boolean);
  }

  function openEdgeContextMenu(edge, event) {
    event.preventDefault();
    event.stopPropagation();
    hideTooltip();
    const sourceNode = displayNodeForEdgeId(edge.source, edge?._sourceNode);
    const targetNode = displayNodeForEdgeId(edge.target, edge?._targetNode);
    const actions = [
      ...(edge?.kind === "hidden_connection"
        ? [{ type: "hidden_connection_expand", label: "Expand indirect path", edge }]
        : []),
      ...evidenceActionsForEdge(edge),
    ];
    contextMenuEl._actions = actions;
    contextMenuEl.innerHTML = [
      `<div class="context-menu-title">${escapeHtml(sourceNode?.label || edge.source)} to ${escapeHtml(targetNode?.label || edge.target)}</div>`,
      `<div class="context-menu-subtitle">${escapeHtml(edgeSubtitle(edge) || "link")}</div>`,
      actions.length
        ? actions.map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${index}">${escapeHtml(action.label)}</button>`).join("")
        : '<div class="context-menu-empty">No evidence is available for this link yet.</div>',
    ].join("");
    contextMenuEl.style.display = "block";
    contextMenuEl.style.left = `${Math.max(10, Math.min(event.clientX, window.innerWidth - 260))}px`;
    contextMenuEl.style.top = `${Math.max(10, Math.min(event.clientY, window.innerHeight - 220))}px`;
  }

  function closeContextMenu() {
    contextMenuEl.style.display = "none";
    contextMenuEl._actions = [];
  }

  function renderAnalysisHtml(payload) {
    if (!payload || typeof payload !== "object") {
      return '<div class="analysis-error">Connection analysis returned an invalid payload.</div>';
    }
    const evidenceById = new Map((Array.isArray(payload.evidence) ? payload.evidence : []).map((item) => [String(item.id || ""), item]));
    const claims = Array.isArray(payload.claims) ? payload.claims : [];
    const pathItems = Array.isArray(payload.path) ? payload.path : [];
    const sourceNode = nodeById.get(payload.sourceNodeId);
    const targetNode = nodeById.get(payload.targetNodeId);
    return `
      <div class="analysis-selection">${escapeHtml(sourceNode?.label || payload.sourceNodeId)} to ${escapeHtml(targetNode?.label || payload.targetNodeId)}</div>
      <div class="analysis-text">${escapeHtml(payload.summary || "No explanation returned.").replaceAll("\n", "<br>")}</div>
      ${claims.length
        ? `<div class="analysis-claims">${claims.map((claim, index) => {
            const links = (Array.isArray(claim.evidence_ids) ? claim.evidence_ids : [])
              .map((id) => evidenceById.get(String(id)))
              .filter(Boolean)
              .map((item) => {
                const url = evidenceActionUrl(item);
                if (!url) return "";
                return `<a href="${url}" target="_blank" rel="noreferrer">${escapeHtml(evidenceDisplayTitle(item))}</a>`;
              })
              .filter(Boolean)
              .join("");
            return `
              <div class="analysis-claim">
                <div class="analysis-claim-header">
                  <div class="analysis-claim-index">${index + 1}</div>
                  <div class="analysis-claim-text">${escapeHtml(claim.text || "")}</div>
                </div>
                <div class="analysis-claim-evidence">
                  <span class="analysis-claim-evidence-label">Evidence</span>
                  ${links || '<span class="dim">No linked evidence.</span>'}
                </div>
              </div>
            `;
          }).join("")}</div>`
        : ""
      }
      ${pathItems.length
        ? `<div class="analysis-path">${pathItems.map((edge) => `
            <div class="analysis-path-item">${escapeHtml(edge.source_label || edge.source_id)} ${escapeHtml(edge.phrase || "is linked to")} ${escapeHtml(edge.target_label || edge.target_id)}</div>
          `).join("")}</div>`
        : ""
      }
    `;
  }

  async function openAnalysisView() {
    if (viewerState.analysisNodeIds.length !== 2) return;
    const [sourceId, targetId] = viewerState.analysisNodeIds;
    const sourceNode = nodeById.get(sourceId);
    const targetNode = nodeById.get(targetId);
    if (!sourceNode || !targetNode) return;

    scorePanelEl.innerHTML = `
      <div class="analysis-viewer">
        <div class="analysis-empty">Analyzing connection between <strong>${escapeHtml(sourceNode.label || sourceId)}</strong> and <strong>${escapeHtml(targetNode.label || targetId)}</strong>...</div>
      </div>
    `;
    setSidebarTab("ranked");
    toggleSidebar(true);

    const response = await fetch(graphFunctionUrl(ANALYZE_CONNECTION_URL), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_id: sourceId, target_id: targetId, graph: currentGraphKey }),
    });
    if (!response.ok) {
      throw new Error(`Connection analysis failed (${response.status})`);
    }
    const payload = await response.json();
    scorePanelEl.innerHTML = renderAnalysisHtml(payload);
  }

  function ensureAddressMap() {
    if (addressMap) return;
    addressMap = L.map("address-map", { zoomControl: true });
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(addressMap);
    addressMarkersLayer = L.layerGroup().addTo(addressMap);
  }

  async function ensureAddressCoordinatesLoaded() {
    if (addressCoordinatesLoaded) return true;
    if (addressCoordinatesLoadingPromise) return addressCoordinatesLoadingPromise;
    addressCoordinatesLoadingPromise = fetch(ADDRESS_COORDINATES_URL)
      .then((response) => {
        if (!response.ok) throw new Error(`Failed to load address coordinates (${response.status})`);
        return response.json();
      })
      .then((payload) => {
        const coordinates = Array.isArray(payload?.coordinates) ? payload.coordinates : [];
        addressCoordinateByNodeId = new Map(
          coordinates
            .filter((item) => item && item.node_id != null && Number.isFinite(Number(item.lat)) && Number.isFinite(Number(item.lon)))
            .map((item) => [
              String(item.node_id),
              {
                lat: Number(item.lat),
                lon: Number(item.lon),
                label: String(item.label || ""),
              },
            ]),
        );
        addressCoordinatesLoaded = true;
        return true;
      })
      .catch((error) => {
        console.error(error);
        addressCoordinateByNodeId = new Map();
        addressCoordinatesLoaded = false;
        return false;
      })
      .finally(() => {
        addressCoordinatesLoadingPromise = null;
      });
    return addressCoordinatesLoadingPromise;
  }

  function ensureAddressMarkers(nodes) {
    nodes.forEach((node) => {
      if (addressMarkerByNodeId.has(node.id)) return;
      const point = addressCoordinateByNodeId.get(node.id);
      if (!point) return;
      const marker = L.marker([point.lat, point.lon], { title: point.label || node.label || node.id });
      marker.bindPopup(`<strong>${escapeHtml(point.label || node.label || node.id)}</strong>`);
      addressMarkerByNodeId.set(node.id, marker);
    });
  }

  function mapAddressNodes() {
    const nodeIds = new Set(visibleNodes.filter((node) => node.kind === "address").map((node) => node.id));
    visibleNodes.forEach((node) => {
      if (node.kind !== "organisation") return;
      (orgAddressIds.get(node.id) || new Set()).forEach((addressId) => nodeIds.add(addressId));
    });
    return [...nodeIds].map((nodeId) => nodeById.get(nodeId)).filter((node) => node?.kind === "address");
  }

  function syncVisibleAddressMarkers() {
    const visibleAddressIds = new Set(mapAddressNodes().map((node) => node.id));
    addressMarkersLayer.clearLayers();
    visibleAddressIds.forEach((nodeId) => {
      const marker = addressMarkerByNodeId.get(nodeId);
      if (marker) marker.addTo(addressMarkersLayer);
    });
    const markers = [...visibleAddressIds].map((nodeId) => addressMarkerByNodeId.get(nodeId)).filter(Boolean);
    if (!markers.length) {
      addressMap.setView([20, 0], 2);
      return;
    }
    addressMap.invalidateSize();
    const bounds = L.latLngBounds(markers.map((marker) => marker.getLatLng()));
    addressMap.fitBounds(bounds.pad(0.2));
  }

  async function openMapView() {
    ensureAddressMap();
    setSidebarTab("map");
    const ok = await ensureAddressCoordinatesLoaded();
    if (!ok) {
      addressMarkersLayer.clearLayers();
      addressMap.setView([20, 0], 2);
      return;
    }
    const addressNodes = mapAddressNodes();
    ensureAddressMarkers(addressNodes);
    syncVisibleAddressMarkers();
  }

  async function applyViewerState(options = {}) {
    if (!options?.preserveExpandedHiddenConnections) {
      viewerState.expandedHiddenConnections = [];
    }
    syncHiddenTypeState();
    rebuildActiveGraph();
    sanitizeSelectionState();
    const projection = projectVisibleGraph();
    const scene = buildCombinedScene(projection);
    const visibleIds = new Set(scene.nodes.map((node) => node.id));
    allNodes.forEach((node) => {
      node._visible = visibleIds.has(node.id);
    });
    visibleNodes = scene.nodes;
    visibleEdges = scene.edges;
    renderer.setGraph({
      nodes: visibleNodes,
      edges: visibleEdges,
      rootIds: scene.rootIds,
    });
    renderExtraTreeSummary();
    if (!options?.preserveViewport) {
      renderer.fitToNodes(visibleNodes);
    }
    renderScorePanel();

    if (document.querySelector('.sidebar-pane[data-pane="map"]')?.classList.contains("active") && addressMap) {
      openMapView().catch(() => {});
    }

    const extraSuffix = viewerState.extraRootIds.length ? ` + ${viewerState.extraRootIds.length} added tree${viewerState.extraRootIds.length === 1 ? "" : "s"}` : "";
    statsEl.textContent = `showing ${visibleNodes.length} nodes, ${visibleEdges.length} edges${extraSuffix}`;
  }

  function bindUiEvents() {
    modeViewerButton?.addEventListener("click", () => setAppMode("viewer"));
    modeBuilderButton?.addEventListener("click", () => setAppMode("builder"));
    builderFormEl?.addEventListener("submit", (event) => {
      event.preventDefault();
      submitBuilderJob().catch((error) => setBuilderStatus(error.message || "Graph build failed to start.", true));
    });
    [builderGraphTitleInput, builderGraphIdInput, builderSeedNameInput, builderSeedNamesInput].forEach((input) => {
      input?.addEventListener("input", updateBuilderVersionInput);
    });
    builderSaveModeInput?.addEventListener("change", () => {
      if (builderGraphVersionInput) builderGraphVersionInput.value = "";
      updateBuilderVersionInput();
    });
    builderRefreshGraphsButton?.addEventListener("click", () => {
      loadGeneratedGraphOptions().catch((error) => setBuilderStatus(error.message || "Generated graph refresh failed.", true));
    });
    builderGraphListEl?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-graph-action]");
      if (!button) return;
      handleGeneratedGraphAction(button).catch((error) => setBuilderStatus(error.message || "Generated graph action failed.", true));
    });
    searchInput.addEventListener("input", () => {
      viewerState.searchQuery = searchInput.value.trim();
      if (viewerState.searchQuery) viewerState.focusedNodeIds.clear();
      applyViewerState();
    });
    searchInput.addEventListener("search", () => {
      viewerState.searchQuery = searchInput.value.trim();
      if (!viewerState.searchQuery) viewerState.focusedNodeIds.clear();
      applyViewerState();
    });
    compareClearButton.addEventListener("click", () => {
      clearExtraRoots();
      applyViewerState();
    });
    canvasSearchInput.addEventListener("input", () => {
      renderCanvasSearchResults();
    });
    canvasSearchResultsEl.addEventListener("click", (event) => {
      const button = event.target.closest(".canvas-search-result");
      if (!button) return;
      addTreeFromCanvasSearch(String(button.dataset.nodeId || ""));
    });
    canvasSearchInput.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        hideCanvasSearchPopover();
        closeContextMenu();
      }
      if (event.key === "Enter") {
        event.preventDefault();
        const firstResult = canvasSearchCandidates(canvasSearchInput.value)[0];
        if (firstResult) addTreeFromCanvasSearch(firstResult.id);
      }
    });
    [showIdentitiesInput, showCompaniesInput, showCharitiesInput, showPeopleInput, showAddressesInput, indirectOnlyInput, sanctionedOnlyInput, negativeNewsOnlyInput]
      .forEach((input) => input.addEventListener("change", applyViewerState));
    showLowConfidenceInput.addEventListener("change", async () => {
      if (showLowConfidenceInput.checked) {
        const ok = await ensureLowConfidenceLoaded();
        if (!ok) showLowConfidenceInput.checked = false;
      }
      applyViewerState();
    });
    showLowConfidenceNodesInput.addEventListener("change", async () => {
      if (showLowConfidenceNodesInput.checked) {
        const ok = await ensureLowConfidenceOrgLoaded();
        if (!ok) showLowConfidenceNodesInput.checked = false;
      }
      applyViewerState();
    });
    toggleSidebarButton.addEventListener("click", () => toggleSidebar());
    detailsModalCloseEl.addEventListener("click", closeDetailsModal);
    detailsModalEl.addEventListener("click", (event) => {
      if (event.target === detailsModalEl) closeDetailsModal();
    });
    scorePanelEl.addEventListener("click", (event) => {
      const button = event.target.closest("[data-ranked-type]");
      if (!button) return;
      const nextCategory = String(button.dataset.rankedType || "");
      if (!["people", "orgs", "addresses"].includes(nextCategory)) return;
      if (viewerState.rankedCategory === nextCategory) return;
      viewerState.rankedCategory = nextCategory;
      renderScorePanel();
    });
    sidebarTabEls.forEach((element) => {
      element.addEventListener("click", () => {
        const tabName = String(element.dataset.tab || "legend");
        setSidebarTab(tabName);
        if (tabName === "map") {
          openMapView().catch(() => {});
        }
      });
    });
    contextMenuEl.addEventListener("click", async (event) => {
      const button = event.target.closest(".context-menu-item");
      if (!button) return;
      const action = (contextMenuEl._actions || [])[Number(button.dataset.actionIndex || -1)];
      closeContextMenu();
      if (!action) return;
      if (action.type === "hidden_connection_expand") {
        if (setExpandedHiddenConnection(action.edge)) {
          applyViewerState({ preserveExpandedHiddenConnections: true });
        }
      } else
      if (action.type === "open_url" && action.url) {
        window.open(action.url, "_blank", "noopener,noreferrer");
      } else if (action.type === "node_claims") {
        const node = nodeById.get(action.nodeId);
        if (node) openNodeAttributionView(node);
      } else if (action.type === "low_confidence_expand" || action.type === "low_confidence_collapse") {
        viewerState.searchQuery = "";
        searchInput.value = "";
        setSingleFocus(action.nodeId);
        setLowConfidenceNodeExpanded(action.nodeId, action.type === "low_confidence_expand");
        applyViewerState();
      } else if (action.type === "merge_start") {
        viewerState.pendingMergeNodeId = action.nodeId;
      } else if (action.type === "merge_cancel") {
        viewerState.pendingMergeNodeId = "";
      } else if (action.type === "merge_persist") {
        const leaderKey = promptForMergeLeader(action);
        if (!leaderKey) return;
        const leaderLabel = leaderKey === action.sourceKey ? action.sourceLabel : action.targetLabel;
        const confirmed = window.confirm(`Merge "${action.sourceLabel}" into "${action.targetLabel}" and display "${leaderLabel}"? This will persist across graph rebuilds.`);
        if (!confirmed) return;
        try {
          await persistMergeOverride({ ...action, operation: "add", leaderKey });
        } catch (error) {
          console.error(error);
          window.alert("Persisted merge failed.");
        }
      } else if (action.type === "merge_remove") {
        const confirmed = window.confirm(`Undo the merge of "${action.sourceLabel}" into "${action.targetLabel}"?`);
        if (!confirmed) return;
        try {
          await persistMergeOverride({ ...action, operation: "remove" });
        } catch (error) {
          console.error(error);
          window.alert("Undo merge failed.");
        }
      } else if (action.type === "hide_add") {
        const confirmed = window.confirm(`Hide "${action.nodeLabel}" across graph rebuilds?`);
        if (!confirmed) return;
        try {
          await persistHiddenOverride({ ...action, operation: "add" });
        } catch (error) {
          console.error(error);
          window.alert("Hide node failed.");
        }
      } else if (action.type === "hide_remove") {
        const confirmed = window.confirm(`Restore "${action.nodeLabel}"?`);
        if (!confirmed) return;
        try {
          await persistHiddenOverride({ ...action, operation: "remove" });
        } catch (error) {
          console.error(error);
          window.alert("Restore node failed.");
        }
      } else if (action.type === "hide_restore_all") {
        const rows = Array.isArray(action.rows) ? action.rows : [];
        if (!rows.length) return;
        const confirmed = window.confirm(`Restore ${rows.length} hidden nodes?`);
        if (!confirmed) return;
        try {
          for (const row of rows) {
            await persistHiddenOverride(
              {
                operation: "remove",
                nodeKey: row.nodeId,
                nodeLabel: row.label || row.nodeId,
              },
              { refresh: false },
            );
          }
          rebuildBaseGraph();
          await applyViewerState();
        } catch (error) {
          console.error(error);
          window.alert("Restore hidden nodes failed.");
        }
      } else if (action.type === "canvas_add_prompt") {
        closeContextMenu();
        showCanvasSearchPopover(canvasSearchAnchor.x, canvasSearchAnchor.y);
      } else if (action.type === "canvas_remove_tree") {
        removeExtraRoot(action.nodeId);
        applyViewerState();
      } else if (action.type === "canvas_clear_trees") {
        clearExtraRoots();
        applyViewerState();
      } else if (action.type === "focus_clear") {
        viewerState.focusedNodeIds.clear();
        applyViewerState();
      } else if (action.type === "analysis_add") {
        viewerState.analysisNodeIds = [...viewerState.analysisNodeIds, action.nodeId].slice(0, 2);
      } else if (action.type === "analysis_remove") {
        viewerState.analysisNodeIds = viewerState.analysisNodeIds.filter((id) => id !== action.nodeId);
      } else if (action.type === "analysis_run") {
        openAnalysisView().catch(() => {
          window.alert("Connection analysis failed.");
        });
      } else if (action.type === "analysis_clear") {
        viewerState.analysisNodeIds = [];
      }
    });
    document.addEventListener("pointerdown", (event) => {
      if (event.button !== 0) return;
      if (event.target.closest("#context-menu")) return;
      if (event.target.closest("#canvas-search-popover")) return;
      closeContextMenu();
      hideCanvasSearchPopover();
    }, true);
    window.addEventListener("blur", () => {
      closeContextMenu();
      hideCanvasSearchPopover();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && detailsModalEl.classList.contains("open")) {
        closeDetailsModal();
        return;
      }
      if (event.key === "Escape") {
        closeContextMenu();
        hideCanvasSearchPopover();
      }
    });
  }

  async function boot() {
    renderLegend();
    initGraphSwitcher();
    await ensureMergeOverridesLoaded();
    renderer = window.IstariWebGLRenderer.createGraphRenderer(container, {
      onHover(node, event, hit) {
        if (!node) {
          hideTooltip();
          return;
        }
        if (hit?.zone === "focus") {
          showTooltip(event, [`Search for ${escapeHtml(node.label || "")}`]);
          return;
        }
        showTooltip(event, tooltipLinesForNode(node));
      },
      onEdgeHover(edge, event) {
        if (!edge) return;
        showTooltip(event, tooltipLinesForEdge(edge));
      },
      onContextMenu(node, event) {
        openContextMenu(node, event);
      },
      onEdgeContextMenu(edge, event) {
        openEdgeContextMenu(edge, event);
      },
      onBackgroundContextMenu(event) {
        openCanvasContextMenu(event);
      },
      onClick(node) {
        if (!node) return;
        setSingleFocus(node.id);
        viewerState.searchQuery = "";
        searchInput.value = "";
        applyViewerState();
      },
      onFocusButton(node) {
        if (!node) return;
        searchInput.value = node.label || "";
        viewerState.searchQuery = (node.label || "").trim();
        viewerState.focusedNodeIds.clear();
        applyViewerState();
      },
      onDrag() {
        hideTooltip();
        closeContextMenu();
      },
      onBackgroundDoubleClick() {
        if (!viewerState.focusedNodeIds.size) return;
        viewerState.focusedNodeIds.clear();
        applyViewerState();
      },
    });
    await renderer.init();
    bindUiEvents();
    await applyViewerState();
  }

  boot().catch((error) => {
    console.error(error);
    scorePanelEl.innerHTML = '<div class="analysis-error">Graph viewer failed to initialize.</div>';
  });
}());
