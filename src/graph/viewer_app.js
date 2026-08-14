(function () {
  const graphDataElement = document.getElementById("graph-data");
  const graphData = JSON.parse(graphDataElement.textContent);
  graphDataElement.remove();
  const rawMainNodes = Array.isArray(graphData.nodes) ? graphData.nodes : [];
  const rawMainEdges = (Array.isArray(graphData.edges) ? graphData.edges : [])
    .filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed");
  const latestEnrichment = graphData.enrichment && typeof graphData.enrichment === "object"
    ? graphData.enrichment
    : {};
  const latestEnrichmentCentreIds = new Set(
    (Array.isArray(latestEnrichment.central_node_ids) ? latestEnrichment.central_node_ids : []).map(String),
  );
  const latestEnrichmentAddedNodeIds = new Set(
    (Array.isArray(latestEnrichment.added_node_ids) ? latestEnrichment.added_node_ids : []).map(String),
  );
  const LOW_CONFIDENCE_DATA_URL = "graph-data-open-letters.json";
  const LOW_CONFIDENCE_NODES_DATA_URL = "graph-data-low-confidence-nodes.json";
  const GRAPH_OPTIONS = [
    { key: "mb", label: "MB", path: "/mb/" },
    { key: "94-park-ave", label: "94 park ave", path: "/94-park-ave/" },
    { key: "iums", label: "IUMS", path: "/iums/" },
    { key: "iran", label: "Iran", path: "/iran/" },
    { key: "sevenspikes", label: "Seven Spikes", path: "/sevenspikes/" },
    { key: "expanded-mb-names", label: "Expanded MB Names", path: "/expanded-mb-names/" },
  ];
  const GRAPH_QUESTION_URL = "/.netlify/functions/graph-question";
  const EVIDENCE_FILE_URL = "/.netlify/functions/evidence-file";
  const MERGE_OVERRIDES_URL = "/.netlify/functions/merge-overrides";
  const LEAFLET_CSS_URL = "https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css";
  const LEAFLET_SCRIPT_URL = "https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js";

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
  const caseQueryInput = document.getElementById("case-query");
  const casePlanSubmitButton = document.getElementById("case-plan-submit");
  const builderFeedbackEl = document.getElementById("builder-feedback");
  const caseDirectButton = document.getElementById("case-direct");
  const casePlanEl = document.getElementById("case-plan");
  const casePlanTitleEl = document.getElementById("case-plan-title");
  const casePlanIdEl = document.getElementById("case-plan-id");
  const casePlanInputsEl = document.getElementById("case-plan-inputs");
  const caseAddInputButton = document.getElementById("case-add-input");
  const caseExpansionInput = document.getElementById("case-expansion");
  const caseEntitiesInput = document.getElementById("case-entities");
  const caseMaxAddressesInput = document.getElementById("case-max-addresses");
  const caseNearbyEnabledInput = document.getElementById("case-nearby-enabled");
  const caseNearbyConfigEl = document.getElementById("case-nearby-config");
  const caseNearbyCentreInput = document.getElementById("case-nearby-centre");
  const caseNearbyRadiusInput = document.getElementById("case-nearby-radius");
  const caseNearbyRadiusValueEl = document.getElementById("case-nearby-radius-value");
  const caseNearbySummaryEl = document.getElementById("case-nearby-summary");
  const caseExpandPeopleInput = document.getElementById("case-expand-people");
  const caseEnrichDocumentsInput = document.getElementById("case-enrich-documents");
  const caseIncludeFormerInput = document.getElementById("case-include-former");
  const caseRunButton = document.getElementById("case-run");
  const caseResetButton = document.getElementById("case-reset");
  const caseOpenResultEl = document.getElementById("case-open-result");
  const builderStatusEl = document.getElementById("builder-status");
  const caseProgressEl = document.getElementById("case-progress");
  const caseProgressOpenEl = document.getElementById("case-progress-open");
  const caseProgressLabelEl = document.getElementById("case-progress-label");
  const caseProgressPercentEl = document.getElementById("case-progress-percent");
  const caseProgressPhaseEl = document.getElementById("case-progress-phase");
  const caseProgressBarEl = document.getElementById("case-progress-bar");
  const caseProgressDetailEl = document.getElementById("case-progress-detail");
  const caseProgressFeedbackEl = document.getElementById("case-progress-feedback");
  const caseCancelButton = document.getElementById("case-cancel");
  const caseTaskListEl = document.getElementById("case-task-list");
  const caseTasksCountEl = document.getElementById("case-tasks-count");
  const runLogBackdropEl = document.getElementById("run-log-backdrop");
  const runLogSheetEl = document.getElementById("run-log-sheet");
  const runLogCloseButton = document.getElementById("run-log-close");
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
  const resolutionPanelEl = document.getElementById("resolution-panel");
  const questionSelectionEl = document.getElementById("question-selection");
  const questionInputEl = document.getElementById("question-input");
  const questionSubmitEl = document.getElementById("question-submit");
  const questionClearEl = document.getElementById("question-clear");
  const questionResultEl = document.getElementById("question-result");
  const graphExpansionStatusEl = document.getElementById("graph-expansion-status");
  const graphExpansionLabelEl = document.getElementById("graph-expansion-label");
  const graphExpansionProgressEl = document.getElementById("graph-expansion-progress");
  const indirectOnlyInput = document.getElementById("indirect-only");
  const sanctionedOnlyInput = document.getElementById("sanctioned-only");
  const negativeNewsOnlyInput = document.getElementById("negative-news-only");
  const focalDistanceFilterEl = document.getElementById("focal-distance-filter");
  const focalDistanceRangeEl = document.getElementById("focal-distance-range");
  const focalDistanceValueEl = document.getElementById("focal-distance-value");
  const detailsModalEl = document.getElementById("details-modal");
  const detailsModalTitleEl = document.getElementById("details-modal-title");
  const detailsModalStatusEl = document.getElementById("details-modal-status");
  const detailsModalBodyEl = document.getElementById("details-modal-body");
  const detailsModalCloseEl = document.getElementById("details-modal-close");
  const ADDRESS_COORDINATES_URL = "address-coordinates.json";
  const currentGeneratedGraphId = detectGeneratedGraphId(window.location.pathname);
  const currentGeneratedGraphVersion = detectGeneratedGraphVersion(window.location.pathname);
  const currentGraphKey = currentGeneratedGraphId || detectGraphKey(window.location.pathname);
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
  let leafletLoadingPromise = null;
  let mergeOverrides = { address: [], name: [], organisation: [], seed: [], hidden: [], rejected: [], audit: [] };
  let mergeOverridesLoadingPromise = null;
  let resolutionCandidatesCache = null;
  let canvasSearchAnchor = { x: 0, y: 0 };
  let currentCaseJobId = "";
  let currentCaseJobStatus = "";
  let currentCasePlan = null;
  let recentCaseJobs = [];
  const watchedCaseJobs = new Set();
  let caseNearbyMap = null;
  let caseNearbyLayer = null;
  let caseNearbyCircle = null;
  let caseNearbyPreviewTimer = null;
  let caseNearbyPreviewRequest = 0;
  let casePlannerRequest = 0;
  let currentEnrichmentJobId = "";
  let currentEnrichmentStatus = "";
  let enrichmentNavigationStarted = false;
  const rawMainNodeIds = new Set(rawMainNodes.map((node) => node.id));

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
    maxFocalDistanceMetres: null,
    showLatestEnrichmentRound: latestEnrichmentAddedNodeIds.size > 0,
    questionNodeIds: [],
    pendingMergeNodeId: "",
    expandedLowConfidenceNodeIds: new Set(),
    rankedCategory: "people",
  };

  const measureCtx = document.createElement("canvas").getContext("2d");
  let tooltipWidth = 0;
  let tooltipHeight = 0;

  function detectGraphKey(pathname) {
    const path = String(pathname || "").toLowerCase();
    const option = GRAPH_OPTIONS.find(({ path: optionPath }) => (
      path === optionPath || path === optionPath.slice(0, -1) || path.startsWith(optionPath)
    ));
    return option?.key || GRAPH_OPTIONS[0].key;
  }

  function detectGeneratedGraphId(pathname) {
    const match = String(pathname || "").match(/^\/generated-graphs\/([^/]+)(?:\/|$)/i);
    if (!match) return "";
    try {
      return decodeURIComponent(match[1]);
    } catch (_error) {
      return match[1];
    }
  }

  function detectGeneratedGraphVersion(pathname) {
    const match = String(pathname || "").match(/\/generated-graphs\/[^/]+\/versions\/v(\d+)(?:\/|$)/i);
    return match ? Number(match[1]) : null;
  }

  function graphFunctionUrl(baseUrl) {
    const url = new URL(baseUrl, window.location.origin);
    url.searchParams.set("graph", currentGraphKey);
    return url.toString();
  }

  function builderApiUrl(path) {
    return `${BUILDER_API_BASE}${path}`;
  }

  function setBuilderStatus(message, isError = false) {
    if (!builderStatusEl) return;
    if (!message) {
      builderStatusEl.textContent = "";
      builderStatusEl.classList.remove("error");
      return;
    }
    const timestamp = new Date().toLocaleTimeString([], { hour12: false });
    const line = `[${timestamp}] ${message}`;
    const existing = builderStatusEl.textContent.trimEnd();
    if (!existing.endsWith(line)) {
      builderStatusEl.textContent = existing ? `${existing}\n${line}` : line;
    }
    builderStatusEl.classList.toggle("error", !!isError);
    builderStatusEl.scrollTop = builderStatusEl.scrollHeight;
  }

  function setBuilderFeedback(message = "", state = "") {
    if (!builderFeedbackEl) return;
    builderFeedbackEl.textContent = message;
    builderFeedbackEl.dataset.state = state;
    builderFeedbackEl.classList.toggle("hidden", !message);
  }

  function renderBuilderStdout(job = {}) {
    if (!builderStatusEl) return;
    if (job.id && currentCaseJobId && job.id !== currentCaseJobId) {
      upsertRecentTask(job);
      return;
    }
    currentCaseJobStatus = String(job.status || currentCaseJobStatus);
    const lines = (Array.isArray(job.stdout) ? job.stdout : []).map((entry) => {
      const timestamp = new Date(entry.created_at || Date.now()).toLocaleTimeString([], { hour12: false });
      return `[${timestamp}] ${String(entry.message || "")}`;
    });
    if (job.error && !lines.some((line) => line.includes(job.error))) {
      lines.push(`[error] ${job.error}`);
    }
    builderStatusEl.textContent = lines.join("\n");
    builderStatusEl.classList.toggle("error", job.status === "failed");
    builderStatusEl.scrollTop = builderStatusEl.scrollHeight;
    renderBuilderProgress(job);
    if (job.id === currentEnrichmentJobId) renderNodeExpansionTask(job);
    upsertRecentTask(job);
  }

  function renderBuilderProgress(job = {}) {
    const status = String(job.status || "");
    const visible = ["planned", "queued", "planning", "running", "completed", "failed", "cancelled"].includes(status)
      && job.stage !== "planning_failed";
    caseProgressEl?.classList.toggle("hidden", !visible);
    if (!visible) return;
    const progress = job.progress || {};
    const activity = job.activity || {};
    const percent = status === "completed" ? 100 : Math.max(0, Math.min(100, Number(progress.percent) || 0));
    const current = Array.isArray(activity.current) ? activity.current : [];
    const active = Number(progress.active) || current.length;
    const skipped = Number(activity.skipped) || 0;
    const retrying = Number(activity.retrying) || 0;
    caseProgressEl.dataset.status = status;
    caseProgressBarEl.value = percent;
    caseProgressBarEl.textContent = `${percent}%`;
    caseProgressPercentEl.textContent = `${percent}%`;
    caseProgressLabelEl.textContent = taskStatusLabel(status);
    caseProgressPhaseEl.textContent = taskPhaseLabel(status, current);
    caseProgressDetailEl.textContent = taskProgressDetail(status, progress, active);
    const feedback = taskFeedback(status, job.error, skipped, retrying);
    caseProgressFeedbackEl.textContent = feedback;
    caseProgressFeedbackEl.classList.toggle("hidden", !feedback);
    caseProgressFeedbackEl.classList.toggle("warning", Boolean(skipped || retrying) && status !== "failed");
    caseProgressFeedbackEl.classList.toggle("error", status === "failed");
    caseCancelButton?.classList.toggle("hidden", !["queued", "planning", "running"].includes(status));
  }

  function taskStatusLabel(status) {
    if (status === "planned") return "Ready to run";
    if (["queued", "planning"].includes(status)) return "Waiting for worker";
    if (status === "running") return "Discovering records";
    if (status === "completed") return "Graph ready";
    if (status === "failed") return "Needs attention";
    if (status === "cancelled") return "Cancelled";
    return "Investigation";
  }

  function taskPhaseLabel(status, current = []) {
    if (status === "planned") return "Review the scope before discovery starts";
    if (["queued", "planning"].includes(status)) return "Queued; discovery starts automatically";
    if (status === "completed") return "Discovery and graph creation completed";
    if (status === "failed") return "Discovery stopped before the graph was completed";
    if (status === "cancelled") return "No further registry work will run";
    if (!current.length) return "Preparing the next registry checks";
    const labels = [...new Set(current.map(taskOperationLabel))];
    return labels.length > 1 ? `${labels[0]} and ${labels.length - 1} more` : labels[0];
  }

  function taskOperationLabel(kind) {
    const labels = {
      resolve_seed: "Resolving the starting point",
      search_seed: "Searching registry candidates",
      adjudicate_seed: "Checking ambiguous matches",
      person_appointments: "Finding appointments",
      company_profile: "Reading company records",
      company_officers: "Finding company officers",
      charity_details: "Reading charity records",
      charity_trustees: "Finding charity trustees",
      address_companies: "Finding companies at addresses",
      address_charities: "Finding charities at addresses",
      address_charity_details: "Checking charity addresses",
      nearby_addresses: "Finding nearby registered addresses",
      organisation_documents: "Finding recent filings and accounts",
      document_extract: "Reading document relationships",
      person_cleanup_scan: "Finding duplicate people",
      person_cleanup_group: "Resolving duplicate people",
    };
    return labels[String(kind)] || "Checking registry records";
  }

  function taskProgressDetail(status, progress, active) {
    const nodes = Number(progress.nodes) || 0;
    const edges = Number(progress.edges) || 0;
    if (status === "planned") return "No registry work has started";
    if (status === "completed") return `${nodes} nodes / ${edges} relationships / ${Number(progress.processed) || 0} checks complete`;
    if (status === "cancelled") return `${nodes} nodes / ${edges} relationships retained in the cancelled task`;
    const work = [
      `${Number(progress.processed) || 0} complete`,
      `${active} active`,
      `${Number(progress.queued) || 0} waiting`,
    ];
    if (Number(progress.failed)) work.push(`${Number(progress.failed)} failed`);
    return `${work.join(" / ")}  |  ${nodes} nodes / ${edges} relationships`;
  }

  function taskFeedback(status, error, skipped, retrying) {
    if (status === "failed") return friendlyTaskError(error);
    const feedback = [];
    if (retrying) feedback.push(`${retrying} registry request${retrying === 1 ? " is" : "s are"} waiting to retry.`);
    if (skipped) feedback.push(`${skipped} source check${skipped === 1 ? " could" : "s could"} not be completed. The graph contains the verified records; open the run log for details.`);
    return feedback.join(" ");
  }

  function friendlyTaskError(error) {
    const message = String(error || "").trim();
    if (/HTTP 429/i.test(message)) return "A registry rate limit stopped this run. Retry the task after a short wait.";
    if (/timeout|timed out|abort/i.test(message)) return "A registry request timed out repeatedly. Retry the task; completed work is retained until the retry starts.";
    if (/HTTP 404/i.test(message)) return "A registry record is no longer available. Check the starting seed, then retry; new runs skip missing related records.";
    return message ? `Discovery stopped: ${message}` : "Discovery stopped before completion. Open the log for details, then retry.";
  }

  function graphRefreshStateKey() {
    return currentGeneratedGraphId ? `istari:graph-refresh:${currentGeneratedGraphId}` : "";
  }

  function expansionResolutionFeedback(job = {}) {
    const messages = (Array.isArray(job.stdout) ? job.stdout : []).map((entry) => String(entry.message || ""));
    const merged = messages.reduce((total, message) => {
      const match = message.match(/cleanup: merged (\d+) duplicate person record/i);
      return total + Number(match?.[1] || 0);
    }, 0);
    const groupMessage = messages.find((message) => /cleanup: found \d+ possible duplicate person group/i.test(message));
    const groups = Number(groupMessage?.match(/found (\d+)/i)?.[1] || 0);
    if (merged) return `Resolution merged ${merged} duplicate person record${merged === 1 ? "" : "s"}.`;
    if (groups) return `Resolution reviewed ${groups} possible duplicate name group${groups === 1 ? "" : "s"}; no merge was supported.`;
    return "Resolution checked the expanded names; no duplicate match was supported.";
  }

  function saveGraphRefreshState(job = {}) {
    const key = graphRefreshStateKey();
    const view = renderer?.getViewState?.();
    if (!key || !view) return;
    try {
      sessionStorage.setItem(key, JSON.stringify({
        savedAt: Date.now(),
        view,
        feedback: expansionResolutionFeedback(job),
      }));
    } catch (_error) {
      // Navigation can continue when browser storage is unavailable.
    }
  }

  function restoreGraphRefreshState() {
    const key = graphRefreshStateKey();
    if (!key) return;
    let stored = null;
    try {
      stored = JSON.parse(sessionStorage.getItem(key) || "null");
      sessionStorage.removeItem(key);
    } catch (_error) {
      return;
    }
    if (!stored?.view || (Date.now() - Number(stored.savedAt || 0)) > 300000) return;
    renderer?.restoreViewState?.(stored.view);
    if (stored.feedback && graphExpansionStatusEl) {
      graphExpansionStatusEl.classList.remove("hidden");
      graphExpansionStatusEl.dataset.state = "complete";
      graphExpansionProgressEl.value = 100;
      graphExpansionProgressEl.textContent = "100%";
      graphExpansionLabelEl.textContent = `Expansion complete. ${stored.feedback}`;
      window.setTimeout(() => graphExpansionStatusEl.classList.add("hidden"), 8000);
    }
  }

  function renderNodeExpansionTask(job = {}) {
    if (!graphExpansionStatusEl) return;
    const status = String(job.status || "");
    currentEnrichmentStatus = status;
    const progress = job.progress || {};
    const currentOperations = Array.isArray(job.activity?.current) ? job.activity.current : [];
    const percent = status === "completed" ? 100 : Math.max(0, Math.min(100, Number(progress.percent) || 0));
    graphExpansionStatusEl.classList.remove("hidden");
    graphExpansionStatusEl.dataset.state = status === "failed" ? "error" : "working";
    graphExpansionProgressEl.value = percent;
    graphExpansionProgressEl.textContent = `${percent}%`;
    graphExpansionLabelEl.textContent = status === "failed"
      ? friendlyTaskError(job.error)
      : status === "completed"
        ? "Expansion complete. Updating graph."
        : `${currentOperations.length ? taskOperationLabel(currentOperations[0]) : taskStatusLabel(status)}: ${percent}% / ${Number(progress.processed) || 0} checks complete.`;
    const path = String(job.result?.artifact?.path || "");
    if (status === "completed" && path && !enrichmentNavigationStarted) {
      enrichmentNavigationStarted = true;
      saveGraphRefreshState(job);
      window.setTimeout(() => window.location.assign(new URL(path, window.location.origin).toString()), 150);
    }
  }

  function enrichmentNodeEligible(node) {
    if (!currentGeneratedGraphId || !node || !rawMainNodeIds.has(node.id)) return false;
    if (["address", "person", "seed", "seed_alias"].includes(node.kind)) return true;
    const registryType = String(node.registry_type || "").toLowerCase();
    return node.kind === "organisation" && ["company", "charity"].includes(registryType);
  }

  function latestEnrichmentAddedCount() {
    return baseNodes.filter((node) => latestEnrichmentAddedNodeIds.has(String(node.id))).length;
  }

  function hiddenLatestEnrichmentNodeIds() {
    if (viewerState.showLatestEnrichmentRound) return new Set();
    return new Set(
      baseNodes
        .filter((node) => latestEnrichmentAddedNodeIds.has(String(node.id)))
        .map((node) => node.id),
    );
  }

  function enrichmentRoundActionForNode(node) {
    if (!enrichmentNodeEligible(node)) return null;
    const addedCount = latestEnrichmentAddedCount();
    if (addedCount && latestEnrichmentCentreIds.has(String(node.id))) {
      return {
        label: viewerState.showLatestEnrichmentRound
          ? "Hide expanded round"
          : `Show expanded round (${addedCount.toLocaleString()})`,
        type: viewerState.showLatestEnrichmentRound ? "enrichment_round_hide" : "enrichment_round_show",
        nodeId: node.id,
      };
    }
    return { label: `Expand this ${nodeTypeLabel(node)}`, type: "enrichment_round_run", nodeId: node.id };
  }

  async function startGraphEnrichment(request, statusMessage = "Creating direct expansion task.") {
    if (["creating", "planned", "queued", "running"].includes(currentEnrichmentStatus)) {
      throw new Error("A node expansion is already running.");
    }
    currentEnrichmentStatus = "creating";
    enrichmentNavigationStarted = false;
    graphExpansionStatusEl.classList.remove("hidden");
    graphExpansionStatusEl.dataset.state = "working";
    graphExpansionProgressEl.value = 0;
    graphExpansionLabelEl.textContent = statusMessage;
    const created = await postBuilderJson(`/api/generated-graphs/${encodeURIComponent(currentGeneratedGraphId)}/enrich`, request);
    const job = created.job || {};
    currentEnrichmentJobId = String(job.id || "");
    currentEnrichmentStatus = String(job.status || "planned");
    currentCaseJobId = currentEnrichmentJobId;
    currentCaseJobStatus = String(job.status || "planned");
    if (!currentEnrichmentJobId) throw new Error("The backend did not create an enrichment task.");
    renderBuilderStdout(job);
    const started = await postBuilderJson(`/api/investigations/${encodeURIComponent(currentEnrichmentJobId)}/start`, {});
    renderBuilderStdout(started.job || {});
    await watchBuilderJob(currentEnrichmentJobId);
  }

  async function runOneEnrichmentRound(nodeId) {
    if (!currentGeneratedGraphId) throw new Error("Only generated graphs can be expanded.");
    const node = rawMainNodes.find((candidate) => String(candidate.id) === String(nodeId));
    if (!enrichmentNodeEligible(node)) throw new Error("This node cannot be used as a discovery seed.");
    if (["creating", "planned", "queued", "running"].includes(currentEnrichmentStatus)) {
      throw new Error("An enrichment task is already running.");
    }
    await startGraphEnrichment({
      sourceVersion: currentGeneratedGraphVersion || undefined,
      centralNodeIds: [node.id],
      scopeNodeIds: [],
      expandRelationships: true,
      expansionCycles: 0,
      expandPeople: true,
      enrichMissingDocuments: false,
      entityCeiling: 5000,
      includeFormer: true,
    }, `Expanding direct relationships for ${node.label || node.id}.`);
  }

  function upsertRecentTask(job) {
    if (!job?.id) return;
    recentCaseJobs = [job, ...recentCaseJobs.filter((item) => item.id !== job.id)]
      .sort((left, right) => new Date(right.created_at || 0) - new Date(left.created_at || 0))
      .slice(0, 10);
    renderRecentTasks();
  }

  function renderRecentTasks() {
    if (!caseTaskListEl) return;
    caseTasksCountEl.textContent = recentCaseJobs.length ? String(recentCaseJobs.length) : "";
    if (!recentCaseJobs.length) {
      caseTaskListEl.innerHTML = '<div class="case-task-empty">No investigations yet.</div>';
      return;
    }
    caseTaskListEl.innerHTML = recentCaseJobs.map((job) => {
      const status = String(job.status || "planned");
      const draft = job.draft || job.plan || {};
      const title = String(draft.title || "Untitled investigation");
      const progress = job.progress || {};
      const seedCount = Array.isArray(draft.seeds) ? draft.seeds.length : 0;
      const resultPath = String(job.result?.artifact?.path || "");
      const clearable = ["planned", "completed", "failed", "cancelled"].includes(status);
      const selected = job.id === currentCaseJobId;
      const detail = status === "completed"
        ? `${Number(progress.nodes) || 0} nodes / ${Number(progress.edges) || 0} relationships`
        : `${seedCount} seed${seedCount === 1 ? "" : "s"} / ${taskListWorkLabel(status, progress)}`;
      return `
        <article class="case-task-row" data-status="${escapeHtml(status)}" data-selected="${selected}">
          <button class="case-task-select" type="button" data-task-id="${escapeHtml(job.id)}" aria-label="View task ${escapeHtml(title)}">
            <span class="case-task-state" aria-hidden="true"></span>
            <span class="case-task-copy">
              <strong>${escapeHtml(title)}</strong>
              <small>${escapeHtml(detail)} / ${escapeHtml(relativeTaskTime(job.updated_at || job.created_at))}</small>
            </span>
            <span class="case-task-status">${escapeHtml(taskStatusLabel(status))}</span>
          </button>
          ${(resultPath || clearable) ? `<span class="case-task-actions">
            ${resultPath ? `<a class="case-task-open" href="${escapeHtml(resultPath)}">Open</a>` : ""}
            ${clearable ? `<button class="case-task-clear" type="button" data-task-id="${escapeHtml(job.id)}" data-task-title="${escapeHtml(title)}" aria-label="Clear task ${escapeHtml(title)}" title="Clear task"><svg aria-hidden="true" viewBox="0 0 24 24" fill="none"><path d="M4 7h16M9 7V4h6v3m-8 0 1 13h8l1-13M10 11v5m4-5v5" /></svg></button>` : ""}
          </span>` : ""}
        </article>
      `;
    }).join("");
  }

  function taskListWorkLabel(status, progress) {
    if (status === "planned") return "not started";
    if (["queued", "planning"].includes(status)) return "waiting";
    if (status === "running") return `${Number(progress.processed) || 0} checks complete`;
    if (status === "failed") return "stopped";
    if (status === "cancelled") return "cancelled";
    return status;
  }

  function relativeTaskTime(value) {
    const milliseconds = Date.now() - new Date(value || 0).getTime();
    if (!Number.isFinite(milliseconds) || milliseconds < 0) return "now";
    const minutes = Math.floor(milliseconds / 60_000);
    if (minutes < 1) return "now";
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h ago`;
    return `${Math.floor(hours / 24)}d ago`;
  }

  async function loadRecentTasks() {
    if (!caseTaskListEl) return;
    try {
      const response = await fetch(builderApiUrl("/api/investigations?limit=10"), { cache: "no-store" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok || data.ok === false) throw new Error(data.error || `Task list failed with ${response.status}`);
      recentCaseJobs = Array.isArray(data.jobs) ? data.jobs : [];
      renderRecentTasks();
    } catch (_error) {
      caseTaskListEl.innerHTML = '<div class="case-task-empty error">Recent tasks are temporarily unavailable.</div>';
    }
  }

  async function showRecentTask(jobId) {
    const response = await fetch(builderApiUrl(`/api/investigations/${encodeURIComponent(jobId)}`), { cache: "no-store" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.error || `Task detail failed with ${response.status}`);
    const job = data.job || {};
    currentCaseJobId = String(job.id || "");
    currentCaseJobStatus = String(job.status || "");
    caseRunButton.textContent = currentCaseJobStatus === "failed" ? "Retry task" : "Run approved scope";
    setBuilderFeedback();
    caseOpenResultEl.classList.add("hidden");
    if (["planned", "failed"].includes(currentCaseJobStatus)) renderCasePlan(job.draft || job.plan);
    else casePlanEl.classList.add("hidden");
    const path = String(job.result?.artifact?.path || "");
    if (path) {
      caseOpenResultEl.href = path;
      caseOpenResultEl.classList.remove("hidden");
    }
    renderBuilderStdout(job);
    if (["queued", "planning", "running"].includes(currentCaseJobStatus) && !watchedCaseJobs.has(currentCaseJobId)) {
      watchBuilderJob(currentCaseJobId).catch((error) => {
        if (currentCaseJobId === jobId) setBuilderFeedback(error.message || "Live task updates stopped.", "error");
      });
    }
  }

  async function cancelCurrentTask() {
    if (!currentCaseJobId || !window.confirm("Cancel this investigation? Completed registry work will remain in its task record.")) return;
    const response = await fetch(builderApiUrl(`/api/investigations/${encodeURIComponent(currentCaseJobId)}`), { method: "DELETE" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.error || `Cancellation failed with ${response.status}`);
    await showRecentTask(currentCaseJobId);
    await loadRecentTasks();
  }

  async function clearRecentTask(jobId, title) {
    if (!jobId || !window.confirm(`Clear the task "${title || "Untitled investigation"}"? Its generated graph will not be deleted.`)) return;
    const response = await fetch(builderApiUrl(`/api/investigations/${encodeURIComponent(jobId)}/clear`), { method: "DELETE" });
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.ok === false) throw new Error(data.error || `Task clearing failed with ${response.status}`);
    recentCaseJobs = recentCaseJobs.filter((job) => job.id !== jobId);
    if (currentCaseJobId === jobId) {
      currentCaseJobId = "";
      currentCaseJobStatus = "";
      caseProgressEl?.classList.add("hidden");
      caseOpenResultEl.classList.add("hidden");
      setRunLogOpen(false);
      setBuilderStatus("");
    }
    renderRecentTasks();
  }

  function setRunLogOpen(open) {
    const next = !!open;
    runLogSheetEl?.classList.toggle("hidden", !next);
    runLogBackdropEl?.classList.toggle("hidden", !next);
    caseProgressOpenEl?.setAttribute("aria-expanded", String(next));
    document.body.classList.toggle("run-log-open", next);
    if (next) {
      builderStatusEl.scrollTop = builderStatusEl.scrollHeight;
      runLogCloseButton?.focus();
    }
  }

  function setAppMode(mode) {
    const isBuilder = mode === "builder";
    document.body.classList.toggle("builder-mode", isBuilder);
    builderPanelEl?.classList.toggle("hidden", !isBuilder);
    modeViewerButton?.classList.toggle("active", !isBuilder);
    modeBuilderButton?.classList.toggle("active", isBuilder);
    if (isBuilder) loadRecentTasks();
    if (!isBuilder && renderer) {
      window.requestAnimationFrame(() => applyViewerState());
    }
  }

  function renderCasePlan(plan) {
    currentCasePlan = plan;
    casePlanTitleEl.value = plan.title || "Untitled case";
    casePlanIdEl.value = plan.graphId || "";
    casePlanInputsEl.innerHTML = (Array.isArray(plan.seeds) ? plan.seeds : []).map(renderCaseInput).join("");
    caseExpansionInput.value = String(Number.isInteger(plan.expansionCycles) ? plan.expansionCycles : 1);
    caseEntitiesInput.value = String(plan.entityCeiling || 5000);
    caseMaxAddressesInput.value = String(plan.nearby?.maxAddresses || 200);
    const nearbyRadius = Number(plan.nearby?.radiusMetres) || 250;
    caseNearbyEnabledInput.checked = !!plan.nearby?.enabled;
    caseNearbyCentreInput.value = nearbyCentreFromPlan(plan);
    caseNearbyRadiusInput.value = String(nearbyRadius);
    updateNearbyControls({ preview: !!plan.nearby?.enabled });
    caseExpandPeopleInput.checked = plan.expandPeople !== false;
    caseEnrichDocumentsInput.checked = plan.enrichDocuments !== false;
    caseIncludeFormerInput.checked = plan.includeFormer !== false;
    casePlanEl.classList.remove("hidden");
    casePlanEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  function safeExternalUrl(value) {
    try {
      const url = new URL(String(value || ""));
      return ["http:", "https:"].includes(url.protocol) ? url.toString() : "";
    } catch (_error) {
      return "";
    }
  }

  function renderCaseInput(input = {}) {
    const kind = String(input.kind || "person");
    const options = ["address", "person", "company", "charity"].map((value) => (
      `<option value="${value}"${value === kind ? " selected" : ""}>${value}</option>`
    )).join("");
    return `
      <div class="case-input">
        <select class="case-input-kind" aria-label="Input type">${options}</select>
        <input class="case-input-value" type="text" aria-label="Input value" value="${escapeHtml(input.value || "")}" />
        <button class="case-input-remove" type="button" aria-label="Remove input">Remove</button>
      </div>
    `;
  }

  function approvedCasePlan() {
    const title = String(casePlanTitleEl.value || "").trim();
    if (!title) throw new Error("Add a graph name.");
    const seeds = [...casePlanInputsEl.querySelectorAll(".case-input")].map((row) => ({
      kind: row.querySelector(".case-input-kind").value,
      value: row.querySelector(".case-input-value").value.trim(),
    })).filter((input) => input.value);
    if (!seeds.length) throw new Error("Add at least one seed.");
    return {
      title,
      seeds,
      expansionCycles: Number(caseExpansionInput.value || 1),
      expandPeople: caseExpandPeopleInput.checked,
      enrichDocuments: caseEnrichDocumentsInput.checked,
      entityCeiling: Number(caseEntitiesInput.value || 5000),
      includeFormer: caseIncludeFormerInput.checked,
      nearby: {
        enabled: caseNearbyEnabledInput.checked,
        radiusMetres: Number(caseNearbyRadiusInput.value || 250),
        maxAddresses: Number(caseMaxAddressesInput.value || 200),
      },
    };
  }

  function startDirectContract() {
    casePlannerRequest += 1;
    currentCaseJobId = "";
    currentCaseJobStatus = "";
    caseOpenResultEl.classList.add("hidden");
    caseProgressEl?.classList.add("hidden");
    caseRunButton.textContent = "Run approved scope";
    setBuilderFeedback();
    setBuilderStatus("$ new research contract");
    renderCasePlan({
      title: "New investigation",
      seeds: [{ kind: "address", value: "" }],
      expansionCycles: 1,
      expandPeople: true,
      enrichDocuments: true,
      entityCeiling: 5000,
      includeFormer: true,
      nearby: { enabled: false, radiusMetres: 250, maxAddresses: 200 },
    });
    casePlanTitleEl.focus();
    casePlanTitleEl.select();
  }

  function nearbyCentreFromPlan(plan = {}) {
    return (Array.isArray(plan.seeds) ? plan.seeds : [])
      .filter((seed) => seed?.kind === "address")
      .map((seed) => String(seed.value || "").trim())
      .find(Boolean) || "";
  }

  function nearbyRadius() {
    return Math.max(50, Math.min(2000, Number(caseNearbyRadiusInput?.value) || 250));
  }

  function updateNearbyRadiusLabel() {
    if (caseNearbyRadiusValueEl) caseNearbyRadiusValueEl.textContent = `${nearbyRadius().toLocaleString()} m`;
    if (caseNearbyCircle) caseNearbyCircle.setRadius(nearbyRadius());
  }

  function updateNearbyControls(options = {}) {
    const enabled = !!caseNearbyEnabledInput?.checked;
    caseNearbyConfigEl?.classList.toggle("hidden", !enabled);
    updateNearbyRadiusLabel();
    if (!enabled) {
      caseNearbyPreviewRequest += 1;
      if (caseNearbyPreviewTimer) window.clearTimeout(caseNearbyPreviewTimer);
      caseNearbyLayer?.clearLayers();
      caseNearbyCircle = null;
      return;
    }
    window.requestAnimationFrame(() => {
      ensureCaseNearbyMap().catch((error) => {
        if (caseNearbySummaryEl) caseNearbySummaryEl.textContent = error.message || "Map unavailable.";
      });
    });
    if (options.preview !== false) scheduleNearbyPreview(0);
  }

  function syncNearbyCentreFromSeeds() {
    const addressRow = [...casePlanInputsEl.querySelectorAll(".case-input")]
      .find((row) => row.querySelector(".case-input-kind")?.value === "address");
    caseNearbyCentreInput.value = addressRow?.querySelector(".case-input-value")?.value.trim() || "";
    if (caseNearbyEnabledInput.checked) scheduleNearbyPreview();
  }

  function scheduleNearbyPreview(delay = 450) {
    if (caseNearbyPreviewTimer) window.clearTimeout(caseNearbyPreviewTimer);
    caseNearbyPreviewTimer = window.setTimeout(() => {
      previewNearbyAddresses().catch((error) => {
        if (caseNearbySummaryEl) caseNearbySummaryEl.textContent = error.message || "Address preview failed.";
      });
    }, delay);
  }

  async function ensureCaseNearbyMap() {
    if (caseNearbyMap) {
      caseNearbyMap.invalidateSize();
      return;
    }
    await ensureLeafletLoaded();
    caseNearbyMap = L.map("case-nearby-map", { zoomControl: true }).setView([51.505, -0.09], 13);
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(caseNearbyMap);
    caseNearbyLayer = L.layerGroup().addTo(caseNearbyMap);
  }

  async function previewNearbyAddresses() {
    if (!caseNearbyEnabledInput?.checked) return;
    const address = String(caseNearbyCentreInput?.value || "").trim();
    if (!address) {
      caseNearbySummaryEl.textContent = "Enter a centre to preview nearby addresses.";
      caseNearbyLayer?.clearLayers();
      return;
    }
    const requestId = ++caseNearbyPreviewRequest;
    caseNearbySummaryEl.textContent = "Counting registered addresses...";
    const preview = await postBuilderJson("/api/nearby-addresses/preview", {
      address,
      radius_metres: nearbyRadius(),
      max_addresses: Number(caseMaxAddressesInput?.value) || 200,
    });
    if (requestId !== caseNearbyPreviewRequest) return;
    const addresses = Array.isArray(preview.addresses) ? preview.addresses : [];
    const shownCount = addresses.length;
    const addressCount = Number(preview.address_count) || shownCount;
    const companyCount = Number(preview.company_count) || 0;
    caseNearbySummaryEl.textContent = addressCount > shownCount
      ? `${addressCount.toLocaleString()} registered addresses and ${companyCount.toLocaleString()} companies within ${nearbyRadius().toLocaleString()} m. The nearest ${shownCount.toLocaleString()} will be plotted.`
      : `${addressCount.toLocaleString()} registered ${addressCount === 1 ? "address" : "addresses"} and ${companyCount.toLocaleString()} ${companyCount === 1 ? "company" : "companies"} within ${nearbyRadius().toLocaleString()} m.`;
    await renderNearbyPreview(preview);
  }

  async function renderNearbyPreview(preview) {
    await ensureCaseNearbyMap();
    const lat = Number(preview.centre?.lat);
    const lon = Number(preview.centre?.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
    caseNearbyLayer.clearLayers();
    caseNearbyCircle = L.circle([lat, lon], {
      radius: Number(preview.radius_metres) || nearbyRadius(),
      color: "#6e95b9",
      weight: 1.5,
      fillColor: "#6e95b9",
      fillOpacity: 0.08,
    }).addTo(caseNearbyLayer);
    L.circleMarker([lat, lon], {
      radius: 5,
      color: "#d8dee9",
      weight: 2,
      fillColor: "#0d1117",
      fillOpacity: 1,
    }).bindPopup(`<strong>${escapeHtml(preview.centre?.address || "Search centre")}</strong>`).addTo(caseNearbyLayer);
    (Array.isArray(preview.addresses) ? preview.addresses : []).forEach((item) => {
      const itemLat = Number(item.lat);
      const itemLon = Number(item.lon);
      if (!Number.isFinite(itemLat) || !Number.isFinite(itemLon)) return;
      const companies = Array.isArray(item.companies) ? item.companies.length : 0;
      L.circleMarker([itemLat, itemLon], {
        radius: 4,
        color: "#6e95b9",
        weight: 1.5,
        fillColor: "#6e95b9",
        fillOpacity: 0.75,
      }).bindPopup(`<strong>${escapeHtml(item.address || "Registered address")}</strong><br>${Number(item.distance_metres) || 0} m | ${companies} ${companies === 1 ? "company" : "companies"}`).addTo(caseNearbyLayer);
    });
    caseNearbyMap.invalidateSize();
    caseNearbyMap.fitBounds(caseNearbyCircle.getBounds().pad(0.12), { maxZoom: 17 });
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
    const query = String(caseQueryInput?.value || "").trim();
    if (!query) throw new Error("Write a short investigation brief first.");
    const request = ++casePlannerRequest;
    caseOpenResultEl.classList.add("hidden");
    casePlanEl.classList.add("hidden");
    caseProgressEl?.classList.add("hidden");
    setBuilderFeedback("Interpreting the investigation brief...", "working");
    setBuilderStatus(`$ plan ${query}`);
    try {
      const data = await postBuilderJson("/api/investigations/draft", { query });
      if (request !== casePlannerRequest) return;
      if (!data.draft) throw new Error("The planner did not return an investigation form.");
      currentCaseJobId = "";
      currentCaseJobStatus = "";
      caseRunButton.textContent = "Run approved scope";
      renderCasePlan(data.draft);
      setBuilderFeedback("Scope ready. Check the seeds and expansion before you run it.", "success");
      setBuilderStatus("contract: ready for review");
    } catch (error) {
      if (request !== casePlannerRequest) return;
      throw error;
    }
  }

  async function pollBuilderJob(jobId) {
    let consecutiveFailures = 0;
    for (let attempt = 0; attempt < 360; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, attempt === 0 ? 800 : 2500));
      let response;
      try {
        response = await fetch(builderApiUrl(`/api/investigations/${encodeURIComponent(jobId)}`), { cache: "no-store" });
      } catch (error) {
        consecutiveFailures += 1;
        if (consecutiveFailures < 12) continue;
        throw error;
      }
      const data = await response.json().catch(() => ({}));
      if (!response.ok || data.ok === false) {
        const transient = response.status === 429 || response.status >= 500;
        consecutiveFailures += 1;
        if (transient && consecutiveFailures < 12) continue;
        throw new Error(data.error || `Case status failed with ${response.status}`);
      }
      consecutiveFailures = 0;
      const job = data.job || {};
      const selected = currentCaseJobId === jobId;
      renderBuilderStdout(job);
      if (job.status === "completed") {
        const graph = job.result?.artifact || {};
        const path = graph.path || "";
        if (selected && path) {
          caseOpenResultEl.href = path;
          caseOpenResultEl.classList.remove("hidden");
        }
        if (selected) caseRunButton.disabled = false;
        await loadGeneratedGraphOptions();
        await loadRecentTasks();
        return;
      }
      if (["failed", "cancelled"].includes(job.status)) {
        if (selected) {
          caseRunButton.disabled = false;
        }
        await loadRecentTasks();
        return;
      }
    }
    if (currentCaseJobId === jobId) {
      caseRunButton.disabled = false;
      setBuilderStatus("poll timeout; the backend job may still be running", true);
    }
  }

  async function runPlannedCase() {
    if (!currentCasePlan) throw new Error("Draft a case scope first.");
    caseRunButton.disabled = true;
    setBuilderFeedback();
    setBuilderStatus("$ run approved scope");
    const draft = approvedCasePlan();
    let plannedJob = null;
    if (currentCaseJobId && ["planned", "failed"].includes(currentCaseJobStatus)) {
      plannedJob = recentCaseJobs.find((job) => job.id === currentCaseJobId) || { id: currentCaseJobId, status: currentCaseJobStatus, draft };
    } else {
      const graphId = String(casePlanIdEl.value || "").trim();
      const created = await postBuilderJson("/api/investigations", { draft, graphId });
      plannedJob = created.job || {};
      currentCaseJobId = String(plannedJob.id || "");
    }
    if (!currentCaseJobId) throw new Error("The backend did not accept the investigation.");
    renderBuilderStdout(plannedJob);
    const data = await postBuilderJson(`/api/investigations/${encodeURIComponent(currentCaseJobId)}/start`, { draft });
    renderBuilderStdout(data.job || {});
    await watchBuilderJob(currentCaseJobId);
  }

  async function watchBuilderJob(jobId) {
    if (watchedCaseJobs.has(jobId)) return;
    watchedCaseJobs.add(jobId);
    try {
      if (typeof EventSource !== "function") return await pollBuilderJob(jobId);
      await new Promise((resolve, reject) => {
        let received = false;
        let settled = false;
        const stream = new EventSource(builderApiUrl(`/api/investigations/${encodeURIComponent(jobId)}/events`));
        const fallback = () => {
          if (settled) return;
          settled = true;
          stream.close();
          pollBuilderJob(jobId).then(resolve, reject);
        };
        const timeout = window.setTimeout(() => {
          if (!received) fallback();
        }, 5000);
        stream.addEventListener("update", (event) => {
          received = true;
          window.clearTimeout(timeout);
          try {
            const data = JSON.parse(event.data || "{}");
            const job = data.job || {};
            const selected = currentCaseJobId === jobId;
            renderBuilderStdout(job);
            if (job.status === "completed") {
              settled = true;
              stream.close();
              const path = job.result?.artifact?.path || "";
              if (selected && path) {
                caseOpenResultEl.href = path;
                caseOpenResultEl.classList.remove("hidden");
              }
              if (selected) caseRunButton.disabled = false;
              Promise.all([loadGeneratedGraphOptions(), loadRecentTasks()]).finally(resolve);
            } else if (["failed", "cancelled"].includes(job.status)) {
              settled = true;
              stream.close();
              if (selected) caseRunButton.disabled = false;
              loadRecentTasks().finally(resolve);
            }
          } catch (error) {
            settled = true;
            stream.close();
            reject(error);
          }
        });
        stream.onerror = fallback;
      });
    } finally {
      watchedCaseJobs.delete(jobId);
    }
  }

  function resetCaseDesk() {
    casePlannerRequest += 1;
    currentCaseJobId = "";
    currentCaseJobStatus = "";
    currentCasePlan = null;
    casePlanEl.classList.add("hidden");
    caseOpenResultEl.classList.add("hidden");
    caseProgressEl?.classList.add("hidden");
    caseRunButton.disabled = false;
    caseRunButton.textContent = "Run approved scope";
    setBuilderFeedback();
    setRunLogOpen(false);
    setBuilderStatus("");
    caseQueryInput?.focus();
  }

  function currentGraphOption() {
    return GRAPH_OPTIONS.find((option) => option.key === currentGraphKey) || null;
  }

  function setGraphSwitcherSelection(optionEl, label) {
    graphSwitcherMenuEl.querySelectorAll(".graph-switcher-option").forEach((element) => {
      const isActive = element === optionEl;
      element.classList.toggle("active", isActive);
      element.setAttribute("aria-current", isActive ? "page" : "false");
    });
    graphSwitcherLabelEl.textContent = label;
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
    graphSwitcherLabelEl.textContent = activeOption?.label || document.title || currentGeneratedGraphId || "Graph";
    graphSwitcherOptionEls.forEach((optionEl) => {
      const isActive = !currentGeneratedGraphId && optionEl.dataset.graphKey === currentGraphKey;
      optionEl.classList.toggle("active", isActive);
      optionEl.setAttribute("aria-current", isActive ? "page" : "false");
    });
    graphSwitcherButtonEl.addEventListener("click", (event) => {
      event.stopPropagation();
      setGraphSwitcherOpen(graphSwitcherMenuEl.classList.contains("hidden"));
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
        setRunLogOpen(false);
      }
    });
  }

  async function loadGeneratedGraphOptions() {
    if (!graphSwitcherMenuEl) return;
    const response = await fetch(builderApiUrl("/api/generated-graphs"));
    if (!response.ok) return;
    const data = await response.json();
    const graphs = Array.isArray(data.graphs) ? data.graphs : [];
    graphSwitcherMenuEl.querySelectorAll(".graph-switcher-row.generated").forEach((element) => element.remove());
    graphs.forEach((graph) => {
      const graphTitle = String(graph.title || graph.id || "").trim();
      const graphId = String(graph.id || detectGeneratedGraphId(graph.path)).trim().toLowerCase();
      const path = canonicalGeneratedGraphPath(graphId);
      if (!path || !graphTitle) return;
      const button = document.createElement("a");
      button.className = "graph-switcher-option generated";
      button.role = "menuitem";
      button.href = path;
      button.dataset.graphKey = graphId;
      button.textContent = graphTitle;
      const isActive = graphId.toLowerCase() === currentGeneratedGraphId.toLowerCase();
      button.classList.toggle("active", isActive);
      button.setAttribute("aria-current", isActive ? "page" : "false");
      const deleteButton = document.createElement("button");
      deleteButton.className = "graph-delete-button";
      deleteButton.type = "button";
      deleteButton.setAttribute("aria-label", `Delete ${graphTitle}`);
      deleteButton.title = `Delete ${graphTitle}`;
      deleteButton.innerHTML = '<svg aria-hidden="true" viewBox="0 0 24 24" fill="none"><path d="M4 7h16M9 7V4h6v3m-8 0 1 13h8l1-13M10 11v5m4-5v5" /></svg>';
      deleteButton.addEventListener("click", async (event) => {
        event.stopPropagation();
        const confirmed = window.confirm(`Delete "${graphTitle}" and all stored graph data? This cannot be undone.`);
        if (!confirmed) return;
        deleteButton.disabled = true;
        try {
          const response = await fetch(builderApiUrl(`/api/generated-graphs/${encodeURIComponent(graphId)}`), { method: "DELETE" });
          const result = await response.json().catch(() => ({}));
          if (!response.ok || result.ok === false) throw new Error(result.error || `Delete failed with ${response.status}`);
          if (isActive) {
            window.location.assign("/mb/");
            return;
          }
          row.remove();
        } catch (error) {
          deleteButton.disabled = false;
          window.alert(error.message || "The graph could not be deleted.");
        }
      });
      const row = document.createElement("div");
      row.className = "graph-switcher-row generated";
      row.append(button, deleteButton);
      graphSwitcherMenuEl.appendChild(row);
      if (isActive) setGraphSwitcherSelection(button, graphTitle);
    });
    return graphs;
  }

  function canonicalGeneratedGraphPath(graphId) {
    const cleanId = String(graphId || "").trim().toLowerCase();
    return /^[a-z0-9-]+$/.test(cleanId) ? `/generated-graphs/${cleanId}/` : "";
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
    if (edge.relationship_status === "former") return COLORS.purple;
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
      ["show-low-confidence", "Documents", false],
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

  function focalDistance(node) {
    const value = Number(node?.focal_distance_metres);
    return Number.isFinite(value) && value >= 0 ? value : null;
  }

  function configureFocalDistanceFilter() {
    const distances = allNodes.map(focalDistance).filter((value) => value !== null);
    const configuredRadius = Number(graphData.focus_radius_metres);
    if (!distances.length) {
      focalDistanceFilterEl?.classList.add("hidden");
      viewerState.maxFocalDistanceMetres = null;
      return;
    }
    const maximum = Math.max(
      Number.isFinite(configuredRadius) ? configuredRadius : 0,
      ...distances,
    );
    const roundedMaximum = Math.max(25, Math.ceil(maximum / 25) * 25);
    focalDistanceRangeEl.max = String(roundedMaximum);
    focalDistanceRangeEl.value = String(roundedMaximum);
    focalDistanceFilterEl.dataset.maximum = String(roundedMaximum);
    focalDistanceFilterEl.classList.remove("hidden");
    viewerState.maxFocalDistanceMetres = null;
    renderFocalDistanceValue();
  }

  function renderFocalDistanceValue() {
    if (!focalDistanceValueEl || !focalDistanceRangeEl) return;
    const selected = Number(focalDistanceRangeEl.value);
    const maximum = Number(focalDistanceFilterEl?.dataset.maximum);
    focalDistanceValueEl.textContent = selected >= maximum ? `All within ${maximum} m` : `Up to ${selected} m`;
  }

  function applyFocalDistanceFilter(projection) {
    const maximum = viewerState.maxFocalDistanceMetres;
    if (maximum === null) return projection;
    const visibleIds = new Set(
      [...projection.visibleIds].filter((nodeId) => {
        const node = nodeById.get(nodeId);
        if (node?.kind === "seed") return true;
        const distance = focalDistance(node);
        return distance !== null && distance <= maximum;
      }),
    );
    return {
      ...projection,
      visibleIds,
      edgeIds: projection.edgeIds.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target)),
    };
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
    const hiddenEnrichmentNodeIds = hiddenLatestEnrichmentNodeIds();
    const activeBaseNodes = baseNodes.filter((node) => !hiddenEnrichmentNodeIds.has(node.id));
    const mainNodeIds = new Set(activeBaseNodes.map((node) => node.id));
    const activeBaseEdges = baseEdges.filter((edge) => mainNodeIds.has(edge.source) && mainNodeIds.has(edge.target));
    allEdges = activeBaseEdges.filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed").map((edge) => ({ ...edge }));
    if ((!viewerState.showLowConfidence || !lowConfidenceLoaded)
      && (!viewerState.showLowConfidenceNodes || !lowConfidenceOrgLoaded)) {
      allNodes = activeBaseNodes.filter((node) => node.kind !== "seed").map((node) => ({ ...node }));
      if (Array.isArray(mergeOverrides?.organisation) && mergeOverrides.organisation.length) {
        const merged = applyMergeOverrides(allNodes, allEdges, { organisation: mergeOverrides.organisation });
        allNodes = merged.nodes;
        allEdges = merged.edges;
      }
      rebuildIndexes();
      return;
    }
    const activeSeedIds = new Set(
      activeBaseNodes
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

    allNodes = activeBaseNodes
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
    if (node.promoted_to_seed) return 0;
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
    return applyFocalDistanceFilter(applyHighlightOnlyFilters(projection));
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
    nodes.forEach((node) => {
      node._pillWidth = pillWidth(node);
      node._pillHeight = pillHeight(node);
    });
    let curY = bounds.top + 72;
    [0, 1, 2, 3, 4].forEach((lane) => {
      const laneNodes = nodes.filter((node) => Number(node.lane || 0) === lane);
      const neighborXByNodeId = new Map(
        laneNodes.map((node) => [node.id, avgNeighborX(node, edgeAdjacency, nodeLookup, fallbackCenter)]),
      );
      const lowConfidenceAnchorByNodeId = lane === 2
        ? new Map(laneNodes.map((node) => [node.id, lowConfidenceLaneAnchorX(node, edgeAdjacency, nodeLookup)]))
        : new Map();
      laneNodes.sort((left, right) => {
        if (lane === 0) {
          const rootDifference = Number(rootIds.has(right.id)) - Number(rootIds.has(left.id));
          if (rootDifference !== 0) return rootDifference;
          const promotedDifference = Number(!!right.promoted_to_seed) - Number(!!left.promoted_to_seed);
          if (promotedDifference !== 0) return promotedDifference;
        }
        if (lane === 2) {
          const leftAnchor = lowConfidenceAnchorByNodeId.get(left.id);
          const rightAnchor = lowConfidenceAnchorByNodeId.get(right.id);
          if (leftAnchor != null || rightAnchor != null) {
            if (leftAnchor == null) return 1;
            if (rightAnchor == null) return -1;
            const anchorDiff = leftAnchor - rightAnchor;
            if (anchorDiff !== 0) return anchorDiff;
          }
        }
        const connectionDiff = nodeConnectionOrderScore(right) - nodeConnectionOrderScore(left);
        if (connectionDiff !== 0) return connectionDiff;
        const neighborDiff = neighborXByNodeId.get(left.id) - neighborXByNodeId.get(right.id);
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
        const nodeWidth = node._pillWidth;
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
      const rowStep = rows.length ? Math.max(...rows.flat().map((node) => node._pillHeight)) + rowGap : 0;
      rows.forEach((row, rowIndex) => {
        const rowW = row.reduce((sum, node) => sum + node._pillWidth, 0) + (spacing * Math.max(0, row.length - 1));
        let cx = usableMin + Math.max(0, (maxRowW - rowW) / 2);
        const rowY = curY + (rowIndex * rowStep);
        row.forEach((node) => {
          const widthForNode = node._pillWidth;
          node.x = cx + (widthForNode / 2);
          node.y = rowY;
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

  function buildSceneForProjection(projection, bounds, sceneKey = "base") {
    const rootIds = new Set(projection.rootIds || []);
    const lowConfidenceOnlyIds = lowConfidenceOnlyVisibleNodeIdsForProjection(projection);
    const sceneNodes = allNodes
      .filter((node) => projection.visibleIds.has(node.id))
      .map((node) => ({
        ...node,
        _sceneKey: `${sceneKey}:node:${node.id}`,
        _visible: true,
        _lowConfidenceOnlyVisible: lowConfidenceOnlyIds.has(node.id),
      }));
    const sceneEdges = projection.edgeIds
      .filter((edge) => projection.visibleIds.has(edge.source) && projection.visibleIds.has(edge.target))
      .map((edge, index) => ({
        ...edge,
        _sceneKey: `${sceneKey}:edge:${edge.id || `${edge.source}:${edge.target}:${edge.kind || "link"}:${index}`}`,
      }));
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
          buildSceneForProjection(entry.projection, bounds, `tree-${start + offset}`),
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
    tooltipWidth = tooltip.offsetWidth;
    tooltipHeight = tooltip.offsetHeight;
    positionTooltip(event);
  }

  function positionTooltip(event) {
    if (!event) return;
    const pad = 14;
    const width = tooltipWidth;
    const height = tooltipHeight;
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
      const distance = focalDistance(node);
      if (distance !== null) {
        const anchor = String(node.focal_address || "").trim();
        lines.push(`${node.kind === "address" && node.focal_distance_basis === "postcode_centroid" ? "Focal distance" : "Focal association"}: ${distance.toLocaleString()} m${anchor && anchor !== node.label ? ` via ${escapeHtml(anchor)}` : ""}`);
      }
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
    const roleTitle = String(titleMatch?.[1] || "").trim();
    const representedOrganisations = Array.isArray(edge?.represented_organisation_labels) ? edge.represented_organisation_labels : [];
    const representedSigners = Array.isArray(edge?.represented_signer_labels) ? edge.represented_signer_labels : [];
    const subject = roleTitle && !sourceLabel.toLowerCase().startsWith(`${roleTitle.toLowerCase()} `)
      ? `${roleTitle} ${sourceLabel}`
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
    if (baseNodes.some(isLowConfidenceDocumentNode)) return true;
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
      sourceLabel: String(row?.sourceLabel || ""),
      targetLabel: String(row?.targetLabel || ""),
      leaderLabel: String(row?.leaderLabel || ""),
      reason: String(row?.reason || ""),
      decidedAt: String(row?.decidedAt || ""),
    })).filter((row) => {
      if (!row.sourceId || !row.targetId || row.sourceId === row.targetId) return false;
      const key = `${row.sourceId}||${row.targetId}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }

  function normalizeRejectedRows(rows) {
    return normalizeMergeOverrideRows(rows).map((row) => ({
      ...row,
      kind: ["address", "name", "organisation"].includes(String(row.kind || ""))
        ? String(row.kind)
        : String((Array.isArray(rows) ? rows : []).find((candidate) => candidate?.sourceId === row.sourceId && candidate?.targetId === row.targetId)?.kind || "name"),
    }));
  }

  function normalizeAuditRows(rows) {
    return (Array.isArray(rows) ? rows : []).map((row) => ({
      id: String(row?.id || ""),
      action: String(row?.action || ""),
      at: String(row?.at || ""),
      kind: String(row?.kind || ""),
      sourceId: String(row?.sourceId || ""),
      targetId: String(row?.targetId || ""),
      sourceLabel: String(row?.sourceLabel || ""),
      targetLabel: String(row?.targetLabel || ""),
      reason: String(row?.reason || ""),
    })).filter((row) => row.action && row.at);
  }

  function readMergeOverrides(overrides = {}) {
    return {
      address: normalizeMergeOverrideRows(overrides.address),
      name: normalizeMergeOverrideRows(overrides.name),
      organisation: normalizeMergeOverrideRows(overrides.organisation),
      seed: normalizeHiddenOverrideRows(overrides.seed),
      hidden: normalizeHiddenOverrideRows(overrides.hidden),
      rejected: normalizeRejectedRows(overrides.rejected),
      audit: normalizeAuditRows(overrides.audit),
    };
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
    const targetDistance = focalDistance(target);
    const sourceDistance = focalDistance(source);
    if (sourceDistance !== null && (targetDistance === null || sourceDistance < targetDistance)) {
      target.focal_distance_metres = sourceDistance;
      target.focal_address = source.focal_address;
      target.focal_origin = source.focal_origin;
      target.focal_distance_basis = source.focal_distance_basis;
    }
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

  function applySeedPromotions(nodes, overrides) {
    const promotedKeys = new Set(normalizeHiddenOverrideRows(overrides?.seed).map((row) => row.nodeId));
    if (!promotedKeys.size) return nodes.slice();
    return nodes.map((node) => {
      if (node.kind === "seed" || !nodeMergeStableKeys(node).some((key) => promotedKeys.has(key))) return node;
      const promoted = cloneNodeForMerge(node);
      promoted.kind = "seed_alias";
      promoted.lane = 0;
      promoted.promoted_to_seed = true;
      promoted.seed_names = uniqueValues([...(promoted.seed_names || []), promoted.label]);
      promoted.tooltip_lines = uniqueValues([...(promoted.tooltip_lines || []), `Seed: ${promoted.label}`]);
      return promoted;
    });
  }

  function rebuildBaseGraph() {
    const merged = applyMergeOverrides(rawMainNodes, rawMainEdges, mergeOverrides);
    const promotedNodes = applySeedPromotions(merged.nodes, mergeOverrides);
    const filtered = applyHiddenOverrides(promotedNodes, merged.edges, mergeOverrides);
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
        mergeOverrides = readMergeOverrides(overrides);
        resolutionCandidatesCache = null;
        rebuildBaseGraph();
        return true;
      })
      .catch((error) => {
        console.warn(error);
        mergeOverrides = readMergeOverrides();
        resolutionCandidatesCache = null;
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
        sourceLabel: action.sourceLabel,
        targetLabel: action.targetLabel,
        leaderLabel: action.leaderLabel,
        reason: action.reason,
      }),
    });
    if (!response.ok) {
      throw new Error(`Merge persistence failed (${response.status})`);
    }
    const payload = await response.json();
    const overrides = payload?.overrides || {};
    mergeOverrides = readMergeOverrides(overrides);
    resolutionCandidatesCache = null;
    viewerState.pendingMergeNodeId = "";
    rebuildBaseGraph();
    await applyViewerState();
  }

  async function persistSeedOverride(action) {
    const affectedNodeId = baseNodes.find((node) => nodeMergeStableKeys(node).includes(action.nodeKey))?.id || "";
    const response = await fetch(graphFunctionUrl(MERGE_OVERRIDES_URL), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        graph: currentGraphKey,
        operation: String(action.operation || "add"),
        kind: "seed",
        nodeId: action.nodeKey,
        label: action.nodeLabel,
      }),
    });
    if (!response.ok) throw new Error(`Seed promotion persistence failed (${response.status})`);
    const payload = await response.json();
    mergeOverrides = readMergeOverrides(payload?.overrides || {});
    resolutionCandidatesCache = null;
    rebuildBaseGraph();
    const promotedNode = baseNodes.find((node) => (
      node.promoted_to_seed && nodeMergeStableKeys(node).includes(action.nodeKey)
    ));
    if (action.operation === "add" && promotedNode) {
      searchInput.value = "";
      viewerState.searchQuery = "";
      setSingleFocus(promotedNode.id);
    } else if (action.operation === "remove" && viewerState.focusedNodeIds.has(affectedNodeId)) {
      viewerState.focusedNodeIds.clear();
    }
    await applyViewerState();
  }

  async function persistResolutionDecision(action) {
    const response = await fetch(graphFunctionUrl(MERGE_OVERRIDES_URL), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        graph: currentGraphKey,
        operation: String(action.operation || "add"),
        kind: "rejected",
        resolutionKind: action.kind,
        sourceId: action.sourceKey,
        targetId: action.targetKey,
        sourceLabel: action.sourceLabel,
        targetLabel: action.targetLabel,
        reason: action.reason,
      }),
    });
    if (!response.ok) throw new Error(`Resolution persistence failed (${response.status})`);
    const payload = await response.json();
    mergeOverrides = readMergeOverrides(payload?.overrides || {});
    resolutionCandidatesCache = null;
    if (document.querySelector('.sidebar-pane[data-pane="resolve"]')?.classList.contains("active")) renderResolutionPanel();
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
    mergeOverrides = readMergeOverrides(overrides);
    resolutionCandidatesCache = null;
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

  function resolutionPairKey(kind, sourceKey, targetKey) {
    return `${kind}:${[String(sourceKey || ""), String(targetKey || "")].sort().join("||")}`;
  }

  function resolutionKeysForNode(node) {
    const kind = mergeKindForNode(node);
    if (!kind) return [];
    const keys = nodeMergeStableKeys(node).filter((key) => !key.startsWith("node-id:"));
    const labels = [node.label, ...(Array.isArray(node.aliases) ? node.aliases : [])]
      .map((value) => String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim())
      .filter(Boolean)
      .map((value) => `label:${value}`);
    return uniqueValues([...keys, ...labels]);
  }

  function resolutionReason(key) {
    if (key.startsWith("address:")) return "Same normalised address";
    if (key.startsWith("org:")) return "Same registry identifier";
    if (key.startsWith("person:") || key.startsWith("individual:") || key.startsWith("identity:")) return "Shared person identity key";
    if (key.startsWith("label:")) return "Same normalised name or alias";
    return "Shared stable identity key";
  }

  function resolutionCandidates() {
    if (resolutionCandidatesCache) return resolutionCandidatesCache;
    const buckets = new Map();
    rawMainNodes.forEach((node) => {
      const kind = mergeKindForNode(node);
      if (!kind) return;
      resolutionKeysForNode(node).forEach((key) => {
        const bucketKey = `${kind}:${key}`;
        if (!buckets.has(bucketKey)) buckets.set(bucketKey, []);
        buckets.get(bucketKey).push(node);
      });
    });
    const decided = new Set();
    ["address", "name", "organisation"].forEach((kind) => {
      normalizeMergeOverrideRows(mergeOverrides[kind]).forEach((row) => decided.add(resolutionPairKey(kind, row.sourceId, row.targetId)));
    });
    normalizeRejectedRows(mergeOverrides.rejected).forEach((row) => decided.add(resolutionPairKey(row.kind, row.sourceId, row.targetId)));
    const candidates = new Map();
    buckets.forEach((nodes, bucketKey) => {
      const separator = bucketKey.indexOf(":");
      const kind = bucketKey.slice(0, separator);
      const sharedKey = bucketKey.slice(separator + 1);
      const uniqueNodes = [...new Map(nodes.map((node) => [node.id, node])).values()].slice(0, 12);
      for (let left = 0; left < uniqueNodes.length; left += 1) {
        for (let right = left + 1; right < uniqueNodes.length; right += 1) {
          const source = uniqueNodes[left];
          const target = uniqueNodes[right];
          const sourceKey = nodeMergePrimaryKey(source);
          const targetKey = nodeMergePrimaryKey(target);
          const pairKey = resolutionPairKey(kind, sourceKey, targetKey);
          if (!sourceKey || !targetKey || decided.has(pairKey)) continue;
          const current = candidates.get(pairKey) || { kind, source, target, sourceKey, targetKey, reasons: [] };
          current.reasons = uniqueValues([...current.reasons, resolutionReason(sharedKey)]);
          candidates.set(pairKey, current);
        }
      }
    });
    resolutionCandidatesCache = [...candidates.values()]
      .sort((left, right) => right.reasons.length - left.reasons.length || String(left.source.label).localeCompare(String(right.source.label)))
      .slice(0, 30);
    return resolutionCandidatesCache;
  }

  function resolutionDecisionLabel(row, side) {
    const explicit = String(row?.[`${side}Label`] || "").trim();
    if (explicit) return explicit;
    const key = String(row?.[`${side}Id`] || "");
    const node = rawMainNodes.find((candidate) => nodeHasStableKey(candidate, key));
    return String(node?.label || key || "node");
  }

  function renderResolutionPanel() {
    if (!resolutionPanelEl) return;
    const candidates = resolutionCandidates();
    const actions = [];
    const candidateHtml = candidates.length ? candidates.map((candidate) => {
      const sourceLabel = String(candidate.source.label || candidate.source.id);
      const targetLabel = String(candidate.target.label || candidate.target.id);
      const baseAction = {
        kind: candidate.kind,
        sourceKey: candidate.sourceKey,
        targetKey: candidate.targetKey,
        sourceLabel,
        targetLabel,
        reason: candidate.reasons.join("; "),
      };
      const keepLeftIndex = actions.push({ ...baseAction, type: "merge", leaderKey: candidate.sourceKey, leaderLabel: sourceLabel }) - 1;
      const keepRightIndex = actions.push({ ...baseAction, type: "merge", leaderKey: candidate.targetKey, leaderLabel: targetLabel }) - 1;
      const rejectIndex = actions.push({ ...baseAction, type: "reject" }) - 1;
      return `
        <div class="resolution-card">
          <div class="resolution-pair">${escapeHtml(sourceLabel)}<br>${escapeHtml(targetLabel)}</div>
          <div class="resolution-reason">${escapeHtml(candidate.reasons.join(" · "))}</div>
          <div class="resolution-actions">
            <button data-resolution-index="${keepLeftIndex}">Keep first</button>
            <button data-resolution-index="${keepRightIndex}">Keep second</button>
            <button data-resolution-index="${rejectIndex}" data-resolution-action="reject">Not the same</button>
          </div>
        </div>`;
    }).join("") : '<div class="context-menu-empty">No unresolved duplicate candidates.</div>';

    const decisions = [];
    ["address", "name", "organisation"].forEach((kind) => {
      normalizeMergeOverrideRows(mergeOverrides[kind]).forEach((row) => decisions.push({ ...row, kind, decision: "Merged" }));
    });
    normalizeRejectedRows(mergeOverrides.rejected).forEach((row) => decisions.push({ ...row, decision: "Rejected" }));
    const decisionHtml = decisions.length ? decisions.map((decision) => {
      const sourceLabel = resolutionDecisionLabel(decision, "source");
      const targetLabel = resolutionDecisionLabel(decision, "target");
      const undoIndex = actions.push({
        type: decision.decision === "Merged" ? "undo_merge" : "undo_reject",
        kind: decision.kind,
        sourceKey: decision.sourceId,
        targetKey: decision.targetId,
        sourceLabel,
        targetLabel,
        reason: decision.reason,
      }) - 1;
      return `
        <div class="resolution-decision">
          <div class="resolution-pair">${escapeHtml(decision.decision)}: ${escapeHtml(sourceLabel)} / ${escapeHtml(targetLabel)}</div>
          <div class="resolution-reason">${escapeHtml(decision.reason || decision.leaderLabel || decision.kind)}</div>
          <div class="resolution-actions"><button data-resolution-index="${undoIndex}">Undo</button></div>
        </div>`;
    }).join("") : '<div class="context-menu-empty">No manual decisions yet.</div>';

    const auditHtml = normalizeAuditRows(mergeOverrides.audit).slice(-12).reverse().map((row) => `
      <div class="resolution-audit">${escapeHtml(row.at.replace("T", " ").replace("Z", ""))} · ${escapeHtml(row.action.replaceAll("_", " "))} · ${escapeHtml(row.sourceLabel || row.sourceId)} / ${escapeHtml(row.targetLabel || row.targetId)}</div>
    `).join("");
    resolutionPanelEl._actions = actions;
    resolutionPanelEl.innerHTML = `
      <div class="resolution-heading">Suspected duplicates <span>${candidates.length}</span></div>
      ${candidateHtml}
      <div class="resolution-heading">Decisions <span>${decisions.length}</span></div>
      ${decisionHtml}
      <div class="resolution-heading">Audit trail</div>
      ${auditHtml || '<div class="context-menu-empty">No decision history.</div>'}
    `;
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
    const promotedSeedKeys = new Set(normalizeHiddenOverrideRows(mergeOverrides.seed).map((row) => row.nodeId));
    const isPromotedSeed = nodeMergeStableKeys(node).some((key) => promotedSeedKeys.has(key));
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
    const questionSelected = viewerState.questionNodeIds.includes(node.id);
    const enrichmentRoundAction = enrichmentRoundActionForNode(node);
    const actions = [
      { label: "Explain claims and attribution", type: "node_claims", nodeId: node.id },
      enrichmentRoundAction,
      questionSelected
        ? { label: "Remove from question selection", type: "question_remove", nodeId: node.id }
        : viewerState.questionNodeIds.length < 8
          ? { label: "Add to question selection", type: "question_add", nodeId: node.id }
          : null,
      viewerState.questionNodeIds.length
        ? { label: "Ask about selected subgraph", type: "question_open" }
        : null,
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
      mergeKind === "name" && mergePrimaryKey
        ? {
            label: isPromotedSeed ? "Restore as person" : "Promote to seed",
            type: isPromotedSeed ? "seed_remove" : "seed_add",
            nodeKey: mergePrimaryKey,
            nodeLabel: String(node.label || node.id || "node"),
          }
        : null,
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

  function evidenceItemsForEdge(edge) {
    const evidenceItems = [];
    const seen = new Set();
    const pushEvidence = (evidence, sourceEdge = edge) => {
      if (!evidence || typeof evidence !== "object") return;
      const url = String(evidenceActionUrl(evidence) || evidence.document_url || "").trim();
      const page = String(evidence.page_hint || evidence.page_number || "").trim();
      const key = `${url}||${page}||${String(evidence.title || "")}||${String(evidence.notes || "")}`;
      if (seen.has(key)) return;
      seen.add(key);
      evidenceItems.push({ evidence, sourceEdge });
    };
    (Array.isArray(edge?.evidence_items) ? edge.evidence_items : []).forEach((item) => pushEvidence(item, edge));
    if (edge?.evidence) pushEvidence(edge.evidence, edge);
    (Array.isArray(edge?.pathEdges) ? edge.pathEdges : []).forEach((pathEdge) => {
      (Array.isArray(pathEdge?.evidence_items) ? pathEdge.evidence_items : []).forEach((item) => pushEvidence(item, pathEdge));
      if (pathEdge?.evidence) pushEvidence(pathEdge.evidence, pathEdge);
    });
    return evidenceItems;
  }

  function evidenceActionsForEdge(edge) {
    return evidenceItemsForEdge(edge)
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

  function edgeDateRows(edge) {
    const pathEdges = Array.isArray(edge?.pathEdges) ? edge.pathEdges : [];
    const valueFor = (field) => edge?.[field] || pathEdges.find((pathEdge) => pathEdge?.[field])?.[field] || "";
    const fields = [
      ["Filing date", valueFor("filing_date")],
      ["Start date", valueFor("start_date")],
      ["End date", valueFor("end_date")],
      ["Statement date", valueFor("statement_date")],
      ["Record date", valueFor("date")],
    ];
    return fields.filter(([, value]) => String(value || "").trim());
  }

  function renderEdgeEvidenceHtml(edge) {
    const sourceNode = displayNodeForEdgeId(edge.source, edge?._sourceNode);
    const targetNode = displayNodeForEdgeId(edge.target, edge?._targetNode);
    const why = plainText(tooltipLinesForEdge(edge).join(" ")) || plainText(edge.tooltip) || "No relationship explanation supplied.";
    const dates = edgeDateRows(edge);
    const evidence = evidenceItemsForEdge(edge);
    const pathEdges = Array.isArray(edge?.pathEdges) ? edge.pathEdges : [];
    const evidenceProvider = (() => {
      const url = String(evidenceActionUrl(evidence[0]?.evidence) || "").toLowerCase();
      if (url.includes("company-information.service.gov.uk")) return "Companies House";
      if (url.includes("charitycommission.gov.uk")) return "Charity Commission";
      try {
        return url ? new URL(url).hostname : "";
      } catch (_error) {
        return "";
      }
    })();
    const sourceProvider = String(edge?.source_provider || edge?.provider || pathEdges.find((pathEdge) => pathEdge?.source_provider)?.source_provider || evidenceProvider || "Not supplied");
    const confidence = String(edge?.confidence || pathEdges.find((pathEdge) => pathEdge?.confidence)?.confidence || "Not supplied");
    return `
      <div class="analysis-selection">${escapeHtml(sourceNode?.label || edge.source)} to ${escapeHtml(targetNode?.label || edge.target)}</div>
      <div class="analysis-section">
        <div class="analysis-section-title">Why these nodes are connected</div>
        <div class="analysis-text">${escapeHtml(why)}</div>
      </div>
      <div class="analysis-claim-meta">Relationship: ${escapeHtml(edgeSubtitle(edge) || edge.kind || "link")}<br>Provider: ${escapeHtml(sourceProvider)}<br>Confidence: ${escapeHtml(confidence)}${dates.map(([label, value]) => `<br>${escapeHtml(label)}: ${escapeHtml(value)}`).join("")}</div>
      <div class="analysis-section">
        <div class="analysis-section-title">Source records</div>
        ${evidence.length ? evidence.map(({ evidence: item }, index) => {
          const url = evidenceActionUrl(item);
          const details = [item.filing_date || item.date, item.filing_description, item.page_hint ? `Page: ${item.page_hint}` : "", item.page_number ? `Page ${item.page_number}` : "", item.notes]
            .map((value) => String(value || "").trim()).filter(Boolean);
          return `
            <div class="analysis-claim">
              <div class="analysis-claim-header">
                <div class="analysis-claim-index">${index + 1}</div>
                <div class="analysis-claim-text">${escapeHtml(evidenceDisplayTitle(item, "Registry record"))}</div>
              </div>
              ${details.length ? `<div class="analysis-claim-note">${escapeHtml(details.join(" · "))}</div>` : ""}
              <div class="analysis-claim-evidence">${url ? `<a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">Open exact source</a>` : '<span class="dim">No source URL supplied.</span>'}</div>
            </div>`;
        }).join("") : '<div class="context-menu-empty">No source record is attached to this relationship.</div>'}
      </div>
    `;
  }

  function openEdgeEvidenceView(edge) {
    openDetailsModal({
      title: "Relationship evidence",
      status: `${edgeSubtitle(edge) || edge.kind || "Link"} · ${edge.confidence || "confidence unavailable"}`,
      bodyHtml: renderEdgeEvidenceHtml(edge),
    });
  }

  function openEdgeContextMenu(edge, event) {
    event.preventDefault();
    event.stopPropagation();
    hideTooltip();
    const sourceNode = displayNodeForEdgeId(edge.source, edge?._sourceNode);
    const targetNode = displayNodeForEdgeId(edge.target, edge?._targetNode);
    const actions = [
      { type: "edge_details", label: "View relationship evidence", edge },
      ...(edge?.kind === "hidden_connection"
        ? [{ type: "hidden_connection_expand", label: "Expand indirect path", edge }]
        : []),
      ...evidenceActionsForEdge(edge),
    ];
    contextMenuEl._actions = actions;
    contextMenuEl.innerHTML = [
      `<div class="context-menu-title">${escapeHtml(sourceNode?.label || edge.source)} to ${escapeHtml(targetNode?.label || edge.target)}</div>`,
      `<div class="context-menu-subtitle">${escapeHtml(edgeSubtitle(edge) || "link")}</div>`,
      actions.map((action, index) => `<button type="button" class="context-menu-item" data-action-index="${index}">${escapeHtml(action.label)}</button>`).join(""),
    ].join("");
    contextMenuEl.style.display = "block";
    contextMenuEl.style.left = `${Math.max(10, Math.min(event.clientX, window.innerWidth - 260))}px`;
    contextMenuEl.style.top = `${Math.max(10, Math.min(event.clientY, window.innerHeight - 220))}px`;
  }

  function closeContextMenu() {
    contextMenuEl.style.display = "none";
    contextMenuEl._actions = [];
  }

  function renderQuestionSelection() {
    if (!questionSelectionEl) return;
    viewerState.questionNodeIds = viewerState.questionNodeIds.filter((id, index, ids) => ids.indexOf(id) === index && nodeById.has(id)).slice(0, 8);
    const labels = viewerState.questionNodeIds.map((id) => nodeById.get(id)?.label || id);
    questionSelectionEl.textContent = labels.length
      ? `Selected: ${labels.join(" · ")}`
      : "Select nodes from their context menus.";
    questionSubmitEl.disabled = !labels.length;
  }

  function shortestVisiblePath(sourceId, targetId) {
    const adjacency = new Map();
    visibleEdges.forEach((edge) => {
      if (!adjacency.has(edge.source)) adjacency.set(edge.source, []);
      if (!adjacency.has(edge.target)) adjacency.set(edge.target, []);
      adjacency.get(edge.source).push({ next: edge.target, edge });
      adjacency.get(edge.target).push({ next: edge.source, edge });
    });
    const queue = [sourceId];
    const visited = new Set(queue);
    const previous = new Map();
    while (queue.length) {
      const current = queue.shift();
      if (current === targetId) break;
      for (const step of adjacency.get(current) || []) {
        if (visited.has(step.next)) continue;
        visited.add(step.next);
        previous.set(step.next, { from: current, edge: step.edge });
        queue.push(step.next);
      }
    }
    if (!visited.has(targetId)) return [];
    const edges = [];
    let cursor = targetId;
    while (cursor !== sourceId) {
      const step = previous.get(cursor);
      if (!step) return [];
      edges.unshift(step.edge);
      cursor = step.from;
    }
    return edges;
  }

  function buildQuestionSubgraph() {
    const selected = viewerState.questionNodeIds.filter((id) => nodeById.has(id));
    const collectedEdges = [];
    if (selected.length === 1) {
      visibleEdges.filter((edge) => edge.source === selected[0] || edge.target === selected[0]).slice(0, 40).forEach((edge) => collectedEdges.push(edge));
    } else {
      for (let index = 1; index < selected.length; index += 1) {
        shortestVisiblePath(selected[0], selected[index]).forEach((edge) => collectedEdges.push(edge));
      }
    }
    const uniqueEdges = [...new Map(collectedEdges.map((edge, index) => {
      const key = `${edge.source}||${edge.target}||${edge.kind || ""}||${edge.role_type || ""}`;
      return [key, { ...edge, id: String(edge.id || `visible-edge-${index + 1}`) }];
    })).values()].slice(0, 100);
    const nodeIds = new Set(selected);
    uniqueEdges.forEach((edge) => {
      nodeIds.add(edge.source);
      nodeIds.add(edge.target);
    });
    const nodes = [...nodeIds].map((id) => nodeById.get(id)).filter(Boolean).slice(0, 60).map((node) => ({
      id: node.id,
      label: node.label || node.id,
      kind: node.kind,
    }));
    const allowedIds = new Set(nodes.map((node) => node.id));
    const edges = uniqueEdges.filter((edge) => allowedIds.has(edge.source) && allowedIds.has(edge.target)).map((edge) => ({
      id: edge.id,
      source: edge.source,
      source_label: nodeById.get(edge.source)?.label || edge.source,
      target: edge.target,
      target_label: nodeById.get(edge.target)?.label || edge.target,
      kind: edge.kind,
      phrase: edge.phrase || edge.role_type || "is linked to",
      confidence: edge.confidence || "",
      evidence: edge.evidence,
      evidence_items: edge.evidence_items,
    }));
    return { nodes, edges };
  }

  function renderQuestionAnswer(payload) {
    const edges = new Map((Array.isArray(payload?.context?.edges) ? payload.context.edges : []).map((edge) => [String(edge.id), edge]));
    const evidence = new Map((Array.isArray(payload?.context?.evidence) ? payload.context.evidence : []).map((item) => [String(item.id), item]));
    const claims = Array.isArray(payload?.claims) ? payload.claims : [];
    questionResultEl.innerHTML = `
      <div class="analysis-text">${escapeHtml(payload?.answer || "No answer returned.").replaceAll("\n", "<br>")}</div>
      ${claims.map((claim) => `
        <div class="analysis-claim">
          <div class="analysis-claim-text">${escapeHtml(claim.text || "")}</div>
          ${(Array.isArray(claim.edge_ids) ? claim.edge_ids : []).map((id) => edges.get(String(id))).filter(Boolean).map((edge) => `
            <div class="question-citation">
              ${escapeHtml(edge.source_label || edge.source_id)} ${escapeHtml(edge.phrase || "is linked to")} ${escapeHtml(edge.target_label || edge.target_id)} · ${escapeHtml(edge.confidence || "confidence unavailable")}
              ${(Array.isArray(edge.evidence_ids) ? edge.evidence_ids : []).map((id) => evidence.get(String(id))).filter(Boolean).map((item) => {
                const url = evidenceActionUrl(item);
                return url ? ` · <a href="${escapeHtml(url)}" target="_blank" rel="noreferrer">${escapeHtml(evidenceDisplayTitle(item))}</a>` : "";
              }).join("")}
            </div>
          `).join("")}
        </div>
      `).join("")}
    `;
  }

  async function askSelectedSubgraph() {
    const question = String(questionInputEl.value || "").trim();
    if (!question) throw new Error("Enter a question first.");
    const subgraph = buildQuestionSubgraph();
    if (!subgraph.edges.length) throw new Error("The selected nodes have no visible connecting path.");
    questionSubmitEl.disabled = true;
    questionResultEl.innerHTML = '<div class="analysis-empty">Reading the selected visible relationships...</div>';
    try {
      const response = await fetch(graphFunctionUrl(GRAPH_QUESTION_URL), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ graph: currentGraphKey, question, subgraph }),
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(payload.error || `Subgraph question failed (${response.status})`);
      renderQuestionAnswer(payload);
    } finally {
      questionSubmitEl.disabled = false;
    }
  }

  function loadExternalAsset(tagName, url) {
    const attribute = tagName === "link" ? "href" : "src";
    const existing = document.querySelector(`${tagName}[${attribute}="${url}"]`);
    if (existing?.dataset.loaded === "true") return Promise.resolve();
    return new Promise((resolve, reject) => {
      const element = existing || document.createElement(tagName);
      element.addEventListener("load", () => {
        element.dataset.loaded = "true";
        resolve();
      }, { once: true });
      element.addEventListener("error", () => reject(new Error(`Failed to load ${url}`)), { once: true });
      if (existing) return;
      element[attribute] = url;
      if (tagName === "link") element.rel = "stylesheet";
      document.head.appendChild(element);
    });
  }

  function ensureLeafletLoaded() {
    if (window.L) return Promise.resolve();
    if (!leafletLoadingPromise) {
      leafletLoadingPromise = Promise.all([
        loadExternalAsset("link", LEAFLET_CSS_URL),
        loadExternalAsset("script", LEAFLET_SCRIPT_URL),
      ]).then(() => undefined).finally(() => {
        leafletLoadingPromise = null;
      });
    }
    return leafletLoadingPromise;
  }

  async function ensureAddressMap() {
    if (addressMap) return;
    await ensureLeafletLoaded();
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
    setSidebarTab("map");
    await ensureAddressMap();
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
    }, { fit: !options?.preserveViewport });
    renderExtraTreeSummary();
    renderScorePanel();
    if (document.querySelector('.sidebar-pane[data-pane="resolve"]')?.classList.contains("active")) renderResolutionPanel();
    renderQuestionSelection();

    if (document.querySelector('.sidebar-pane[data-pane="map"]')?.classList.contains("active") && addressMap) {
      openMapView().catch(() => {});
    }

    const extraSuffix = viewerState.extraRootIds.length ? ` + ${viewerState.extraRootIds.length} added tree${viewerState.extraRootIds.length === 1 ? "" : "s"}` : "";
    const hiddenEnrichmentCount = hiddenLatestEnrichmentNodeIds().size;
    const enrichmentSuffix = hiddenEnrichmentCount ? ` / ${hiddenEnrichmentCount.toLocaleString()} latest-round node${hiddenEnrichmentCount === 1 ? "" : "s"} hidden` : "";
    const distanceSuffix = viewerState.maxFocalDistanceMetres === null ? "" : ` / up to ${viewerState.maxFocalDistanceMetres} m`;
    statsEl.textContent = `showing ${visibleNodes.length} nodes, ${visibleEdges.length} edges${extraSuffix}${enrichmentSuffix}${distanceSuffix}`;
  }

  function bindUiEvents() {
    modeViewerButton?.addEventListener("click", () => setAppMode("viewer"));
    modeBuilderButton?.addEventListener("click", () => setAppMode("builder"));
    builderFormEl?.addEventListener("submit", (event) => {
      event.preventDefault();
      submitBuilderJob().catch((error) => {
        setBuilderFeedback(error.message || "The investigation brief could not be interpreted.", "error");
        setBuilderStatus(error.message || "Case planning failed to start.", true);
      });
    });
    caseDirectButton?.addEventListener("click", startDirectContract);
    caseRunButton?.addEventListener("click", () => {
      runPlannedCase().catch((error) => {
        caseRunButton.disabled = false;
        setBuilderFeedback(error.message || "Discovery could not start.", "error");
        setBuilderStatus(error.message || "Case discovery failed to start.", true);
      });
    });
    caseResetButton?.addEventListener("click", resetCaseDesk);
    caseNearbyEnabledInput?.addEventListener("change", () => updateNearbyControls());
    caseNearbyCentreInput?.addEventListener("input", () => scheduleNearbyPreview());
    caseNearbyRadiusInput?.addEventListener("input", updateNearbyRadiusLabel);
    caseNearbyRadiusInput?.addEventListener("change", () => scheduleNearbyPreview(0));
    caseMaxAddressesInput?.addEventListener("change", () => {
      if (caseNearbyEnabledInput?.checked) scheduleNearbyPreview(0);
    });
    caseProgressOpenEl?.addEventListener("click", () => setRunLogOpen(true));
    caseCancelButton?.addEventListener("click", () => {
      cancelCurrentTask().catch((error) => setBuilderStatus(error.message || "The task could not be cancelled.", true));
    });
    caseTaskListEl?.addEventListener("click", (event) => {
      const clearButton = event.target.closest(".case-task-clear");
      if (clearButton) {
        clearRecentTask(clearButton.dataset.taskId, clearButton.dataset.taskTitle)
          .catch((error) => setBuilderStatus(error.message || "The task could not be cleared.", true));
        return;
      }
      const taskButton = event.target.closest(".case-task-select");
      if (!taskButton) return;
      showRecentTask(taskButton.dataset.taskId).catch((error) => setBuilderStatus(error.message || "The task could not be opened.", true));
    });
    runLogCloseButton?.addEventListener("click", () => setRunLogOpen(false));
    runLogBackdropEl?.addEventListener("click", () => setRunLogOpen(false));
    caseAddInputButton?.addEventListener("click", () => {
      casePlanInputsEl.insertAdjacentHTML("beforeend", renderCaseInput());
      casePlanInputsEl.querySelector(".case-input:last-child .case-input-value")?.focus();
    });
    casePlanInputsEl?.addEventListener("click", (event) => {
      const removeButton = event.target.closest(".case-input-remove");
      if (removeButton) {
        removeButton.closest(".case-input")?.remove();
        syncNearbyCentreFromSeeds();
      }
    });
    casePlanInputsEl?.addEventListener("input", syncNearbyCentreFromSeeds);
    casePlanInputsEl?.addEventListener("change", syncNearbyCentreFromSeeds);
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
    focalDistanceRangeEl?.addEventListener("input", renderFocalDistanceValue);
    focalDistanceRangeEl?.addEventListener("change", () => {
      const selected = Number(focalDistanceRangeEl.value);
      const maximum = Number(focalDistanceFilterEl?.dataset.maximum);
      viewerState.maxFocalDistanceMetres = selected >= maximum ? null : selected;
      renderFocalDistanceValue();
      applyViewerState();
    });
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
    resolutionPanelEl?.addEventListener("click", async (event) => {
      const button = event.target.closest("[data-resolution-index]");
      if (!button) return;
      const action = (resolutionPanelEl._actions || [])[Number(button.dataset.resolutionIndex || -1)];
      if (!action) return;
      button.disabled = true;
      try {
        if (action.type === "merge") {
          await persistMergeOverride({ ...action, operation: "add" });
        } else if (action.type === "reject") {
          await persistResolutionDecision({ ...action, operation: "add" });
        } else if (action.type === "undo_merge") {
          await persistMergeOverride({ ...action, operation: "remove" });
        } else if (action.type === "undo_reject") {
          await persistResolutionDecision({ ...action, operation: "remove" });
        }
      } catch (error) {
        console.error(error);
        window.alert("Resolution decision could not be saved.");
      } finally {
        button.disabled = false;
      }
    });
    questionSubmitEl?.addEventListener("click", () => {
      askSelectedSubgraph().catch((error) => {
        questionResultEl.innerHTML = `<div class="analysis-error">${escapeHtml(error.message || "Question failed.")}</div>`;
      });
    });
    questionClearEl?.addEventListener("click", () => {
      viewerState.questionNodeIds = [];
      questionInputEl.value = "";
      questionResultEl.innerHTML = "";
      renderQuestionSelection();
    });
    sidebarTabEls.forEach((element) => {
      element.addEventListener("click", () => {
        const tabName = String(element.dataset.tab || "legend");
        setSidebarTab(tabName);
        if (tabName === "resolve") renderResolutionPanel();
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
      } else if (action.type === "edge_details") {
        openEdgeEvidenceView(action.edge);
      } else if (action.type === "node_claims") {
        const node = nodeById.get(action.nodeId);
        if (node) openNodeAttributionView(node);
      } else if (action.type === "question_add") {
        viewerState.questionNodeIds = [...viewerState.questionNodeIds, action.nodeId].slice(0, 8);
        renderQuestionSelection();
      } else if (action.type === "question_remove") {
        viewerState.questionNodeIds = viewerState.questionNodeIds.filter((id) => id !== action.nodeId);
        renderQuestionSelection();
      } else if (action.type === "question_open") {
        setSidebarTab("ask");
        toggleSidebar(true);
        questionInputEl.focus();
      } else if (action.type === "enrichment_round_show" || action.type === "enrichment_round_hide") {
        viewerState.showLatestEnrichmentRound = action.type === "enrichment_round_show";
        applyViewerState();
      } else if (action.type === "enrichment_round_run") {
        runOneEnrichmentRound(action.nodeId).catch((error) => {
          currentEnrichmentStatus = "failed";
          graphExpansionStatusEl.classList.remove("hidden");
          graphExpansionStatusEl.dataset.state = "error";
          graphExpansionProgressEl.value = 0;
          graphExpansionLabelEl.textContent = error.message || "The expansion could not start.";
        });
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
          await persistMergeOverride({ ...action, operation: "add", leaderKey, leaderLabel });
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
      } else if (action.type === "seed_add" || action.type === "seed_remove") {
        const promoting = action.type === "seed_add";
        const confirmed = window.confirm(`${promoting ? "Promote" : "Restore"} "${action.nodeLabel}" ${promoting ? "to a seed" : "as a person"}?`);
        if (!confirmed) return;
        try {
          await persistSeedOverride({ ...action, operation: promoting ? "add" : "remove" });
        } catch (error) {
          console.error(error);
          window.alert("Seed status could not be saved.");
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
    configureFocalDistanceFilter();
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
      onPointerMove(event) {
        positionTooltip(event);
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
    restoreGraphRefreshState();
  }

  boot().catch((error) => {
    console.error(error);
    scorePanelEl.innerHTML = '<div class="analysis-error">Graph viewer failed to initialize.</div>';
  });
}());
