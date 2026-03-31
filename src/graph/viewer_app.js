(function () {
  const rawMainNodes = {nodes_json};
  const rawMainEdges = {edges_json}.filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed");
  const LOW_CONFIDENCE_DATA_URL = "graph-data-low-confidence.json";
  const ANALYZE_CONNECTION_URL = "/.netlify/functions/analyze-connection";
  const EVIDENCE_FILE_URL = "/.netlify/functions/evidence-file";

  const COLORS = {
    amber: 0xd4a017,
    blue: 0x58a6ff,
    green: 0x3fb950,
    purple: 0xb382f0,
    slate: 0x64748b,
    red: 0xff2222,
    white: 0xd0d4dc,
  };

  const container = document.getElementById("graph");
  const tooltip = document.getElementById("tooltip");
  const searchInput = document.getElementById("search");
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
  const ADDRESS_COORDINATES_URL = "address-coordinates.json";

  let showIdentitiesInput;
  let showCompaniesInput;
  let showCharitiesInput;
  let showOrganisationsInput;
  let showPeopleInput;
  let showAddressesInput;
  let showLowConfidenceInput;

  let baseNodes = rawMainNodes.slice();
  let baseEdges = rawMainEdges.slice();
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

  const viewerState = {
    searchQuery: "",
    focusedNodeIds: new Set(),
    hiddenTypes: new Set(),
    showLowConfidence: false,
    showIndirectOnly: false,
    analysisNodeIds: [],
  };

  const measureCtx = document.createElement("canvas").getContext("2d");

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function normalizeNodeKind(node) {
    if (!node) return "";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "charity") return "charity";
    if (node.kind === "organisation" && String(node.registry_type || "").toLowerCase() === "company") return "company";
    return node.kind;
  }

  function nodeTypeLabel(node) {
    const kind = normalizeNodeKind(node);
    if (kind === "seed_alias") return "identity";
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

  function isFilterableType(typeKey) {
    return ["identity", "company", "charity", "organisation", "address", "person"].includes(typeKey);
  }

  function nodeMatchesQuery(node, query) {
    if (!query) return false;
    const q = query.toLowerCase();
    if (String(node.label || "").toLowerCase().includes(q)) return true;
    return (Array.isArray(node.aliases) ? node.aliases : []).some((alias) => String(alias || "").toLowerCase().includes(q));
  }

  function nodeColorValue(node) {
    const kind = normalizeNodeKind(node);
    if (node.sanctioned) return COLORS.red;
    if (kind === "seed_alias") return COLORS.amber;
    if (kind === "charity" || kind === "company" || kind === "organisation") return COLORS.green;
    if (kind === "address") return COLORS.purple;
    return COLORS.blue;
  }

  function nodeRankScore(node) {
    const seedFlag = node.kind === "seed_alias" ? 2.8 : node.kind === "person" ? 1.4 : 0;
    const sanctionedFlag = node.sanctioned ? 3.5 : 0;
    return (Number(node.score || 0) * 4.5)
      + (Number(node.role_count || 0) * 0.8)
      + (Number(node.org_count || 0) * 0.45)
      + seedFlag
      + sanctionedFlag;
  }

  function edgeColorValue(edge) {
    if (edge.is_low_confidence) return COLORS.amber;
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
      ["show-identities", "Identity", true],
      ["show-charities", "Charity", true],
      ["show-companies", "Company", true],
      ["show-organisations", "Organisation", true],
      ["show-addresses", "Address", true],
      ["show-people", "Person", true],
      ["show-low-confidence", "Low confidence overlay", false],
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
    showOrganisationsInput = document.getElementById("show-organisations");
    showPeopleInput = document.getElementById("show-people");
    showAddressesInput = document.getElementById("show-addresses");
    showLowConfidenceInput = document.getElementById("show-low-confidence");
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
      allEdges.map((edge) => [edge.source, edge.target].sort().join("||")),
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
    allNodes = baseNodes.filter((node) => node.kind !== "seed").map((node) => ({ ...node }));
    allEdges = baseEdges.filter((edge) => edge.kind !== "shared_org" && edge.kind !== "cross_seed").map((edge) => ({ ...edge }));
    if (!viewerState.showLowConfidence || !lowConfidenceLoaded) {
      rebuildIndexes();
      return;
    }
    const activeLowNodeIds = new Set();
    const activeLowEdgeIds = new Set();
    lowConfidenceNodes.forEach((node) => {
      if (node.kind === "organisation") activeLowNodeIds.add(node.id);
    });
    lowConfidenceEdges.forEach((edge) => {
      if (mainNodeIds.has(edge.source) || mainNodeIds.has(edge.target) || activeLowNodeIds.has(edge.source) || activeLowNodeIds.has(edge.target)) {
        activeLowEdgeIds.add(edge.id);
        if (!mainNodeIds.has(edge.source)) activeLowNodeIds.add(edge.source);
        if (!mainNodeIds.has(edge.target)) activeLowNodeIds.add(edge.target);
      }
    });
    const matchedLowSearchIds = new Set(
      lowConfidenceNodes.filter((node) => nodeMatchesQuery(node, viewerState.searchQuery)).map((node) => node.id),
    );
    matchedLowSearchIds.forEach((nodeId) => {
      activeLowNodeIds.add(nodeId);
      (lowConfidenceEdgesByNodeId.get(nodeId) || []).forEach((edge) => {
        activeLowEdgeIds.add(edge.id);
        if (!mainNodeIds.has(edge.source)) activeLowNodeIds.add(edge.source);
        if (!mainNodeIds.has(edge.target)) activeLowNodeIds.add(edge.target);
      });
    });
    allNodes.push(...lowConfidenceNodes.filter((node) => activeLowNodeIds.has(node.id)).map((node) => ({ ...node })));
    allEdges.push(...lowConfidenceEdges.filter((edge) => activeLowEdgeIds.has(edge.id)).map((edge) => ({ ...edge })));
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

  function syncHiddenTypeState() {
    viewerState.hiddenTypes = new Set([
      showIdentitiesInput?.checked ? null : "identity",
      showCompaniesInput?.checked ? null : "company",
      showCharitiesInput?.checked ? null : "charity",
      showOrganisationsInput?.checked ? null : "organisation",
      showPeopleInput?.checked ? null : "person",
      showAddressesInput?.checked ? null : "address",
    ].filter(Boolean));
    viewerState.showLowConfidence = !!showLowConfidenceInput?.checked;
    viewerState.showIndirectOnly = !!indirectOnlyInput?.checked;
  }

  function getMatchedNodeIds(query) {
    if (!query) return new Set();
    return new Set(
      allNodes
        .filter((node) => node.kind !== "seed" && nodeMatchesQuery(node, query))
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

  function hiddenConnectionStepLine(edge) {
    if (edge.tooltip) return edge.tooltip;
    const source = nodeById.get(edge.source);
    const target = nodeById.get(edge.target);
    return `${source?.label || edge.source} is linked to ${target?.label || edge.target}`;
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
      edge.kind !== "hidden_connection"
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

  function findBridgeConnections(startId) {
    const startNode = nodeById.get(startId);
    if (!isBridgeStartNode(startNode)) return [];
    const connections = new Map();
    const hiddenQueue = [];
    const visited = new Set([startId]);
    (edgesByNodeId.get(startId) || []).forEach((edge) => {
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
    if (!visibleIds.size) return new Set();
    const filteredIds = new Set(
      [...visibleIds].filter((id) => {
        const node = nodeById.get(id);
        if (!node || node.kind === "seed") return false;
        if (node.is_low_confidence && !viewerState.showLowConfidence) return false;
        const typeKey = nodeTypeKey(node);
        if (!isFilterableType(typeKey)) return true;
        return !viewerState.hiddenTypes.has(typeKey);
      }),
    );
    if (!rootIds.size || viewerState.showIndirectOnly) return filteredIds;
    let changed = true;
    while (changed) {
      changed = false;
      const degree = new Map();
      filteredIds.forEach((id) => degree.set(id, 0));
      allEdges.forEach((edge) => {
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

  function buildSearchProjection(matchedIds) {
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
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), matchedIds, { keepDisconnectedIdentities: true });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (viewerState.showLowConfidence || !edge.is_low_confidence));
    return { rootIds: matchedIds, visibleIds: filteredVisibleIds, edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)) };
  }

  function buildIndirectOrgProjection() {
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
    const filteredVisibleIds = applyTypeFilters(expandRelatedAddresses(visibleIds), qualifyingOrgIds, { keepDisconnectedIdentities: true });
    const edgeIds = allEdges.filter((edge) => filteredVisibleIds.has(edge.source) && filteredVisibleIds.has(edge.target) && (viewerState.showLowConfidence || !edge.is_low_confidence));
    return { rootIds: qualifyingOrgIds, visibleIds: filteredVisibleIds, edgeIds: edgeIds.concat(deriveVisibleBridgeEdges(filteredVisibleIds)) };
  }

  function projectVisibleGraph() {
    const matchedIds = getMatchedNodeIds(viewerState.searchQuery);
    const rootIds = matchedIds.size ? matchedIds : new Set(viewerState.focusedNodeIds);
    if (matchedIds.size) return buildSearchProjection(matchedIds);
    if (viewerState.showIndirectOnly) return buildIndirectOrgProjection();
    if (!rootIds.size) {
      const visibleIds = applyTypeFilters(expandRelatedAddresses(new Set(allNodes.filter((node) => node.kind !== "seed").map((node) => node.id))), new Set());
      const edgeIds = allEdges.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target) && (viewerState.showLowConfidence || !edge.is_low_confidence));
      return { rootIds, visibleIds, edgeIds };
    }
    const subgraph = collectConnectedSubgraph(rootIds);
    const visibleIds = applyTypeFilters(expandRelatedAddresses(new Set(subgraph.reachableIds)), rootIds);
    const edgeIds = allEdges.filter((edge) => visibleIds.has(edge.source) && visibleIds.has(edge.target) && (viewerState.showLowConfidence || !edge.is_low_confidence));
    return { rootIds, visibleIds, edgeIds };
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

  function avgNeighborX(node, visibleEdgeSet) {
    const xs = [];
    (edgesByNodeId.get(node.id) || []).forEach((edge) => {
      if (!visibleEdgeSet.has(edge)) return;
      const other = nodeById.get(edge.source === node.id ? edge.target : edge.source);
      if (other && other._visible && other.x != null && other.lane !== node.lane) xs.push(other.x);
    });
    if (!xs.length) return (container.clientWidth || window.innerWidth) / 2;
    return xs.reduce((sum, value) => sum + value, 0) / xs.length;
  }

  function layoutVisibleNodes(rootIds) {
    const width = container.clientWidth || window.innerWidth;
    const visibleEdgeSet = new Set(visibleEdges);
    let curY = 72;
    [1, 2, 3, 4].forEach((lane) => {
      const laneNodes = visibleNodes.filter((node) => Number(node.lane || 0) === lane);
      if (lane === 1 || lane === 4) {
        laneNodes.sort((left, right) => {
          const scoreDiff = nodeRankScore(right) - nodeRankScore(left);
          if (scoreDiff !== 0) return scoreDiff;
          return avgNeighborX(left, visibleEdgeSet) - avgNeighborX(right, visibleEdgeSet);
        });
      } else {
        laneNodes.sort((left, right) => avgNeighborX(left, visibleEdgeSet) - avgNeighborX(right, visibleEdgeSet));
      }
      const spacing = 16;
      const rowGap = 18;
      const pad = 18;
      const usableMin = pad;
      const usableMax = width - pad;
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
        const rowW = row.reduce((sum, node) => sum + pillWidth(node), 0) + spacing * (row.length - 1);
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
  }

  function ensureSceneMetadata() {
    visibleNodes.forEach((node) => {
      node._colorValue = nodeColorValue(node);
      node._rankScore = nodeRankScore(node);
      node._fontSize = fontSize(node);
    });
    visibleEdges.forEach((edge) => {
      edge._sourceNode = nodeById.get(edge.source);
      edge._targetNode = nodeById.get(edge.target);
      edge._colorValue = edgeColorValue(edge);
    });
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
    if (!node?.is_low_confidence) {
      return Array.isArray(node?.tooltip_lines) ? node.tooltip_lines.slice() : [node?.label || "Node"];
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
    return lines;
  }

  function tooltipLinesForEdge(edge) {
    if (!edge?.is_low_confidence) {
      return Array.isArray(edge?.tooltip_lines) ? edge.tooltip_lines.slice() : [edge?.tooltip || "link"];
    }
    const sourceLabel = String(nodeById.get(edge?.source)?.label || edge?.source || "Source");
    const targetLabel = String(nodeById.get(edge?.target)?.label || edge?.target || "Target");
    const rawType = String(edge?.role_label || edge?.role_type || "").trim();
    const baseType = rawType.replace(/\s*\([^)]*\)\s*$/, "").toLowerCase();
    const titleMatch = rawType.match(/\(([^)]+)\)\s*$/);
    const title = String(titleMatch?.[1] || "").trim();
    const subject = title && !sourceLabel.toLowerCase().startsWith(`${title.toLowerCase()} `)
      ? `${title} ${sourceLabel}`
      : sourceLabel;
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
  }

  function renderScorePanel() {
    const rankedNodes = visibleNodes
      .filter((node) => nodeRankScore(node) > 0 && (node.lane === 1 || node.lane === 4))
      .sort((left, right) => nodeRankScore(right) - nodeRankScore(left))
      .slice(0, 12);
    scorePanelEl.innerHTML = rankedNodes.length
      ? `
        <h2>Top ranked on screen</h2>
        <div class="score-list">
          ${rankedNodes.map((node) => `
            <div class="score-item">
              <div class="score-item-title">
                <strong>${escapeHtml(node.label || "Unknown")}</strong>
                <span>${nodeRankScore(node).toFixed(2)}</span>
              </div>
              <div class="score-item-meta">${Number(node.org_count || 0)} orgs, ${Number(node.role_count || 0)} roles</div>
            </div>
          `).join("")}
        </div>
      `
      : '<div class="score-empty">No scored identity or person nodes are currently visible.</div>';
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

  function evidenceLabelForEdge(edge) {
    const firstLine = tooltipLinesForEdge(edge)[0];
    const summary = plainText(firstLine);
    return summary ? `Evidence for: ${summary}` : "Evidence";
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
        url: `https://register-of-charities.charitycommission.gov.uk/charity-search/-/charity-details/${encodeURIComponent(registryNumber)}`,
      };
    }
    return null;
  }

  function openContextMenu(node, event) {
    event.preventDefault();
    event.stopPropagation();
    hideTooltip();
    const actions = [
      registryActionForNode(node),
      viewerState.focusedNodeIds.has(node.id) ? { label: "Clear focus", type: "focus_clear" } : null,
      viewerState.analysisNodeIds.includes(node.id)
        ? { label: "Remove from connection analysis", type: "analysis_remove", nodeId: node.id }
        : (viewerState.analysisNodeIds.length < 2 ? { label: "Add to connection analysis", type: "analysis_add", nodeId: node.id } : null),
      viewerState.analysisNodeIds.length === 2 ? { label: "Analyze connection", type: "analysis_run" } : null,
      viewerState.analysisNodeIds.length === 2 ? { label: "Clear analysis selection", type: "analysis_clear" } : null,
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
    const sourceNode = nodeById.get(edge.source);
    const targetNode = nodeById.get(edge.target);
    const actions = evidenceActionsForEdge(edge);
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
              .join(" · ");
            return `
              <div class="analysis-claim">
                <div class="analysis-claim-text">${index + 1}. ${escapeHtml(claim.text || "")}</div>
                <div class="analysis-claim-evidence">${links}</div>
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

    const response = await fetch(ANALYZE_CONNECTION_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_id: sourceId, target_id: targetId }),
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

  async function applyViewerState() {
    syncHiddenTypeState();
    rebuildActiveGraph();
    const projection = projectVisibleGraph();
    allNodes.forEach((node) => {
      node._visible = projection.visibleIds.has(node.id);
    });
    visibleNodes = allNodes.filter((node) => node._visible);
    visibleEdges = projection.edgeIds;
    layoutVisibleNodes(projection.rootIds);
    ensureSceneMetadata();
    renderer.setGraph({
      nodes: visibleNodes,
      edges: visibleEdges,
      rootIds: [...projection.rootIds],
    });
    renderer.fitToNodes(visibleNodes);
    renderScorePanel();

    if (document.querySelector('.sidebar-pane[data-pane="map"]')?.classList.contains("active") && addressMap) {
      openMapView().catch(() => {});
    }

    statsEl.textContent = `showing ${visibleNodes.length} nodes, ${visibleEdges.length} edges`;
  }

  function bindUiEvents() {
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
    [showIdentitiesInput, showCompaniesInput, showCharitiesInput, showOrganisationsInput, showPeopleInput, showAddressesInput, indirectOnlyInput]
      .forEach((input) => input.addEventListener("change", applyViewerState));
    showLowConfidenceInput.addEventListener("change", async () => {
      if (showLowConfidenceInput.checked) {
        const ok = await ensureLowConfidenceLoaded();
        if (!ok) showLowConfidenceInput.checked = false;
      }
      applyViewerState();
    });
    toggleSidebarButton.addEventListener("click", () => toggleSidebar());
    sidebarTabEls.forEach((element) => {
      element.addEventListener("click", () => {
        const tabName = String(element.dataset.tab || "legend");
        setSidebarTab(tabName);
        if (tabName === "map") {
          openMapView().catch(() => {});
        }
      });
    });
    contextMenuEl.addEventListener("click", (event) => {
      const button = event.target.closest(".context-menu-item");
      if (!button) return;
      const action = (contextMenuEl._actions || [])[Number(button.dataset.actionIndex || -1)];
      closeContextMenu();
      if (!action) return;
      if (action.type === "open_url" && action.url) {
        window.open(action.url, "_blank", "noopener,noreferrer");
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
    document.addEventListener("click", closeContextMenu);
    window.addEventListener("blur", closeContextMenu);
  }

  async function boot() {
    renderLegend();
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
      onClick(node) {
        if (!node) return;
        viewerState.focusedNodeIds = new Set([node.id]);
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
