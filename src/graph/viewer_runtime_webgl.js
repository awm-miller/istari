(function () {
  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function createWorldTransform(x, y, k) {
    return {
      x,
      y,
      k,
      applyX(worldX) {
        return (worldX * this.k) + this.x;
      },
      applyY(worldY) {
        return (worldY * this.k) + this.y;
      },
      invertX(screenX) {
        return (screenX - this.x) / this.k;
      },
      invertY(screenY) {
        return (screenY - this.y) / this.k;
      },
    };
  }

  function pillWidth(node) {
    return Number(node?._pillWidth || 56);
  }

  function pillHeight(node) {
    return Number(node?._pillHeight || 24);
  }

  function pillBounds(node) {
    const width = pillWidth(node);
    const height = pillHeight(node);
    return {
      x: (node.x || 0) - (width / 2),
      y: (node.y || 0) - (height / 2),
      width,
      height,
      radius: height / 2,
    };
  }

  function focusButtonBounds(node) {
    if (node.kind === "seed") return null;
    const width = pillWidth(node);
    const height = pillHeight(node);
    return {
      cx: (node.x || 0) + (width / 2) - 14,
      cy: node.y || 0,
      r: 8,
    };
  }

  function badgeSpec(node) {
    const registryType = String(node?.registry_type || "").toLowerCase();
    if (node?.kind === "organisation" && registryType === "charity") return { fill: 0x3fb950, stroke: 0xffffff, icon: "heart" };
    if (node?.kind === "organisation" && registryType === "company") return { fill: 0x3fb950, stroke: 0xffffff, icon: "building" };
    return null;
  }

  function isLowConfidenceDocumentNode(node) {
    return !!node
      && !!node.is_low_confidence
      && node.kind === "organisation"
      && String(node.registry_type || "").toLowerCase() === "other"
      && !!node.low_confidence_expandable;
  }

  function lowConfidenceColor(node) {
    return String(node?.low_confidence_category || "") === "unresolved_org"
      ? 0xff5fbf
      : 0xfacc15;
  }

  function nodeStrokeWidth(node) {
    if (node._focused) return 2.8;
    if (node.sanctioned) return 3.4;
    if (node.egypt_judgment_hit) return 3.2;
    if (node.adverse_media_hit) return 3.0;
    if (node.is_low_confidence) return 1.4;
    return 1.2;
  }

  function nodeFillAlpha(node) {
    if (node._batchSelected) return 0.34;
    if (node._focused) return 0.28;
    if (node.is_low_confidence) return 0.14;
    if (node.sanctioned) return 0.48;
    if (node.egypt_judgment_hit) return 0.28;
    if (node.adverse_media_hit) return 0.24;
    return 0.18;
  }

  function pillEdgePoint(node, towardsX, towardsY) {
    const bounds = pillBounds(node);
    const cx = node.x || 0;
    const cy = node.y || 0;
    const dx = towardsX - cx;
    const dy = towardsY - cy;
    if (dx === 0 && dy === 0) return { x: cx, y: cy };
    const halfWidth = bounds.width / 2;
    const halfHeight = bounds.height / 2;
    const scale = 1 / Math.max(Math.abs(dx) / halfWidth, Math.abs(dy) / halfHeight);
    return { x: cx + (dx * scale), y: cy + (dy * scale) };
  }

  function edgeEndpoints(edge) {
    const source = edge?._sourceNode;
    const target = edge?._targetNode;
    if (!source || !target) return null;
    const start = pillEdgePoint(source, target.x || 0, target.y || 0);
    const end = pillEdgePoint(target, source.x || 0, source.y || 0);
    return { start, end };
  }

  function drawDashedLine(graphics, x1, y1, x2, y2, dashLength, gapLength) {
    const dx = x2 - x1;
    const dy = y2 - y1;
    const length = Math.sqrt((dx * dx) + (dy * dy));
    if (!length) return;
    const ux = dx / length;
    const uy = dy / length;
    let position = 0;
    while (position < length) {
      const dashEnd = Math.min(position + dashLength, length);
      graphics.moveTo(x1 + (ux * position), y1 + (uy * position));
      graphics.lineTo(x1 + (ux * dashEnd), y1 + (uy * dashEnd));
      position += dashLength + gapLength;
    }
  }

  function capsuleOutlinePoints(bounds, arcSteps = 10) {
    const radius = bounds.height / 2;
    const leftCx = bounds.x + radius;
    const rightCx = bounds.x + bounds.width - radius;
    const cy = bounds.y + radius;
    const points = [
      { x: bounds.x + radius, y: bounds.y },
      { x: bounds.x + bounds.width - radius, y: bounds.y },
    ];
    for (let step = 1; step <= arcSteps; step += 1) {
      const angle = (-Math.PI / 2) + ((step / arcSteps) * Math.PI);
      points.push({ x: rightCx + (radius * Math.cos(angle)), y: cy + (radius * Math.sin(angle)) });
    }
    points.push(
      { x: bounds.x + bounds.width - radius, y: bounds.y + bounds.height },
      { x: bounds.x + radius, y: bounds.y + bounds.height },
    );
    for (let step = 1; step <= arcSteps; step += 1) {
      const angle = (Math.PI / 2) + ((step / arcSteps) * Math.PI);
      points.push({ x: leftCx + (radius * Math.cos(angle)), y: cy + (radius * Math.sin(angle)) });
    }
    return points;
  }

  function drawDashedPolyline(graphics, points, dashLength, gapLength) {
    if (points.length < 2) return;
    const closedPoints = [...points, points[0]];
    for (let index = 0; index < closedPoints.length - 1; index += 1) {
      const start = closedPoints[index];
      const end = closedPoints[index + 1];
      const dx = end.x - start.x;
      const dy = end.y - start.y;
      const length = Math.sqrt((dx * dx) + (dy * dy));
      if (!length) continue;
      const ux = dx / length;
      const uy = dy / length;
      let position = 0;
      while (position < length) {
        const dashEnd = Math.min(position + dashLength, length);
        graphics.moveTo(start.x + (ux * position), start.y + (uy * position));
        graphics.lineTo(start.x + (ux * dashEnd), start.y + (uy * dashEnd));
        position += dashLength + gapLength;
      }
    }
  }

  function drawDashedCapsuleBorder(graphics, bounds, color, width) {
    drawDashedPolyline(graphics, capsuleOutlinePoints(bounds), 6, 4);
    graphics.stroke({ color, width, alpha: 1 });
  }

  function drawSearchGlyph(graphics, cx, cy, color) {
    graphics.circle(cx - 1.5, cy - 1.5, 2.8);
    graphics.stroke({ color, width: 1.4, alpha: 1 });
    graphics.moveTo(cx + 1.2, cy + 1.2);
    graphics.lineTo(cx + 4.8, cy + 4.8);
    graphics.stroke({ color, width: 1.4, alpha: 1 });
  }

  function createGraphRenderer(container, options) {
    const host = document.createElement("div");
    host.className = "graph-stage";
    const labelLayer = document.createElement("div");
    labelLayer.className = "graph-label-layer";
    const labelWorld = document.createElement("div");
    labelWorld.className = "graph-label-world";
    labelLayer.appendChild(labelWorld);
    container.innerHTML = "";
    container.append(host, labelLayer);

    const app = new PIXI.Application();
    const world = new PIXI.Container();
    const edgeLayer = new PIXI.Graphics();
    const nodeLayer = new PIXI.Graphics();
    const overlayLayer = new PIXI.Graphics();
    const hoverLayer = new PIXI.Graphics();
    world.addChild(edgeLayer);
    world.addChild(nodeLayer);
    world.addChild(overlayLayer);
    world.addChild(hoverLayer);

    const focusElementTemplate = document.createElement("span");
    focusElementTemplate.className = "graph-node-focus";
    focusElementTemplate.setAttribute("aria-hidden", "true");
    const badgeElementTemplates = new Map([
      ["heart", '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 20s-6-3.9-6-8.2A3.8 3.8 0 0 1 12 9a3.8 3.8 0 0 1 6 2.8C18 16.1 12 20 12 20Z"></path></svg>'],
      ["building", '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 19V7.8L9 5v14M11 19V9h8v10M8 10.6h.01M8 13.6h.01M8 16.6h.01M15 12h.01M15 15h.01"></path></svg>'],
    ].map(([icon, markup]) => {
      const element = document.createElement("span");
      element.className = "graph-node-badge";
      element.innerHTML = markup;
      return [icon, element];
    }));

    let transform = createWorldTransform(0, 0, 1);
    let zoomBehavior = null;
    let sceneNodes = [];
    let sceneEdges = [];
    let sceneNodeByKey = new Map();
    let rootIds = new Set();
    let labelNodes = [];
    const labelElementBySceneKey = new Map();
    let hoveredNodeKey = "";
    let hoveredEdgeKey = "";
    let draggingNode = null;
    let draggingPointerId = null;
    let dragStartClient = null;
    let dragMoved = false;
    let suppressClickUntil = 0;

    function nodeSceneKey(node) {
      return String(node?._sceneKey || node?.id || "");
    }

    function edgeSceneKey(edge, index = 0) {
      return String(edge?._sceneKey || edge?.id || `${edge?.source || ""}:${edge?.target || ""}:${edge?.kind || "link"}:${index}`);
    }

    function syncWorldTransform(nextTransform) {
      transform = createWorldTransform(nextTransform.x, nextTransform.y, nextTransform.k);
      world.position.set(transform.x, transform.y);
      world.scale.set(transform.k, transform.k);
      labelWorld.style.transform = `translate(${transform.x}px, ${transform.y}px) scale(${transform.k})`;
      options.onTransform?.(transform);
    }

    async function init() {
      await app.init({
        resizeTo: host,
        backgroundAlpha: 0,
        antialias: true,
      });
      app.stage.addChild(world);
      host.appendChild(app.canvas);

      zoomBehavior = d3.zoom()
        .filter(() => !host.dataset.dragging)
        .scaleExtent([0.05, 6])
        .on("zoom", (event) => syncWorldTransform(event.transform));
      d3.select(host).call(zoomBehavior);
      d3.select(host).on("dblclick.zoom", null);

      host.addEventListener("pointerdown", handlePointerDown, true);
      host.addEventListener("pointermove", handlePointerMove);
      host.addEventListener("pointerup", handlePointerUp, true);
      host.addEventListener("pointercancel", handlePointerUp, true);
      host.addEventListener("mouseleave", handlePointerLeave);
      host.addEventListener("click", handleClick);
      host.addEventListener("contextmenu", handleContextMenu);
      host.addEventListener("dblclick", handleDoubleClick);
      syncWorldTransform(d3.zoomIdentity);
    }

    function destroy() {
      app.destroy(true, { children: true });
    }

    function labelCandidates() {
      return sceneNodes;
    }

    function labelClasses(node) {
      const classes = ["graph-node-label"];
      if (node._focused) classes.push("highlight");
      if (node._batchSelected) classes.push("selected");
      if (node.sanctioned) classes.push("sanctioned");
      if (node.egypt_judgment_hit) classes.push("egypt-judgment");
      if (node.adverse_media_hit) classes.push("adverse-media");
      if (node._hovered) classes.push("hovered");
      if (badgeSpec(node)) classes.push("has-badge");
      if (node.kind !== "seed") classes.push("has-focus");
      return classes.join(" ");
    }

    function createLabelElement(node) {
      const element = document.createElement("div");
      element.dataset.nodeId = String(node.id);
      const spec = badgeSpec(node);
      if (spec) element.appendChild(badgeElementTemplates.get(spec.icon).cloneNode(true));
      const textElement = document.createElement("span");
      textElement.className = "graph-node-text";
      textElement.textContent = String(node.label || "");
      element.appendChild(textElement);
      if (node.kind !== "seed") element.appendChild(focusElementTemplate.cloneNode(true));
      element.dataset.sceneKey = nodeSceneKey(node);
      labelElementBySceneKey.set(nodeSceneKey(node), element);
      return element;
    }

    function updateLabelElement(element, node) {
      element.className = labelClasses(node);
      const bounds = pillBounds(node);
      element.style.width = `${bounds.width}px`;
      element.style.height = `${bounds.height}px`;
      element.style.transform = `translate(${bounds.x}px, ${bounds.y}px)`;
      element.style.fontSize = `${Number(node._fontSize || 11)}px`;
      const spec = badgeSpec(node);
      if (spec) element.style.setProperty("--badge-color", `#${spec.fill.toString(16).padStart(6, "0")}`);
    }

    function updateLabels() {
      labelNodes = labelCandidates();
      const activeKeys = new Set(labelNodes.map(nodeSceneKey));
      labelElementBySceneKey.forEach((element, sceneKey) => {
        if (activeKeys.has(sceneKey)) return;
        element.remove();
        labelElementBySceneKey.delete(sceneKey);
      });
      const fragment = document.createDocumentFragment();
      labelNodes.forEach((node) => {
        const sceneKey = nodeSceneKey(node);
        let element = labelElementBySceneKey.get(sceneKey);
        if (!element) {
          element = createLabelElement(node);
          fragment.appendChild(element);
        }
        updateLabelElement(element, node);
      });
      if (fragment.childNodes.length) labelWorld.appendChild(fragment);
    }

    function drawHoveredNode() {
      hoverLayer.clear();
      const node = sceneNodeByKey.get(hoveredNodeKey);
      if (!node) return;
      const bounds = pillBounds(node);
      if (node.is_low_confidence) {
        drawDashedCapsuleBorder(hoverLayer, bounds, lowConfidenceColor(node), 2.2);
        return;
      }
      const strokeColor = node.sanctioned ? 0xff2222 : node.egypt_judgment_hit ? 0xff9800 : node.adverse_media_hit ? 0xff6a00 : node._colorValue;
      hoverLayer.roundRect(bounds.x, bounds.y, bounds.width, bounds.height, bounds.radius);
      hoverLayer.stroke({ color: strokeColor, width: nodeStrokeWidth(node) + 0.8, alpha: 1 });
      if (node.sanctioned && node.adverse_media_hit) {
        const inset = 2.4;
        hoverLayer.roundRect(
          bounds.x + inset,
          bounds.y + inset,
          Math.max(0, bounds.width - (inset * 2)),
          Math.max(0, bounds.height - (inset * 2)),
          Math.max(0, bounds.radius - inset),
        );
        hoverLayer.stroke({ color: 0xff6a00, width: 2.6, alpha: 1 });
      }
      if (node._lowConfidenceOnlyVisible) {
        drawDashedCapsuleBorder(hoverLayer, bounds, 0xfacc15, 2.2);
      }
    }

    function drawScene({ syncLabels = true } = {}) {
      edgeLayer.clear();
      nodeLayer.clear();
      overlayLayer.clear();

      const edgeGroups = new Map();
      sceneEdges.forEach((edge) => {
        const endpoints = edgeEndpoints(edge);
        if (!endpoints) return;
        const width = edge.kind === "hidden_connection" ? 1.8 : edge.kind === "alias" ? 2.5 : 1.4 + ((edge.weight || 0) * 1.5);
        const isFormer = edge.relationship_status === "former";
        const alpha = isFormer ? 0.56 : edge.is_low_confidence ? 0.72 : edge.kind === "address_link" ? 0.75 : 0.45;
        const dashed = isFormer || edge.kind === "hidden_connection" || edge.is_low_confidence;
        const key = `${edge._colorValue}:${width}:${alpha}:${dashed ? 1 : 0}`;
        if (!edgeGroups.has(key)) {
          edgeGroups.set(key, { color: edge._colorValue, width, alpha, dashed, lines: [] });
        }
        edgeGroups.get(key).lines.push(endpoints);
      });
      edgeGroups.forEach((group) => {
        group.lines.forEach(({ start, end }) => {
          if (group.dashed) drawDashedLine(edgeLayer, start.x, start.y, end.x, end.y, 8, 6);
          else {
            edgeLayer.moveTo(start.x, start.y);
            edgeLayer.lineTo(end.x, end.y);
          }
        });
        edgeLayer.stroke({ color: group.color, width: group.width, alpha: group.alpha });
      });

      const nodeFillGroups = new Map();
      const nodeStrokeGroups = new Map();
      sceneNodes.forEach((node) => {
        const bounds = pillBounds(node);
        const fillColor = node._colorValue;
        const strokeColor = node.sanctioned ? 0xff2222 : node.egypt_judgment_hit ? 0xff9800 : node.adverse_media_hit ? 0xff6a00 : fillColor;
        const hasCombinedSanctionAdverse = !!(node.sanctioned && node.adverse_media_hit);
        const fillAlpha = nodeFillAlpha(node);
        const fillKey = `${fillColor}:${fillAlpha}`;
        if (!nodeFillGroups.has(fillKey)) nodeFillGroups.set(fillKey, { color: fillColor, alpha: fillAlpha, bounds: [] });
        nodeFillGroups.get(fillKey).bounds.push(bounds);
        if (node.is_low_confidence) {
          drawDashedCapsuleBorder(overlayLayer, bounds, lowConfidenceColor(node), 1.8);
        } else {
          const strokeWidth = nodeStrokeWidth(node);
          const strokeAlpha = node._focused ? 1 : (node.sanctioned || node.egypt_judgment_hit || node.adverse_media_hit ? 1 : 0.7);
          const strokeKey = `${strokeColor}:${strokeWidth}:${strokeAlpha}`;
          if (!nodeStrokeGroups.has(strokeKey)) {
            nodeStrokeGroups.set(strokeKey, { color: strokeColor, width: strokeWidth, alpha: strokeAlpha, bounds: [] });
          }
          nodeStrokeGroups.get(strokeKey).bounds.push(bounds);
          if (hasCombinedSanctionAdverse) {
            const inset = 2.4;
            const innerWidth = 2.1;
            overlayLayer.roundRect(
              bounds.x + inset,
              bounds.y + inset,
              Math.max(0, bounds.width - (inset * 2)),
              Math.max(0, bounds.height - (inset * 2)),
              Math.max(0, bounds.radius - inset),
            );
            overlayLayer.stroke({ color: 0xff6a00, width: innerWidth, alpha: 1 });
          }
          if (node._lowConfidenceOnlyVisible) {
            drawDashedCapsuleBorder(overlayLayer, bounds, 0xfacc15, 1.8);
          }
        }
        if (node._batchSelected) {
          overlayLayer.roundRect(bounds.x - 2, bounds.y - 2, bounds.width + 4, bounds.height + 4, bounds.radius + 2);
          overlayLayer.stroke({ color: 0x58a6ff, width: 2.4, alpha: 1 });
        }
      });
      nodeFillGroups.forEach((group) => {
        group.bounds.forEach((bounds) => nodeLayer.roundRect(bounds.x, bounds.y, bounds.width, bounds.height, bounds.radius));
        nodeLayer.fill({ color: group.color, alpha: group.alpha });
      });
      nodeStrokeGroups.forEach((group) => {
        group.bounds.forEach((bounds) => nodeLayer.roundRect(bounds.x, bounds.y, bounds.width, bounds.height, bounds.radius));
        nodeLayer.stroke({ color: group.color, width: group.width, alpha: group.alpha });
      });

      drawHoveredNode();
      if (syncLabels) updateLabels();
    }

    function fitToNodes(nodes) {
      if (!nodes.length) return;
      const bounds = { x0: Infinity, x1: -Infinity, y0: Infinity, y1: -Infinity };
      nodes.forEach((node) => {
        const nodeBounds = pillBounds(node);
        bounds.x0 = Math.min(bounds.x0, nodeBounds.x);
        bounds.x1 = Math.max(bounds.x1, nodeBounds.x + nodeBounds.width);
        bounds.y0 = Math.min(bounds.y0, nodeBounds.y);
        bounds.y1 = Math.max(bounds.y1, nodeBounds.y + nodeBounds.height);
      });
      bounds.x0 -= 60;
      bounds.x1 += 60;
      bounds.y0 -= 40;
      bounds.y1 += 40;
      const width = Math.max(1, bounds.x1 - bounds.x0);
      const height = Math.max(1, bounds.y1 - bounds.y0);
      const viewportWidth = host.clientWidth || container.clientWidth || window.innerWidth;
      const viewportHeight = host.clientHeight || container.clientHeight || window.innerHeight;
      const scale = clamp(Math.min(viewportWidth / width, viewportHeight / height, 1.5) * 0.85, 0.05, 6);
      const x = ((viewportWidth - (width * scale)) / 2) - (bounds.x0 * scale);
      const y = ((viewportHeight - (height * scale)) / 2) - (bounds.y0 * scale);
      if (zoomBehavior) {
        d3.select(host).call(zoomBehavior.transform, d3.zoomIdentity.translate(x, y).scale(scale));
      } else {
        syncWorldTransform(createWorldTransform(x, y, scale));
      }
    }

    function separateOverlappingRows(nodes, gap = 16) {
      const nodesByLane = new Map();
      nodes.forEach((node) => {
        const lane = Number(node.lane || 0);
        if (!nodesByLane.has(lane)) nodesByLane.set(lane, []);
        nodesByLane.get(lane).push(node);
      });
      nodesByLane.forEach((laneNodes) => {
        const rows = [];
        laneNodes
          .slice()
          .sort((left, right) => Number(left.y || 0) - Number(right.y || 0))
          .forEach((node) => {
            const row = rows.find((candidate) => (
              Math.abs(Number(node.y || 0) - candidate.y)
              < ((pillHeight(node) + candidate.height) / 2) + gap
            ));
            if (row) {
              row.nodes.push(node);
              row.height = Math.max(row.height, pillHeight(node));
              return;
            }
            rows.push({ y: Number(node.y || 0), height: pillHeight(node), nodes: [node] });
          });
        rows.forEach((row) => {
          const ordered = row.nodes.slice().sort((left, right) => Number(left.x || 0) - Number(right.x || 0));
          if (ordered.length < 2) return;
          const desiredLeft = Math.min(...ordered.map((node) => Number(node.x || 0) - (pillWidth(node) / 2)));
          const desiredRight = Math.max(...ordered.map((node) => Number(node.x || 0) + (pillWidth(node) / 2)));
          const packed = [];
          let previousRight = Number.NEGATIVE_INFINITY;
          let changed = false;
          ordered.forEach((node) => {
            const halfWidth = pillWidth(node) / 2;
            const desiredX = Number(node.x || 0);
            const nextX = Math.max(desiredX, previousRight + gap + halfWidth);
            if (Math.abs(nextX - desiredX) > 0.5) changed = true;
            packed.push({ node, x: nextX });
            previousRight = nextX + halfWidth;
          });
          if (!changed) return;
          const packedLeft = packed[0].x - (pillWidth(packed[0].node) / 2);
          const last = packed[packed.length - 1];
          const packedRight = last.x + (pillWidth(last.node) / 2);
          const offset = ((desiredLeft + desiredRight) - (packedLeft + packedRight)) / 2;
          packed.forEach((item) => { item.node.x = item.x + offset; });
        });
      });
    }

    function getViewState() {
      return {
        transform: { x: transform.x, y: transform.y, k: transform.k },
        positions: Object.fromEntries(sceneNodes.map((node) => [
          nodeSceneKey(node),
          { x: Number(node.x) || 0, y: Number(node.y) || 0 },
        ])),
      };
    }

    function restoreViewState(state) {
      const positions = state?.positions && typeof state.positions === "object" ? state.positions : {};
      sceneNodes.forEach((node) => {
        const position = positions[nodeSceneKey(node)];
        if (!Number.isFinite(Number(position?.x)) || !Number.isFinite(Number(position?.y))) return;
        node.x = Number(position.x);
        node.y = Number(position.y);
      });
      separateOverlappingRows(sceneNodes);
      const savedTransform = state?.transform;
      if (
        Number.isFinite(Number(savedTransform?.x))
        && Number.isFinite(Number(savedTransform?.y))
        && Number.isFinite(Number(savedTransform?.k))
      ) {
        const nextTransform = d3.zoomIdentity
          .translate(Number(savedTransform.x), Number(savedTransform.y))
          .scale(clamp(Number(savedTransform.k), 0.05, 6));
        if (zoomBehavior) d3.select(host).call(zoomBehavior.transform, nextTransform);
        else syncWorldTransform(nextTransform);
      }
      drawScene();
    }

    function pickHit(clientX, clientY) {
      const rect = host.getBoundingClientRect();
      const worldX = transform.invertX(clientX - rect.left);
      const worldY = transform.invertY(clientY - rect.top);
      for (let index = sceneNodes.length - 1; index >= 0; index -= 1) {
        const node = sceneNodes[index];
        const focus = focusButtonBounds(node);
        if (focus) {
          const dx = focus.cx - worldX;
          const dy = focus.cy - worldY;
          if ((dx * dx) + (dy * dy) <= Math.max(focus.r, 10 / transform.k) ** 2) {
            return { node, zone: "focus" };
          }
        }
        const bounds = pillBounds(node);
        if (worldX >= bounds.x && worldX <= bounds.x + bounds.width && worldY >= bounds.y && worldY <= bounds.y + bounds.height) {
          return { node, zone: "body" };
        }
      }
      let bestEdge = null;
      let bestDistance = Infinity;
      for (const edge of sceneEdges) {
        const endpoints = edgeEndpoints(edge);
        if (!endpoints) continue;
        const distanceSquared = distanceToSegmentSquared(worldX, worldY, endpoints.start.x, endpoints.start.y, endpoints.end.x, endpoints.end.y);
        const threshold = Math.max(10 / transform.k, 5);
        if (distanceSquared <= threshold ** 2 && distanceSquared < bestDistance) {
          bestDistance = distanceSquared;
          bestEdge = edge;
        }
      }
      if (bestEdge) {
        return { edge: bestEdge, zone: "edge" };
      }
      return null;
    }

    function distanceToSegmentSquared(px, py, x1, y1, x2, y2) {
      const dx = x2 - x1;
      const dy = y2 - y1;
      if (dx === 0 && dy === 0) {
        return ((px - x1) ** 2) + ((py - y1) ** 2);
      }
      const t = Math.max(0, Math.min(1, (((px - x1) * dx) + ((py - y1) * dy)) / ((dx * dx) + (dy * dy))));
      const cx = x1 + (t * dx);
      const cy = y1 + (t * dy);
      return ((px - cx) ** 2) + ((py - cy) ** 2);
    }

    function setHoveredNode(nextNodeKey) {
      const previousNode = sceneNodeByKey.get(hoveredNodeKey);
      const nextNode = sceneNodeByKey.get(nextNodeKey);
      if (previousNode) previousNode._hovered = false;
      if (nextNode) nextNode._hovered = true;
      hoveredNodeKey = nextNodeKey;
      drawHoveredNode();
      if (previousNode) {
        const previousElement = labelElementBySceneKey.get(nodeSceneKey(previousNode));
        if (previousElement) updateLabelElement(previousElement, previousNode);
      }
      if (nextNode) {
        const nextElement = labelElementBySceneKey.get(nodeSceneKey(nextNode));
        if (nextElement) updateLabelElement(nextElement, nextNode);
      }
    }

    function handlePointerDown(event) {
      const hit = pickHit(event.clientX, event.clientY);
      if (!hit || hit.zone !== "body") return;
      draggingNode = hit.node;
      draggingPointerId = event.pointerId;
      dragStartClient = { x: event.clientX, y: event.clientY };
      dragMoved = false;
      host.dataset.dragging = "1";
      host.setPointerCapture?.(event.pointerId);
      event.preventDefault();
      event.stopPropagation();
      options.onDragStart?.(hit.node, event);
    }

    function handlePointerMove(event) {
      if (draggingNode && draggingPointerId === event.pointerId) {
        if (!dragMoved) {
          const distance = Math.hypot(event.clientX - dragStartClient.x, event.clientY - dragStartClient.y);
          if (distance < 4) return;
          dragMoved = true;
        }
        const rect = host.getBoundingClientRect();
        draggingNode.x = transform.invertX(event.clientX - rect.left);
        draggingNode.y = transform.invertY(event.clientY - rect.top);
        drawScene({ syncLabels: false });
        const labelElement = labelElementBySceneKey.get(nodeSceneKey(draggingNode));
        if (labelElement) updateLabelElement(labelElement, draggingNode);
        options.onDrag?.(draggingNode, event);
        return;
      }
      const hit = pickHit(event.clientX, event.clientY);
      const nodeKey = hit?.node ? nodeSceneKey(hit.node) : "";
      const edgeKey = hit?.edge ? edgeSceneKey(hit.edge) : "";
      const hitChanged = nodeKey !== hoveredNodeKey || edgeKey !== hoveredEdgeKey;
      if (nodeKey !== hoveredNodeKey) {
        setHoveredNode(nodeKey);
      }
      hoveredEdgeKey = edgeKey;
      if (hitChanged) {
        options.onHover?.(hit?.node || null, event, hit || null);
        options.onEdgeHover?.(hit?.edge || null, event, hit || null);
      } else if (hit) {
        options.onPointerMove?.(event, hit);
      }
    }

    function handlePointerUp(event) {
      if (!draggingNode || draggingPointerId !== event.pointerId) return;
      const finishedNode = draggingNode;
      const finishedDragMoved = dragMoved;
      draggingNode = null;
      draggingPointerId = null;
      dragStartClient = null;
      dragMoved = false;
      delete host.dataset.dragging;
      if (finishedDragMoved) suppressClickUntil = Date.now() + 180;
      host.releasePointerCapture?.(event.pointerId);
      if (finishedDragMoved) options.onDragEnd?.(finishedNode, event);
    }

    function handlePointerLeave() {
      if (!draggingNode) {
        setHoveredNode("");
        hoveredEdgeKey = "";
        options.onHover?.(null, null, null);
        options.onEdgeHover?.(null, null, null);
      }
    }

    function handleClick(event) {
      if (Date.now() < suppressClickUntil) return;
      if (host.dataset.dragging) return;
      const hit = pickHit(event.clientX, event.clientY);
      if (!hit) return;
      if (hit.zone === "focus") {
        options.onFocusButton?.(hit.node, event);
        return;
      }
      if (hit.zone === "edge") return;
      options.onClick?.(hit.node, event);
    }

    function handleContextMenu(event) {
      const hit = pickHit(event.clientX, event.clientY);
      if (!hit) {
        event.preventDefault();
        options.onBackgroundContextMenu?.(event);
        return;
      }
      if (hit.zone === "edge") {
        options.onEdgeContextMenu?.(hit.edge, event);
        return;
      }
      if (hit.zone !== "body") return;
      options.onContextMenu?.(hit.node, event);
    }

    function handleDoubleClick(event) {
      const hit = pickHit(event.clientX, event.clientY);
      if (!hit) {
        options.onBackgroundDoubleClick?.(event);
      }
    }

    function setGraph(graph, { fit = false } = {}) {
      sceneNodes = graph.nodes || [];
      sceneEdges = graph.edges || [];
      sceneNodeByKey = new Map(sceneNodes.map((node) => [nodeSceneKey(node), node]));
      sceneEdges.forEach((edge, index) => {
        edge._key = edgeSceneKey(edge, index);
      });
      rootIds = new Set(graph.rootIds || []);
      sceneNodes.forEach((node) => {
        node._focused = rootIds.has(node.id);
        node._hovered = nodeSceneKey(node) === hoveredNodeKey;
      });
      separateOverlappingRows(sceneNodes);
      if (fit) fitToNodes(sceneNodes);
      drawScene();
    }

    return {
      init,
      destroy,
      fitToNodes,
      getViewState,
      restoreViewState,
      setGraph,
      drawScene,
    };
  }

  window.IstariWebGLRenderer = {
    createGraphRenderer,
  };
}());
