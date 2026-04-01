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

  function escapeHtml(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
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

  function nodeStrokeWidth(node) {
    if (node._focused) return 2.8;
    if (node.sanctioned) return 3.4;
    if (node.is_low_confidence) return 1.4;
    return 1.2;
  }

  function nodeFillAlpha(node) {
    if (node._focused) return 0.28;
    if (node.is_low_confidence) return 0.14;
    if (node.sanctioned) return 0.48;
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

  function badgeMarkup(node) {
    const spec = badgeSpec(node);
    if (!spec) return "";
    const iconPath = spec.icon === "heart"
      ? '<path d="M12 20s-6-3.9-6-8.2A3.8 3.8 0 0 1 12 9a3.8 3.8 0 0 1 6 2.8C18 16.1 12 20 12 20Z"></path>'
      : '<path d="M5 19V7.8L9 5v14M11 19V9h8v10M8 10.6h.01M8 13.6h.01M8 16.6h.01M15 12h.01M15 15h.01"></path>';
    return `<span class="graph-node-badge"><svg viewBox="0 0 24 24" aria-hidden="true">${iconPath}</svg></span>`;
  }

  function focusMarkup(node) {
    if (node?.kind === "seed") return "";
    return '<span class="graph-node-focus" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M11 18a7 7 0 1 1 4.95-2.05M16 16l4 4"></path></svg></span>';
  }

  function createGraphRenderer(container, options) {
    const host = document.createElement("div");
    host.className = "graph-stage";
    const labelLayer = document.createElement("div");
    labelLayer.className = "graph-label-layer";
    container.innerHTML = "";
    container.append(host, labelLayer);

    const app = new PIXI.Application();
    const world = new PIXI.Container();
    const edgeLayer = new PIXI.Graphics();
    const nodeLayer = new PIXI.Graphics();
    const overlayLayer = new PIXI.Graphics();
    world.addChild(edgeLayer);
    world.addChild(nodeLayer);
    world.addChild(overlayLayer);

    let transform = createWorldTransform(0, 0, 1);
    let zoomBehavior = null;
    let sceneNodes = [];
    let sceneEdges = [];
    let rootIds = new Set();
    let labelNodes = [];
    let hoveredNodeId = "";
    let hoveredEdgeKey = "";
    let draggingNode = null;
    let draggingPointerId = null;
    let suppressClickUntil = 0;

    function syncWorldTransform(nextTransform) {
      transform = createWorldTransform(nextTransform.x, nextTransform.y, nextTransform.k);
      world.position.set(transform.x, transform.y);
      world.scale.set(transform.k, transform.k);
      updateLabels();
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
      window.addEventListener("resize", updateLabels);

      syncWorldTransform(d3.zoomIdentity);
    }

    function destroy() {
      window.removeEventListener("resize", updateLabels);
      app.destroy(true, { children: true });
    }

    function labelCandidates() {
      const scale = transform.k;
      if (rootIds.size) return sceneNodes;
      if (scale >= 0.85) return sceneNodes.slice(0, 1400);
      const focused = sceneNodes.filter((node) => node._focused || node._hovered || node._searchHit || node.sanctioned);
      if (scale >= 0.45) {
        const ranked = sceneNodes
          .filter((node) => !node._focused && !node._hovered && !node._searchHit && !node.sanctioned && Number(node._rankScore || 0) > 0)
          .sort((left, right) => Number(right._rankScore || 0) - Number(left._rankScore || 0))
          .slice(0, 220);
        return [...focused, ...ranked];
      }
      return focused;
    }

    function updateLabels() {
      labelNodes = labelCandidates();
      labelLayer.innerHTML = labelNodes.map((node) => {
        const classes = ["graph-node-label"];
        if (node._focused) classes.push("highlight");
        if (node.sanctioned) classes.push("sanctioned");
        if (node._hovered) classes.push("hovered");
        if (badgeSpec(node)) classes.push("has-badge");
        if (node.kind !== "seed") classes.push("has-focus");
        return `<div class="${classes.join(" ")}" data-node-id="${String(node.id)}">${badgeMarkup(node)}<span class="graph-node-text">${escapeHtml(node.label || "")}</span>${focusMarkup(node)}</div>`;
      }).join("");

      labelLayer.querySelectorAll(".graph-node-label").forEach((element) => {
        const nodeId = String(element.getAttribute("data-node-id") || "");
        const node = labelNodes.find((candidate) => String(candidate.id) === nodeId);
        if (!node) return;
        const bounds = pillBounds(node);
        element.style.width = `${bounds.width}px`;
        element.style.height = `${bounds.height}px`;
        element.style.transform = `translate(${transform.applyX(bounds.x)}px, ${transform.applyY(bounds.y)}px) scale(${transform.k})`;
        element.style.fontSize = `${Number(node._fontSize || 11)}px`;
        const spec = badgeSpec(node);
        const badge = element.querySelector(".graph-node-badge");
        if (badge && spec) {
          badge.style.background = `#${spec.fill.toString(16).padStart(6, "0")}`;
        }
      });
    }

    function drawScene() {
      edgeLayer.clear();
      nodeLayer.clear();
      overlayLayer.clear();

      sceneEdges.forEach((edge) => {
        const endpoints = edgeEndpoints(edge);
        if (!endpoints) return;
        const width = edge.kind === "hidden_connection" ? 1.8 : edge.kind === "alias" ? 2.5 : 1.4 + ((edge.weight || 0) * 1.5);
        const alpha = edge.is_low_confidence ? 0.72 : edge.kind === "address_link" ? 0.75 : 0.45;
        if (edge.kind === "hidden_connection" || edge.is_low_confidence) {
          drawDashedLine(edgeLayer, endpoints.start.x, endpoints.start.y, endpoints.end.x, endpoints.end.y, 8, 6);
        } else {
          edgeLayer.moveTo(endpoints.start.x, endpoints.start.y);
          edgeLayer.lineTo(endpoints.end.x, endpoints.end.y);
        }
        edgeLayer.stroke({ color: edge._colorValue, width, alpha });
      });

      sceneNodes.forEach((node) => {
        const bounds = pillBounds(node);
        const isHovered = node._hovered;
        const color = node._colorValue;
        nodeLayer.roundRect(bounds.x, bounds.y, bounds.width, bounds.height, bounds.radius);
        nodeLayer.fill({ color, alpha: nodeFillAlpha(node) });
        if (node.is_low_confidence) {
          drawDashedCapsuleBorder(overlayLayer, bounds, 0xfacc15, isHovered ? 2.2 : 1.8);
        } else {
          nodeLayer.stroke({ color, width: isHovered ? nodeStrokeWidth(node) + 0.8 : nodeStrokeWidth(node), alpha: node._focused || isHovered ? 1 : (node.sanctioned ? 1 : 0.7) });
        }
        if (node.sanctioned) {
          overlayLayer.roundRect(bounds.x - 4, bounds.y - 4, bounds.width + 8, bounds.height + 8, (bounds.height + 8) / 2);
          overlayLayer.fill({ color: 0xff2222, alpha: 0.12 });
          overlayLayer.stroke({ color: 0xff8a8a, width: isHovered ? 3.4 : 2.8, alpha: 1 });
        }

      });

      updateLabels();
    }

    function fitToNodes(nodes) {
      if (!nodes.length) return;
      const bounds = {
        x0: Math.min(...nodes.map((node) => pillBounds(node).x)) - 60,
        x1: Math.max(...nodes.map((node) => pillBounds(node).x + pillBounds(node).width)) + 60,
        y0: Math.min(...nodes.map((node) => pillBounds(node).y)) - 40,
        y1: Math.max(...nodes.map((node) => pillBounds(node).y + pillBounds(node).height)) + 40,
      };
      const width = Math.max(1, bounds.x1 - bounds.x0);
      const height = Math.max(1, bounds.y1 - bounds.y0);
      const viewportWidth = host.clientWidth || container.clientWidth || window.innerWidth;
      const viewportHeight = host.clientHeight || container.clientHeight || window.innerHeight;
      const scale = clamp(Math.min(viewportWidth / width, viewportHeight / height, 1.5) * 0.85, 0.05, 6);
      const x = ((viewportWidth - (width * scale)) / 2) - (bounds.x0 * scale);
      const y = ((viewportHeight - (height * scale)) / 2) - (bounds.y0 * scale);
      syncWorldTransform(createWorldTransform(x, y, scale));
      if (zoomBehavior) {
        d3.select(host).call(zoomBehavior.transform, d3.zoomIdentity.translate(x, y).scale(scale));
      }
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
          if (Math.sqrt((dx * dx) + (dy * dy)) <= Math.max(focus.r, 10 / transform.k)) {
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
        const distance = distanceToSegment(worldX, worldY, endpoints.start.x, endpoints.start.y, endpoints.end.x, endpoints.end.y);
        const threshold = Math.max(10 / transform.k, 5);
        if (distance <= threshold && distance < bestDistance) {
          bestDistance = distance;
          bestEdge = edge;
        }
      }
      if (bestEdge) {
        return { edge: bestEdge, zone: "edge" };
      }
      return null;
    }

    function distanceToSegment(px, py, x1, y1, x2, y2) {
      const dx = x2 - x1;
      const dy = y2 - y1;
      if (dx === 0 && dy === 0) {
        return Math.sqrt(((px - x1) ** 2) + ((py - y1) ** 2));
      }
      const t = Math.max(0, Math.min(1, (((px - x1) * dx) + ((py - y1) * dy)) / ((dx * dx) + (dy * dy))));
      const cx = x1 + (t * dx);
      const cy = y1 + (t * dy);
      return Math.sqrt(((px - cx) ** 2) + ((py - cy) ** 2));
    }

    function setHoveredNode(nextNodeId) {
      sceneNodes.forEach((node) => {
        node._hovered = String(node.id) === nextNodeId;
      });
      hoveredNodeId = nextNodeId;
      drawScene();
    }

    function handlePointerDown(event) {
      const hit = pickHit(event.clientX, event.clientY);
      if (!hit || hit.zone !== "body") return;
      draggingNode = hit.node;
      draggingPointerId = event.pointerId;
      host.dataset.dragging = "1";
      host.setPointerCapture?.(event.pointerId);
      event.preventDefault();
      event.stopPropagation();
      options.onDragStart?.(hit.node, event);
    }

    function handlePointerMove(event) {
      if (draggingNode && draggingPointerId === event.pointerId) {
        const rect = host.getBoundingClientRect();
        draggingNode.x = transform.invertX(event.clientX - rect.left);
        draggingNode.y = transform.invertY(event.clientY - rect.top);
        drawScene();
        options.onDrag?.(draggingNode, event);
        return;
      }
      const hit = pickHit(event.clientX, event.clientY);
      const nodeId = hit?.node ? String(hit.node.id) : "";
      const edgeKey = hit?.edge ? String(hit.edge._key || "") : "";
      if (nodeId !== hoveredNodeId) {
        setHoveredNode(nodeId);
      }
      hoveredEdgeKey = edgeKey;
      options.onHover?.(hit?.node || null, event, hit || null);
      options.onEdgeHover?.(hit?.edge || null, event, hit || null);
    }

    function handlePointerUp(event) {
      if (!draggingNode || draggingPointerId !== event.pointerId) return;
      const finishedNode = draggingNode;
      draggingNode = null;
      draggingPointerId = null;
      delete host.dataset.dragging;
      suppressClickUntil = Date.now() + 180;
      host.releasePointerCapture?.(event.pointerId);
      options.onDragEnd?.(finishedNode, event);
    }

    function handlePointerLeave() {
      if (!draggingNode) {
        setHoveredNode("");
        options.onHover?.(null, null, null);
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
      if (!hit) return;
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

    function setGraph(graph) {
      sceneNodes = graph.nodes || [];
      sceneEdges = graph.edges || [];
      rootIds = new Set(graph.rootIds || []);
      sceneNodes.forEach((node) => {
        node._focused = rootIds.has(node.id);
        node._hovered = String(node.id) === hoveredNodeId;
      });
      drawScene();
    }

    return {
      init,
      destroy,
      fitToNodes,
      setGraph,
      drawScene,
    };
  }

  window.IstariWebGLRenderer = {
    createGraphRenderer,
  };
}());
