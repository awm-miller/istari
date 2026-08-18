from __future__ import annotations

import json
import unittest

from src.graph.render import render_html
from src.graph.render_context import build_render_context


class GraphRenderingTests(unittest.TestCase):
    def test_leaflet_is_not_a_blocking_page_asset(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertNotIn('<script src="https://cdn.jsdelivr.net/npm/leaflet', html)
        self.assertNotIn('<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet', html)
        self.assertIn("LEAFLET_SCRIPT_URL", html)

    def test_graph_records_are_parsed_as_data_not_javascript_source(self) -> None:
        html = render_html({"nodes": [{"id": "n1"}], "edges": [{"source": "n1", "target": "n2"}]})

        self.assertIn('<script id="graph-data" type="application/json">', html)
        self.assertIn("JSON.parse(graphDataElement.textContent)", html)
        self.assertNotIn("const rawMainNodes = [{", html)

    def test_labels_use_a_persistent_world_layer(self) -> None:
        html = render_html({"nodes": [{"id": "n1", "label": "Visible label"}], "edges": []})

        self.assertIn('labelWorld.className = "graph-label-world"', html)
        self.assertIn("return sceneNodes;", html)
        self.assertNotIn("labelLayer.innerHTML =", html)

    def test_hover_updates_do_not_redraw_the_full_graph(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("function drawHoveredNode()", html)
        self.assertIn("const hitChanged = nodeKey !== hoveredNodeKey || edgeKey !== hoveredEdgeKey", html)
        self.assertIn("edge._key = edgeSceneKey(edge, index)", html)
        self.assertNotIn('setHoveredNode(nextNodeKey) {\n      const previousNode = sceneNodes.find', html)

    def test_added_tree_duplicates_receive_distinct_render_keys(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn('_sceneKey: `${sceneKey}:node:${node.id}`', html)
        self.assertIn("labelElementBySceneKey", html)

    def test_render_payload_removes_only_unused_and_duplicate_fields(self) -> None:
        evidence = {"document_url": "https://example.test/evidence", "notes": "Filed record"}
        context = build_render_context({
            "nodes": [{"id": "n1", "label": "Visible", "incorporation_date": "2020-01-01"}],
            "edges": [{
                "id": "e1",
                "source": "n1",
                "target": "n2",
                "tooltip": "Visible relationship",
                "source_provider": "registry",
                "evidence": evidence,
                "evidence_items": [evidence],
            }],
        })

        nodes = json.loads(context["nodes_json"])
        edges = json.loads(context["edges_json"])
        self.assertNotIn("incorporation_date", nodes[0])
        self.assertNotIn("source_provider", edges[0])
        self.assertNotIn("evidence", edges[0])
        self.assertEqual([evidence], edges[0]["evidence_items"])
        self.assertEqual("Visible relationship", edges[0]["tooltip"])

    def test_graph_switcher_routes_include_every_static_bundle(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        for graph_key in ("mb", "94-park-ave", "iums", "iran", "sevenspikes", "expanded-mb-names"):
            self.assertIn(f'data-graph-key="{graph_key}"', html)
            self.assertIn(f'href="/{graph_key}/"', html)
            self.assertIn(f'path: "/{graph_key}/"', html)
        self.assertIn("GRAPH_OPTIONS.find(({ path: optionPath })", html)

    def test_generated_graph_switcher_selection_survives_navigation(self) -> None:
        html = render_html({"nodes": [], "edges": []}, title_override="Generated graph")

        self.assertIn("function detectGeneratedGraphId(pathname)", html)
        self.assertIn("const currentGraphKey = currentGeneratedGraphId || detectGraphKey", html)
        self.assertIn('const path = canonicalGeneratedGraphPath(graphId)', html)
        self.assertIn('button.href = path', html)
        self.assertIn("button.dataset.graphKey = graphId", html)
        self.assertIn("if (isActive) setGraphSwitcherSelection(button, graphTitle)", html)
        self.assertIn('deleteButton.setAttribute("aria-label", `Delete ${graphTitle}`)', html)

    def test_builder_renders_backend_stdout_instead_of_status_copy(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn('class="builder-stdout"', html)
        self.assertIn("function renderBuilderStdout(job = {})", html)
        self.assertIn("Array.isArray(job.stdout)", html)
        self.assertNotIn('class="builder-status"', html)

    def test_builder_select_options_use_the_dark_colour_scheme(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("color-scheme: dark", html)

    def test_nearby_graphs_expose_the_focal_distance_filter(self) -> None:
        html = render_html({
            "focus_radius_metres": 250,
            "nodes": [{
                "id": "address:nearby",
                "label": "Nearby address",
                "kind": "address",
                "lane": 1,
                "focal_distance_metres": 120,
                "focal_distance_basis": "postcode_centroid",
            }],
            "edges": [],
        })

        self.assertIn('id="focal-distance-filter"', html)
        self.assertIn(".focal-distance-filter.hidden { display: none; }", html)
        self.assertIn('"focus_radius_metres":250', html)
        self.assertIn('"focal_distance_metres": 120', html)
        self.assertIn("function applyFocalDistanceFilter(projection)", html)
        self.assertIn("Minimum distance of the nearby registered address", html)
        self.assertIn(".case-controls select option:disabled", html)

    def test_former_relationships_are_purple_and_dashed(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn('if (edge.relationship_status === "former") return COLORS.purple;', html)
        self.assertIn('const isFormer = edge.relationship_status === "former";', html)
        self.assertIn("if (group.dashed) drawDashedLine", html)

    def test_promoted_people_become_the_active_top_level_root(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("promoted.lane = 0;", html)
        self.assertIn("[0, 1, 2, 3, 4].forEach((lane) =>", html)
        self.assertIn("if (node.promoted_to_seed) return 0;", html)
        self.assertIn("const rootDifference = Number(rootIds.has(right.id)) - Number(rootIds.has(left.id));", html)
        self.assertIn("const promotedDifference = Number(!!right.promoted_to_seed) - Number(!!left.promoted_to_seed);", html)
        self.assertIn("viewerState.focusedNodeIds = new Set(promotedNodes.map((node) => node.id));", html)

    def test_seed_restoration_removes_the_persisted_matching_identity_key(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("function promotedSeedKeyForNode(node, promotedKeys = promotedSeedKeys())", html)
        self.assertIn("const promotedSeedKey = promotedSeedKeyForNode(node, promotedKeys);", html)
        self.assertIn("nodeKey: isPromotedSeed ? promotedSeedKey : mergePrimaryKey", html)

    def test_ctrl_selection_merges_and_expands_all_merged_members(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn('id="graph-selection-merge"', html)
        self.assertIn("function selectedMergeAction()", html)
        self.assertIn('`node-id:${String(node.id || "")}`', html)
        self.assertIn("The first selected node will remain visible", html)
        self.assertIn("merge_member_node_ids", html)
        self.assertIn("centralNodeIds,", html)
        self.assertNotIn('label: "Start merge"', html)
        self.assertNotIn('type: "merge_start"', html)

    def test_graph_switcher_list_scrolls_within_the_viewport(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("max-height: min(70vh, 560px)", html)
        self.assertIn("overflow-y: auto", html)
        self.assertIn(".graph-switcher-menu::-webkit-scrollbar", html)

    def test_beautiful_ui_primitives_are_scoped_and_self_host_fonts(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn("Beautiful UI primitives, scoped to the header, Builder, and Tools", html)
        self.assertIn('.topbar,\n.builder-panel,\n.run-log-sheet,\n.viewer-sidebar,\n.sidebar-handle {', html)
        self.assertIn('.viewer-sidebar :is(.legend-toggle, .sidebar-meta-toggle input[type="checkbox"]):checked', html)
        self.assertIn('url("/assets/inter-latin.woff2")', html)
        self.assertIn('url("/assets/jetbrains-mono-latin.woff2")', html)
        self.assertNotIn("beautiful-ui-five.vercel.app/_next/static/media", html)


if __name__ == "__main__":
    unittest.main()
