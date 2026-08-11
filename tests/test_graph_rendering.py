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
        self.assertIn('plan.recipe === "address-network"', html)

    def test_builder_renders_backend_stdout_instead_of_status_copy(self) -> None:
        html = render_html({"nodes": [], "edges": []})

        self.assertIn('class="builder-stdout"', html)
        self.assertIn("function renderBuilderStdout(job = {})", html)
        self.assertIn("Array.isArray(job.stdout)", html)
        self.assertNotIn('class="builder-status"', html)


if __name__ == "__main__":
    unittest.main()
